require 'multi_json'
require 'diplomat'
require 'sequel'
require 'druid'

require 'dumbo/segment'
require 'dumbo/firehose/hdfs'
require 'dumbo/task/reintake'
require 'dumbo/task/compact_segments'
require 'dumbo/overlord_scanner'

module Dumbo
  class CLI
    def initialize
      $log.info("scan", window: opts[:window])
      @db = Sequel.connect(MultiJson.load(File.read(opts[:database])))
      @druid = Druid::Client.new(opts[:zookeeper], { :discovery_path  => opts[:zookeeper_path]})
      @service_name = opts[:sources].split("/").last.split(".").first
      @sources = MultiJson.load(File.read(opts[:sources]))
      @sources.each do |source_name, source|
        source['service'] = source_name.split("/")[0]
        source['dataSource'] = source_name.split("/")[-1]
      end
      @topics = opts[:topics] || @sources.keys
      @hdfs = Firehose::HDFS.new(opts[:namenodes], @sources)
      @interval = opts[:forced_interval] || [((Time.now.utc-(opts[:window] + opts[:offset]).hours).floor).utc, (Time.now.utc-opts[:offset].hour).floor(1.hour).utc]
      @tasks = []
      @limit = opts[:limit]
      @forced = opts[:force]
      @reverse = opts[:reverse]
      @overlord_scanner = OverlordScanner.new(opts[:overlord])
    end

    def run
      case opts[:mode]
      when "verify"
        $log.info("validating events from HDFS")
        @segments = Dumbo::Segment.all(@db, @druid, @sources)
        @topics.each do |topic|
          validate_events(topic)
        end
        run_tasks
      when "compact"
        $log.info("compacting segments")
        @segments = Dumbo::Segment.all(@db, @druid, @sources)
        @topics.each do |topic|
          compact_segments(topic)
        end
        run_tasks
      else
        $log.error("Unknown mode #{opts[:mode]}, try -h")
      end
      $log.info("k tnx bye")
    end

    def run_tasks
      if @reverse
        @tasks = @tasks.reverse
      end

      log_tasks_number(@service_name, @tasks.length)

      if @limit > 0 && @tasks.length > @limit
        $log.info("Limiting task execution,", actually: @tasks.length, limit: @limit)
        @tasks = @tasks[0,@limit]
      end

      @tasks.each do |task|
        if opts[:dryrun]
          puts task.inspect
        else
          task.run!("http://#{opts[:overlord]}/druid/indexer/v1/task")
        end
      end

      return if opts[:dryrun]

      while @tasks.length > 0
        $log.info("waiting for #{@tasks.length} tasks=#{@tasks.map(&:id)}")
        sleep 30
        @tasks.reject!(&:finished?)
      end
    end

    def validate_events(source_name)
      $log.info("validating events for", source: source_name)

      source = @sources[source_name]

      expectedMetrics = Set.new(source['metrics'].keys).add("events")
      expectedDimensions = Set.new(source['dimensions'])

      validation_interval = @interval
      if source['input']['epoc']
        epoc = Time.parse(source['input']['epoc'])
        if epoc > validation_interval[0]
          $log.info("shortening interval due to epoc,", requested: validation_interval[0], epoc: epoc)
          validation_interval[0] = epoc
        end
      end

      lag_check = [source['input']['gobblin']].flatten.compact.uniq
      if lag_check.size > 0
        last_non_lagging_slot = validation_interval[0]

        $log.info("checking for lag on", paths: lag_check)
        @hdfs.slots(source_name, @interval).each_with_index do |slot, ii|
          next if slot.events < 1
          next unless lag_check.all? do |required_path|
            slot.paths.any? {|existing_path| existing_path.start_with?(required_path) }
          end
          last_non_lagging_slot = slot.time - 1.hour
        end

        if last_non_lagging_slot != validation_interval[1]
          $log.info("last non lagging slot is #{last_non_lagging_slot}")
          validation_interval[1] = last_non_lagging_slot
        end
      end

      @hdfs.slots(source_name, validation_interval).each do |slot|
        if slot.paths.length < 1 || slot.events < 1
          $log.info("skipping segments w/o raw data", slot: slot.time)
          next
        end

        segments = @segments.select do |s|
          s.source == source['dataSource'] &&
          s.interval.first <= slot.time &&
          s.interval.last > slot.time
        end

        segment = segments.first
        segment_events = segment.events!(source['service'], [slot.time, slot.time+1.hour]) if segment
        rebuild = false

        currentMetrics = Set.new(segments.map(&:metrics).flatten)
        currentDimensions = Set.new(segments.map(&:dimensions))

        if @forced
          $log.info("force rebuild segment for", slot: slot.time)
          rebuild = true
        elsif !segment
          $log.info("found missing segment for", slot: slot.time)
          rebuild = true
        elsif segment_events != slot.events
          $log.info("event mismatch", for: slot.time, delta: slot.events - segment_events, hdfs: slot.events, segment: segment_events)
          rebuild = true
        elsif currentMetrics < expectedMetrics
          $log.info("found new metrics", for: slot.time, delta: (expectedMetrics - currentMetrics).to_a)
          rebuild = true
        elsif currentMetrics > expectedMetrics
          $log.info("found deleted metrics", for: slot.time, delta: (currentMetrics - expectedMetrics).to_a)
          rebuild = true
        elsif currentDimensions < expectedDimensions
          $log.info("found new dimensions", for: slot.time, delta: (expectedDimensions - currentDimensions).to_a)
          rebuild = true
        elsif currentDimensions > expectedDimensions
          $log.info("found deleted dimensions", for: slot.time, delta: (currentDimensions - expectedDimensions).to_a)
          rebuild = true
        #todo column check was buggy so it has been disabled see releted commit
        end

        next unless rebuild
        @tasks << Task::Reintake.new(source, [slot.time, slot.time+1.hour], slot.patterns)
      end
    end

    def get_overlapping_segments_and_interval(source, check_interval, available_segments = @segments)
      dataSource = source['dataSource']

      oldest = check_interval.first
      newest = check_interval.last

      check_range = (oldest...newest)

      segments = available_segments.select do |segment|
        if segment.source == dataSource && (segment.interval[0]...segment.interval[-1]).overlaps?(check_range)
          oldest = [oldest, segment.interval[0]].min
          newest = [newest, segment.interval[-1]].max
          true
        else
          false
        end
      end

      return {
        dataSource: dataSource,
        interval: [oldest, newest],
        segments: segments
      }
    end

    def get_segment_granularity(source)
      case (source['output']['segmentGranularity'] || 'hour').downcase
      when 'fifteen_minute'
        15.minutes
      when 'thirty_minute'
        30.minutes
      when 'hour'
        1.hour
      when 'day'
        1.day
      when 'week'
        7.days
      when 'month'
        1.month
      when 'year'
        1.year
      else
        raise "Unsupported segmentGranularity #{source['output']['segmentGranularity']}"
      end
    end

    def compact_segments(topic)
      source = @sources[topic]

      segment_size = get_segment_granularity(source)
      floor_date = segment_size

      $log.info("request compaction", topic: topic, interval: @interval, granularity: (source['output']['segmentGranularity'] || 'hour').downcase)
      compact_interval = [@interval[0].floor(floor_date).utc, @interval[-1].floor(floor_date).utc]
      $log.info("compacting scan", topic: topic, interval: compact_interval)

      expectedMetrics = Set.new(source['metrics'].keys).add("events")
      expectedDimensions = Set.new(source['dimensions'])

      segment_interval = [compact_interval[0], compact_interval[0]]
      while segment_interval[-1] < compact_interval[-1] do
        segment_interval = [segment_interval[-1].utc, (segment_interval[-1] + 1).ceil(floor_date).utc]
        $log.info("scanning segment", topic: topic, interval: segment_interval)

        segment_input = get_overlapping_segments_and_interval(source, segment_interval)

        must_compact = segment_input[:segments].any? do |input_segment|
          should_compact = false

          currentMetrics = Set.new(input_segment.metrics)
          if currentMetrics != expectedMetrics
            possibleMetrics = currentMetrics & expectedMetrics

            if possibleMetrics <= expectedMetrics && possibleMetrics != currentMetrics
              $log.info("detected a possible metrics reduction")
              should_compact = true
            end
          end

          currentDimensions = Set.new(input_segment.dimensions)
          if currentDimensions != expectedDimensions
            possibleDimensions = currentDimensions & expectedDimensions

            if possibleDimensions <= expectedDimensions && possibleDimensions != currentDimensions
              $log.info("detected a possible dimensions reduction")
              should_compact = true
            end
          end

          if input_segment.interval.first > segment_interval.first && input_segment.interval.last < segment_interval.last
            $log.info("detected too small segment size,", is: input_segment.interval, expected: segment_interval)
            should_compact = true
          end

          should_compact
        end

        if segment_input[:segments].size > 0 && source['output']['numShards'] && segment_input[:segments].size < source['output']['numShards']
          $log.info("detected too few segments,", is: segment_input[:segments].size, expected: source['output']['numShards'])
          must_compact = true
        end

        @tasks << Task::CompactSegments.new(source, segment_interval) if must_compact
      end
    end

    def granularity(g)
      case g.downcase
      when 'minute'
        1.minutes
      when 'fifteen_minute'
        15.minutes
      when 'thirty_minute'
        30.minutes
      when 'hour'
        1.hour
      else
        raise "Unsupported granularity #{g}"
      end
    end

    def log_tasks_number service_name, number
      data = []
      begin
        data = MultiJson.load(Diplomat::Kv.get("druid-dumbo/#{service_name}"))
      rescue
      end

      now = Time.now
      current_day = Time.utc(now.year, now.month, now.day, 0, 0, 0)

      new_entry = {"tasks" => number, "ts" => current_day.to_s}

      if data.length > 10
        data = data[1..10]
      end

      updated = false

      data.each do |entry|
        if entry["ts"] == current_day.to_s
          entry["tasks"] = [entry["tasks"], new_entry["tasks"]].min
          updated = true
        end
      end

      data << new_entry unless updated

      begin
        Diplomat::Kv.put("druid-dumbo/#{service_name}", MultiJson.dump(data))
      rescue
      end
    end
  end
end
