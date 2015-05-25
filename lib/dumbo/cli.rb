require 'multi_json'
require 'sequel'
require 'druid'

require 'dumbo/segment'
require 'dumbo/firehose/hdfs'
require 'dumbo/task/index'
require 'dumbo/task/index_hadoop'

module Dumbo
  class CLI
    def initialize
      $log.info("scan", window: opts[:window])
      @db = Sequel.connect(MultiJson.load(File.read(opts[:database])))
      @druid = Druid::Client.new(opts[:zookeeper], { :discovery_path  => opts[:zookeeper_path]})
      @sources = MultiJson.load(File.read(opts[:sources]))
      @sources.each do |source_name, source|
        source['service'] = source_name.split("/")[0]
        source['dataSource'] = source_name.split("/")[-1]
      end
      @topics = opts[:topics] || @sources.keys
      @hdfs = Firehose::HDFS.new(opts[:namenodes], @sources)
      @interval = [((Time.now.utc-(opts[:window] + opts[:offset]).hours).floor(1.day)).utc, (Time.now.utc-opts[:offset].hour).utc]
      @tasks = []
      @limit = opts[:limit]
    end

    def run
      case opts[:mode]
      when "verify"
        $log.info("validating events from HDFS")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          validate_events(topic)
        end
        run_tasks
      when "compact"
        $log.info("Compacting segments")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          compact_segments(topic)
        end
        run_tasks
      when "unshard"
        $log.info("merging segment shards")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          unshard_segments(topic)
        end
        run_tasks
      else
        $log.error("Unknown mode #{opts[:mode]}, try -h")
      end

    end

    def run_tasks
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
          $log.info("Shortening interval due to epoc,", requested: validation_interval[0], epoc: epoc)
          validation_interval[0] = epoc
        end
      end

      @hdfs.slots(source_name, @interval).each do |slot|
        next if slot.paths.length < 1 || slot.events < 1

        segments = @segments.select do |s|
          s.source == source['dataSource'] &&
          s.interval.first <= slot.time &&
          s.interval.last >= slot.time + 1.hour
        end

        segment = segments.first
        segment_events = segment.events!(source['service'], [slot.time, slot.time+1.hour]) if segment
        rebuild = false

        currentMetrics = Set.new(segments.map(&:metrics).flatten)
        currentDimensions = Set.new(segments.map(&:dimensions))

        if !segment
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
        else
          segment.metadata(source['service'])['columns'].each do |name, column|
            next if column['type'] == 'STRING' # dimension column

            case name
            when '__time'
              type = 'LONG'
            when 'events'
              type = 'FLOAT'
            else
              case source['metrics'][name]
              when 'doubleSum'
                type = 'FLOAT'
              when 'longSum'
                type = 'LONG'
              else
                type = 'unknown'
              end
            end

            if type != column['type']
              $log.info("column type mismatch", for: slot.time, column: name, expected: type, got: column['type'])
              rebuild = true
            end
          end
        end

        next unless rebuild
        @tasks << Task::IndexHadoop.new(source, [slot.time, slot.time+1.hour], slot.patterns)
      end
    end

    def compact_segments(topic)
      compact_interval = [@interval[0].utc, @interval[-1].floor(1.day).utc]
      compact_interval[0] -= 1.day if compact_interval[0] == compact_interval[-1]
      $log.info("compacting segments for", topic: topic, interval: compact_interval)

      source = @sources[topic]
      dataSource = source['dataSource']
      compact_range = (compact_interval[0]...compact_interval[-1])
      expectedMetrics = Set.new(source['metrics'].keys).add("events")
      expectedDimensions = Set.new(source['dimensions'])

      segments = @segments.select do |segment|
        segment.source == dataSource &&
        (segment.interval[0]...segment.interval[-1]).overlaps?(compact_range)
      end

      segment_size = case (source['output']['segmentGranularity'] || "day").downcase
      when 'fifteen_minute'
        15.minutes
      when 'thirty_minute'
        30.minutes
      when 'hour'
        1.hour
      when 'day'
        1.day
      else
        raise "Unsupported segmentGranularity #{source['output']['segmentGranularity']}"
      end

      compact_interval[0].to_i.step(compact_interval[-1].to_i - 1, segment_size) do |start_time|
        segment_range = (Time.at(start_time).utc...Time.at(start_time + segment_size).utc)
        segment_interval = [segment_range.first, segment_range.last]

        is_correct_schema = nil

        is_compact = segments.all? do |segment|
          if (segment.interval[0]...segment.interval[-1]).overlaps?(segment_range)
            if is_correct_schema == nil
              currentMetrics = Set.new(segment.metrics)
              currentDimensions = Set.new(segment.dimensions)

              is_correct_schema = (currentMetrics == expectedMetrics) && (currentDimensions == expectedDimensions)
            end
            segment.interval.first == segment_range.first &&
            segment.interval.last == segment_range.last
          else
            true
          end
        end
        # ensure we don't try to compact non-existing data
        is_correct_schema = true if is_correct_schema == nil

        next if is_compact && is_correct_schema
        $log.info("Compact", topic: topic, interval: segment_interval, compact: is_compact, correct_schema: is_correct_schema)
        @tasks << Task::Index.new(source, segment_interval)
      end

    end

    def unshard_segments(topic)
      $log.info("merging segments for", topic: topic)

      source = @sources[topic]
      dataSource = source['dataSource']

      @segments.select do |segment|
        segment.source == dataSource &&
        segment.interval.first >= @interval.first &&
        segment.interval.last <= @interval.last
      end.group_by do |segment|
        segment.interval.map(&:iso8601).join('/')
      end.each do |interval, segments|
        if %w(linear hashed).include?(segments.first.shardSpec['type'])
          $log.info("merging segments", for: interval, segments: segments.length)
          @tasks << Task::Index.new(source, segments.first.interval)
        end
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
      when 'day'
        1.day
      else
        raise "Unsupported granularity #{g}"
      end
    end

  end
end
