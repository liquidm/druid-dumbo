require 'multi_json'
require 'druid'

require 'dumbo/segment'
require 'dumbo/firehose/hdfs'
require 'dumbo/task/index'
require 'dumbo/task/index_hadoop'
require 'dumbo/task/merge'

module Dumbo
  class CLI
    def initialize
      @topics = opts[:topics]
      raise "no topics given" if @topics.empty?
      $log.info("scan", window: opts[:window])
      @db = Mysql2::Client.new(MultiJson.load(File.read(opts[:database])))
      @hdfs = Firehose::HDFS.new(opts[:namenodes])
      @druid = Druid::Client.new(opts[:zookeeper])
      @sources = MultiJson.load(File.read(opts[:sources]))
      @interval = [(Time.now.utc-(opts[:window] + opts[:offset]).hours).floor(1.day), Time.now.utc-opts[:offset].hour]
      @tasks = []
    end

    def run
      if opts[:modes].include?("verify")
        $log.info("validating events from HDFS")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          validate_events(topic)
        end
        run_tasks
      end

      if opts[:modes].include?("verify")
        $log.info("validating schema")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          validate_schema(topic)
        end
        run_tasks
      end

      if opts[:modes].include?("unshard")
        $log.info("merging segment shards")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          unshard_segments(topic)
        end
        run_tasks
      end

      if opts[:modes].include?("daily")
        $log.info("validating daily segments")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          reingest_daily(topic)
        end
        run_tasks
      end
    end

    def run_tasks
      @tasks.each do |task|
        if opts[:dryrun]
          puts task.inspect
        else
          task.run!(opts[:overlord])
        end
      end

      return if opts[:dryrun]

      while @tasks.length > 0
        $log.info("waiting for #{@tasks.length} tasks=#{@tasks.map(&:id)}")
        sleep 30
        @tasks.reject!(&:finished?)
      end
    end

    def validate_events(topic)
      $log.info("validating events for", topic: topic)
      @hdfs.slots(topic, @interval).each do |slot|
        next if slot.paths.length < 1 || slot.events < 1

        segments = @segments.select do |s|
          s.source == topic &&
          s.interval.first <= slot.time &&
          s.interval.last >= slot.time + 1.hour
        end

        segment = segments.first
        segment_events = segment.events!([slot.time, slot.time+1.hour]) if segment
        rebuild = false

        source = @sources[topic]

        source['metrics'] = Set.new(source['aggregators'].keys).add("events")
        metrics = Set.new(segments.map(&:metrics).flatten)

        source['dimensions'] = Set.new(source['dimensions'])
        dimensions = Set.new(segments.map(&:dimensions))

        if !segment
          $log.info("found missing segment for", slot: slot.time)
          rebuild = true
        elsif segment_events != slot.events
          $log.info("event mismatch", for: slot.time, delta: slot.events - segment_events, hdfs: slot.events, segment: segment_events)
          rebuild = true
        elsif metrics < source['metrics']
          $log.info("found new metrics", for: slot.time, delta: (source['metrics'] - metrics).to_a)
          rebuild = true
        elsif metrics > source['metrics']
          $log.info("found deleted metrics", for: slot.time, delta: (metrics - source['metrics']).to_a)
          rebuild = true
        elsif dimensions < source['dimensions']
          $log.info("found new dimensions", for: slot.time, delta: (source['dimensions'] - dimensions).to_a)
          rebuild = true
        elsif dimensions > source['dimensions']
          $log.info("found deleted dimensions", for: slot.time, delta: (dimensions - source['dimensions']).to_a)
          rebuild = true
        end

        next unless rebuild
        @tasks << Task::IndexHadoop.new(topic, [slot.time, slot.time+1.hour], @sources[topic], slot.paths)
      end
    end

    def validate_schema(topic)
      $log.info("validating schema for", topic: topic)
      @hdfs.slots(topic, @interval).each do |slot|
        next if slot.paths.length < 1 || slot.events < 1

        source = @sources[topic]
        source['dataSource'] = topic

        segments = @segments.select do |s|
          s.source == topic &&
          s.interval.first <= slot.time &&
          s.interval.last >= slot.time + 1.hour
        end

        segment = segments.first

        rebuild = false
        segment.metadata['columns'].each do |name, column|
          next if column['type'] == 'STRING' # dimension column

          case name
          when '__time', 'events'
            type = 'LONG'
          else
            case source['aggregators'][name]
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

        next unless rebuild
        @tasks << Task::Index.new(topic, [slot.time, slot.time+1.hour], source, "hour", "minute")
      end
    end

    def unshard_segments(topic)
      $log.info("merging segments for", topic: topic)

      source = @sources[topic]
      source['dataSource'] = topic

      @segments.select do |segment|
        segment.source == topic &&
        segment.interval.first >= @interval.first &&
        segment.interval.last <= @interval.last
      end.group_by do |segment|
        segment.interval.map(&:iso8601).join('/')
      end.each do |interval, segments|
        if %w(linear hashed).include?(segments.first.shardSpec['type'])
          $log.info("merging segments", for: interval, segments: segments.length)
          @tasks << Task::Index.new(topic, segments.first.interval, source, "hour", "minute")
        end
      end
    end

    def reingest_daily(topic)
      $log.info("validating daily segments for", topic: topic)

      source = @sources[topic]
      source['dataSource'] = topic

      @hdfs.slots(topic, @interval).group_by do |slot|
        slot.time.floor(1.day).utc
      end.each do |day, slots|
        events = slots.map(&:events).reduce(:+)
        next if events < 1
        next if day == Time.now.floor(1.day).utc

        segments = @segments.select do |s|
          s.source == "#{topic}_daily" &&
          s.interval.first <= day &&
          s.interval.last >= day + 1.day
        end

        segment = segments.first
        segment_events = segment.events!([day, day+1.day]) if segment
        rebuild = false

        if !segment
          $log.info("found missing daily segment", for: day)
          rebuild = true
        elsif segment_events != events
          $log.info("mismatch", for: day, delta: events - segment_events, hdfs: events, segment: segment_events)
          slots.each do |slot|
            puts "#{slot.time}: #{slot.paths.inspect}"
          end
          rebuild = true
        end

        next unless rebuild

        @tasks << Task::Index.new("#{topic}_daily", [day, day+1.day], source, "day", "day")
      end
    end

  end
end
