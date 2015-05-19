require 'multi_json'
require 'druid'

require 'dumbo/segment'
require 'dumbo/firehose/hdfs'
require 'dumbo/task/index'
require 'dumbo/task/index_hadoop'

module Dumbo
  class CLI
    def initialize
      $log.info("scan", window: opts[:window])
      @db = Mysql2::Client.new(MultiJson.load(File.read(opts[:database])))
      @druid = Druid::Client.new(opts[:zookeeper], { :discovery_path  => opts[:zookeeper_path]})

      @sources = MultiJson.load(File.read(opts[:sources]))
      @sources.each do |source_name, source|
        source['service'] = source_name.split("/")[0]
        source['dataSource'] = source_name.split("/")[-1]
      end
      @topics = opts[:topics] || @sources.keys
      @hdfs = Firehose::HDFS.new(opts[:namenodes], @sources)
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

      if opts[:modes].include?("unshard")
        $log.info("merging segment shards")
        @segments = Dumbo::Segment.all(@db, @druid)
        @topics.each do |topic|
          unshard_segments(topic)
        end
        run_tasks
      end
    end

    def run_tasks
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

  end
end
