require 'mysql2'
require 'multi_json'

module Dumbo
  class Segment
    def self.all(db, druid)
      @all ||= all!(db, druid)
    end

    def self.all!(db, druid)
      query = %Q{SELECT payload FROM druid_segments WHERE used = 1}
      segments = db.query(query).map do |row|
        new(MultiJson.load(row['payload']), druid)
      end
      $log.info("found #{segments.length} segments in metadata store")
      return segments
    end

    attr_reader :source, :interval, :version, :dimensions, :metrics, :loadSpec, :shardSpec

    def initialize(payload, druid)
      @payload = payload
      @source = payload['dataSource']
      @interval = payload['interval'].split('/').map { |str| Time.parse(str) }
      @version = Time.parse(payload['version'])
      @dimensions = Set.new(payload['dimensions'].split(','))
      @metrics = Set.new(payload['metrics'].split(','))
      @loadSpec = payload['loadSpec']
      @shardSpec = payload['shardSpec']
      @druid = druid
    end

    def events
      @events ||= events!
    end

    def events!(interval = nil)
      interval ||= @interval
      query = Druid::Query::Builder.new
        .timeseries
        .long_sum('events')
        .granularity(:all)
        .interval(interval.first, interval.last)

      ds = nil
      while ds.nil?
        ds = @druid.data_source("broker/#{@source}")
      end

      ds.post(query).first['result']['events'] rescue 0
    end

    def metadata
      @metadata ||= metadata!
    end

    def metadata!(interval = nil)
      interval ||= @interval
      query = Druid::Query::Builder.new
        .metadata
        .interval(interval.first, interval.last)

      ds = nil
      while ds.nil?
        ds = @druid.data_source("broker/#{@source}")
      end

      ds.post(query).first
    end

    def as_json(options = {})
      @payload
    end

  end
end
