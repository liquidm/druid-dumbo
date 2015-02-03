require 'dumbo/task/base'

module Dumbo
  module Task
    class Index < Base
      def initialize(topic, interval, source, segmentGranularity = "hour", queryGranularity = "minute")
        @topic = topic
        @interval = interval
        @source = source
        @segmentGranularity = segmentGranularity
        @queryGranularity = queryGranularity
      end

      def as_json(options = {})
        interval = "#{@interval.first.iso8601}/#{@interval.last.iso8601}"
        {
          type: 'index',
          spec: {
            dataSchema: {
              dataSource: @topic,
              metricsSpec: (@source['aggregators'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
              end + [{ type: "longSum", name: "events", fieldName: "events" }],
              granularitySpec: {
                segmentGranularity: @segmentGranularity,
                queryGranularity: @queryGranularity,
                intervals: [interval],
              }
            },
            ioConfig: {
              type: 'index',
              firehose: {
                type: "ingestSegment",
                dataSource: @source['dataSource'],
                interval: interval,
                dimensions: @source['dimensions'],
              },
            },
            tuningConfig: {
              type: 'index',
            },
          },
        }
      end
    end
  end
end
