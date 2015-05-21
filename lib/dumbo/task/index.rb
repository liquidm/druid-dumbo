require 'dumbo/task/base'

module Dumbo
  module Task
    class Index < Base
      def initialize(source, interval)
        @source = source
        @interval = interval
      end

      def as_json(options = {})
        interval = "#{@interval.first.iso8601}/#{@interval.last.iso8601}"
        config = {
          type: 'index',
          spec: {
            dataSchema: {
              dataSource: @source['dataSource'],
              metricsSpec: (@source['metrics'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
              # WARNING: do NOT use count for events, will count in segment vs count in raw input
              end + [{ type: "doubleSum", name: "events", fieldName: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
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
        if (@source['output']['numShards'] || 0) > 1
          config[:spec][:tuningConfig][:targetPartitionSize] = -1
          config[:spec][:tuningConfig][:numShards] = @source['output']['numShards']
        end
        config
      end
    end
  end
end
