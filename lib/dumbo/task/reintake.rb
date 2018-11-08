require 'dumbo/task/base'

module Dumbo
  module Task
    class Reintake < Base
      def initialize(source, interval, paths)
        @source = source
        @interval = interval
        @paths = paths
      end

      def as_json(options = {})
        config = {
          type: 'index_hadoop',
          spec: {
            dataSchema: {
              dataSource: @source['dataSource'],
              parser: {
                parseSpec: {
                  format: "json",
                  timestampSpec: {
                    column: ((@source['input']['timestamp'] || {})['column'] || "timestamp"),
                    format: ((@source['input']['timestamp'] || {})['format'] || "ruby"),
                  },
                  dimensionsSpec: {
                    dimensions: (@source['dimensions'] || []),
                    spatialDimensions: (@source['spacialDimensions'] || []),
                  }
                }
              },
              metricsSpec: (@source['metrics'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
              end + [{ type: "count", name: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
                intervals: ["#{@interval.first.iso8601}/#{@interval.last.iso8601}"],
              }
            },
            ioConfig: {
              type: 'hadoop',
              inputSpec: {
                type: 'static',
                paths: @paths.join(','),
              },
            },
            tuningConfig: {
              type: "hadoop",
              overwriteFiles: true,
              ignoreInvalidRows: true,
              buildV9Directly: true,
              forceExtendableShardSpecs: true,
              useCombiner: true,
              partitionsSpec: {
                type: "none",
              },
              indexSpec: {
                bitmap: {
                  type: @source['output']['bitmap'] || "roaring",
                },
                longEncoding: "auto",
              },
            },
          },
        }
        config[:spec][:tuningConfig][:partitionsSpec] = {
          type: "hashed",
          targetPartitionSize: -1,
          numShards: 10,
        }
        config
      end
    end
  end
end
