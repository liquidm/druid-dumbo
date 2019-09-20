require 'dumbo/task/base'

module Dumbo
  module Task
    class CompactSegments < Base
      def initialize(source, interval)
        @source = source
        @source['input'] ||= {}
        @source['input']['timestamp'] ||= {}
        @interval = interval.map{|ii| ii.iso8601}.join("/")
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
                    column: (@source['input']['timestamp']['column'] || 'timestamp'),
                    format: (@source['input']['timestamp']['format'] || 'ruby'),
                  },
                  dimensionsSpec: {
                    dimensions: (@source['dimensions'] || []),
                    spatialDimensions: (@source['spacialDimensions'] || []),
                  }
                }
              },
              metricsSpec: (@source['metrics'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
              # WARNING: do NOT use count for events, will count in segment vs count in raw input
              end + [{ type: "longSum", name: "events", fieldName: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
                intervals: [@interval],
              }
            },
            ioConfig: {
              type: 'hadoop',
              inputSpec: {
                type: 'dataSource',
                ingestionSpec: {
                  type: 'dataSource',
                  dataSource: @source['dataSource'],
                  interval: @interval,
                  granularity: @source['output']['queryGranularity'] || "minute",
                },
              },
            },
            tuningConfig: {
              type: "hadoop",
              overwriteFiles: true,
              ignoreInvalidRows: true,
              buildV9Directly: true,
              useCombiner: true,
              forceExtendableShardSpecs: true,
              partitionsSpec: {
                type: "hashed",
                numShards: @source['output']['numShards'] || 3
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

        if @source['output']['targetPartitionSize']
          config[:spec][:tuningConfig][:partitionsSpec][:targetPartitionSize] = @source['output']['targetPartitionSize']
          config[:spec][:tuningConfig][:partitionsSpec][:numShards] = -1
        end

        # if @source['output']['partitionDimension']
        #   config[:spec][:tuningConfig][:partitionsSpec] = {
        #     type: "dimension",
        #     partitionDimension: @source['output']['partitionDimension']
        #     targetPartitionSize: (@source['output']['targetPartitionSize'] || 419430400)
        #   }
        # end


        config
      end
    end
  end
end
