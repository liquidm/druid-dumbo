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
          type: 'index_parallel',
          spec: {
            dataSchema: {
              dataSource: @source['dataSource'],
              timestampSpec: {
                column: ((@source['input']['timestamp'] || {})['column'] || "timestamp"),
                format: ((@source['input']['timestamp'] || {})['format'] || "ruby"),
              },
              dimensionsSpec: {
                dimensions: (@source['dimensions'] || []),
                spatialDimensions: (@source['spacialDimensions'] || []),
              },
              metricsSpec: (@source['metrics'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
                # WARNING: do NOT use count for events, will count in segment vs count in raw input
              end + [{ type: "longSum", name: "events", fieldName: "events" }],
              granularitySpec: {
                segmentGranularity: @source['output']['segmentGranularity'] || "hour",
                queryGranularity: @source['output']['queryGranularity'] || "minute",
                intervals: [@interval],
                rollup: true,
              }
            },
            ioConfig: {
              type: 'index_parallel',
              inputSource: {
                type: 'druid',
                dataSource: @source['dataSource'],
                interval: @interval,
              }
            },
            tuningConfig: {
              type: "index_parallel",
              partitionsSpec: {
                type: "dynamic"
              },
              indexSpec: {
                bitmap: {
                  type: @source['output']['bitmap'] || "roaring",
                },
                longEncoding: "auto",
              },
              indexSpecForIntermediatePersists: {
                bitmap: {
                  type: 'roaring',
                  compressRunOnSerialization: false,
                },
                dimensionCompression: 'uncompressed',
                metricCompression: 'none',
                longEncoding: 'longs',
              },
              maxPendingPersists: 3,
              maxNumConcurrentSubTasks: 12,
              forceGuaranteedRollup: false
            },
          },
        }

        config
      end
    end
  end
end
