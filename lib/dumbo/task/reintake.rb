require 'dumbo/task/base'

module Dumbo
  module Task
    class Reintake < Base
      def initialize(source, interval, paths)
        @source = source
        @interval = interval.map{|ii| ii.iso8601}.join("/")
        @paths = paths
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
              end + [{ type: "count", name: "events" }],
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
                type: 'hdfs',
                paths: @paths.join(','),
              },
              inputFormat: {
                type: 'json'
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
              maxPendingPersists: 1,
              maxNumConcurrentSubTasks: 2,
            },
          },
        }
        config
      end
    end
  end
end
