require 'dumbo/task/base'

module Dumbo
  module Task
    class IndexHadoop < Base
      def initialize(source, interval, path, hadoop_version)
        @source = source
        @interval = interval
        @path = path
        @hadoop_version = hadoop_version
      end

      def as_json(options = {})
        config = {
          type: 'index_hadoop',
          hadoopDependencyCoordinates: ["org.apache.hadoop:hadoop-client:#{@hadoop_version}"],
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
                paths: @path,
              },
            },
            tuningConfig: {
              type: "hadoop",
              overwriteFiles: true,
              partitionsSpec: {
                type: "none",
              },
            },
          },
        }
        if (@source['output']['numShards'] || 0) > 1
          config[:spec][:tuningConfig][:partitionsSpec] = {
            type: "hashed",
            targetPartitionSize: -1,
            numShards: @source['output']['numShards'],
          }
        end
        config
      end
    end
  end
end
