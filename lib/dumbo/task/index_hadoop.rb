require 'dumbo/task/base'

module Dumbo
  module Task
    class IndexHadoop < Base
      def initialize(topic, interval, source, paths)
        @topic = topic
        @interval = interval
        @source = source
        @paths = paths
      end

      def as_json(options = {})
        interval = "#{@interval.first.iso8601}/#{@interval.last.iso8601}"
        {
          type: 'index_hadoop',
          spec: {
            dataSchema: {
              dataSource: @topic,
              parser: {
                parseSpec: {
                  format: "json",
                  timestampSpec: {
                    column: ((@source['timestamp'] || {})['column'] || "ts"),
                    format: ((@source['timestamp'] || {})['format'] || "iso"),
                  },
                  dimensionsSpec: {
                    dimensions: (@source['dimensions'] || []),
                    spatialDimensions: (@source['spacialDimensions'] || []),
                  }
                }
              },
              metricsSpec: (@source['aggregators'] || {}).map do |name, aggregator|
                { type: aggregator, name: name, fieldName: name }
              end + [{ type: "count", name: "events" }],
              granularitySpec: {
                segmentGranularity: "hour",
                queryGranularity: "minute",
                intervals: [interval],
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
              partitionsSpec: {
                type: "none",
              },
            },
          },
        }
      end
    end
  end
end
