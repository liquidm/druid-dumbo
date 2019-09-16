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
          type: 'compact',
          dataSource: @source['dataSource'],
          interval: @interval,
          dimensionsSpec: {
            dimensions: (@source['dimensions'] || []),
            spatialDimensions: (@source['spacialDimensions'] || []),
          },
          metricsSpec: (@source['metrics'] || {}).map do |name, aggregator|
            { type: aggregator, name: name, fieldName: name }
            # WARNING: do NOT use count for events, will count in segment vs count in raw input
          end + [{ type: "longSum", name: "events", fieldName: "events" }],
          segmentGranularity: @source['output']['segmentGranularity'] || "hour",
          targetCompactionSizeBytes: 419430400,
          tuningConfig: {
            type: "index",
            partitionDimensions: [@source['output']['partitionDimension']],
            indexSpec: {
              bitmap: {
                type: @source['output']['bitmap'] || "roaring",
              },
              longEncoding: "auto",
            },
          },
        }

        config
      end
    end
  end
end
