require 'dumbo/task/base'

module Dumbo
  module Task
    class Merge < Base
      def initialize(topic, interval, segments)
        @topic = topic
        @interval = interval
        @segments = segments
      end

      def as_json(options = {})
        {
          type: 'merge',
          dataSource: @topic,
          segments: @segments,
        }
      end
    end
  end
end
