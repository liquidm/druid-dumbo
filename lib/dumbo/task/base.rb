module Dumbo
  module Task
    class Base
      attr_reader :id

      def run!(overlord)
        raise "cannot run same task twice" if @overlord

        uri = URI(overlord)
        req = Net::HTTP::Post.new(uri.path, { 'Content-Type' => 'application/json' })
        req.body = self.to_json

        response = Net::HTTP.new(uri.host, uri.port).start do |http|
          http.read_timeout = nil # we wait until druid is finished
          http.request(req)
        end

        if response.code.start_with? '3'
          $log.info("found redirect to active overlord: #{response['Location']}")
          return run!(response['Location'])
        end

        if response.code != '200'
          puts uri
          puts inspect
          puts response.inspect
          raise "request failed"
        end

        @overlord = overlord
        @id = MultiJson.load(response.body)["task"]
      end

      def finished?
        uri = URI(@overlord + "/#{@id}/status")

        req = Net::HTTP::Get.new(uri.path, { 'Content-Type' => 'application/json' })

        response = Net::HTTP.new(uri.host, uri.port).start do |http|
          http.read_timeout = nil # we wait until druid is finished
          http.request(req)
        end

        if response.code.start_with? '3'
          $log.info("found redirect to active overlord: #{response['Location']}")
          return finished?(response['Location'])
        end

        if response.code != '200'
          puts uri
          puts inspect
          puts response.inspect
          raise "request failed"
        end

        MultiJson.load(response.body)["status"]["status"] != "RUNNING"
      end

      def generate!
        File.write(output, self.to_json)
      end

      def to_json
        MultiJson.dump(self.as_json)
      end

      def inspect
        "#{self.class.to_s}\n#{self.to_json}"
      end

      def output
        "#{self.class.to_s.demodulize.underscore}-#{@topic}-#{@interval.first.iso8601}.json"
      end

      class Error < StandardError
        attr_reader :response
        def initialize(response)
          @response = response
        end

        def message
          MultiJson.load(response.body)["error"]
        end
      end

    end
  end
end
