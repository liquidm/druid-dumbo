require 'webhdfs'
require 'dumbo/time_ext'

module Dumbo
  class Firehose
    class HDFS

      def initialize(namenodes)
        [namenodes].flatten.each do |host|
          begin
            $log.info("connecting to", namenode: host)
            @hdfs = WebHDFS::Client.new(host, 50070)
            @hdfs.list('/')
            break
          rescue
            $log.info("failed to use", namenode: host)
            @hdfs = nil
          end
        end
        raise "no namenode is up and running" unless @hdfs
      end

      def slots(topic, interval)
        interval = interval.map { |t| t.floor(1.hour).utc }
        $log.info("scanning HDFS for", interval: interval)
        interval = (interval.first.to_i..interval.last.to_i)
        interval.step(1.hour).map do |time|
          Slot.new(@hdfs, topic, Time.at(time).utc).tap do |slot|
            #$log.debug("found slot at", time: slot.time, files: slot.paths.length, events: slot.events)
          end
        end.reject do |slot|
          slot.events.to_i < 1
        end
      end

      module Helper
        def topics
          @topics ||= clusters.map do |cluster|
            @hdfs.list("/history/#{cluster}").map do |entry|
              entry['pathSuffix'] if entry['type'] == 'DIRECTORY'
            end
          end.flatten.compact.sort.uniq
        end

        def clusters
          @clusters ||= @hdfs.list('/history').map do |entry|
            entry['pathSuffix'] if entry['type'] == 'DIRECTORY'
          end.flatten.compact.sort.uniq
        end
      end

      include Helper

      class Slot
        include Helper

        attr_reader :topic, :time, :paths, :events

        def initialize(hdfs, topic, time)
          @hdfs = hdfs
          @topic = topic
          @time = time
          @paths = paths!
          @events = @paths.map do |path|
            File.basename(path).split('.')[3].to_i
          end.reduce(:+)
        end

        def paths!
          clusters.map do |cluster|
            begin
              path = "/history/#{cluster}/#{@topic}/hourly/#{@time.strftime("%Y/%m/%d/%H")}"
              @hdfs.list(path).map do |entry|
                File.join(path, entry['pathSuffix']) if entry['pathSuffix'] =~ /\.gz$/
              end
            rescue
              nil
            end
          end.flatten.compact
        end
      end
    end
  end
end
