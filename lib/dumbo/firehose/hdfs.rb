require 'fileutils'
require 'webhdfs'
require 'webhdfs/fileutils'
require 'dumbo/time_ext'

module Dumbo
  class Firehose
    class HDFS
      def initialize(namenodes, sources)
        @hdfs = []
        [namenodes].flatten.each do |host|
          begin
            $log.info("connecting to", namenode: host)
            hdfs = WebHDFS::Client.new(host, 50070)
            hdfs.list('/')
            @hdfs << hdfs
          rescue
            $log.info("failed to use", namenode: host)
          end
        end
        raise "no namenode is up and running" if @hdfs.empty?
        @slots = {}
        @sources = sources
      end

      def slots(topic, interval)
        @slots["#{topic}_#{interval}"] ||= slots!(topic, interval)
      end

      def slots!(topic, interval)
        $log.info("scanning HDFS for", interval: interval)
        enumerable_interval(interval).map do |time|
          SlotOptions.new(@sources, @hdfs, topic, Time.at(time).utc).best_slot
        end.reject do |slot|
          slot.events.to_i < 1
        end
      end

      def slots_options!(topic, interval)
        $log.info("scanning HDFS options for", interval: interval)
        enumerable_interval(interval).map do |time|
          SlotOptions.new(@sources, @hdfs, topic, Time.at(time).utc)
        end.reject do |slot|
          slot.best_slot.events.to_i < 1
        end
      end

      def enumerable_interval(interval)
        interval = interval.map { |t| t.floor(1.hour).utc }
        interval = (interval.first.to_i..interval.last.to_i)
        interval.step(1.hour)
      end

      class SlotOptions
        attr_reader :topic, :time

        def initialize(sources, hdfs_servers, topic, time)
          @sources = sources
          @hdfs_servers = hdfs_servers
          @topic = topic
          @time = time
          @all = all!
        end

        def all!
          @hdfs_servers.map do |hdfs|
            Slot.new(@sources, hdfs, @topic, @time)
          end
        end

        def best_slot
          @all.max_by(&:events)
        end

        def synchronize!(dryrun = true)
          unless @hdfs_servers.size > 1
            $log.info("tried to sync but there are no enough hdfs servers")
            return
          end

          all_events = @all.map(&:events).uniq
          unless all_events.size > 1
            $log.info("slot already in sync", time: @time, source: @topic)
            return
          end

          unless dryrun
            $log.info("downloading best option", time: @time, from: best_slot.hdfs.host)
            WebHDFS::FileUtils.set_server(best_slot.hdfs.host, 50070) unless dryrun

            paths = best_slot.paths
            FileUtils.remove_entry_secure('/tmp/druid-dumbo')
            FileUtils.mkdir '/tmp/druid-dumbo'

            paths.each do |path|
              file = path.split('/')[-1]
              WebHDFS::FileUtils.copy_to_local(path, "/tmp/druid-dumbo/#{file}")
            end
          end

          @all.each do |option|
            next if option == best_slot
            next if option.events == best_slot.events

            $log.info("found differences between options", delta: (best_slot.events - option.events), best: best_slot.events, current: option.events)

            unless dryrun
              $log.info("|-- deleting data", at: option.hdfs.host)
              option.paths.each do |path|
                option.hdfs.delete(path)
              end

              $log.info("|-- uploading best option", to: option.hdfs.host)
              folder = option.pattern.split('/')[0..-2].join('/')
              WebHDFS::FileUtils.set_server(option.hdfs.host, 50070)

              paths.each do |path|
                file = path.split('/')[-1]
                $log.info("|---- copying", file: file, folder: folder, from: best_slot.hdfs.host, to: option.hdfs.host)
                WebHDFS::FileUtils.copy_from_local("/tmp/druid-dumbo/#{file}", "#{folder}/#{file}")
              end

              $log.info("|-- sync done", at: option.hdfs.host)
            end
          end
        end
      end

      class Slot
        attr_reader :hdfs, :topic, :time, :paths, :events

        def initialize(sources, hdfs, topic, time)
          @sources = sources
          @hdfs = hdfs
          @topic = topic
          @time = time
          @paths = paths!
          @events = @paths.map do |path|
            File.basename(path).split('.')[3].to_i
          end.reduce(:+)
        end

        def pattern
          @paths.map do |path|
            tokens = path.split('/')
            suffix = tokens[-1].split('.')
            tokens[-1] = "*.#{suffix[-1]}"
            tokens.join('/')
          end.compact.uniq.sort.first
        end

        def paths!
          begin
            @sources[@topic]['input']['camus'].map do |hdfs_root|
              path = "#{hdfs_root}/hourly/#{@time.strftime("%Y/%m/%d/%H")}"
              begin
                @hdfs.list(path).map do |entry|
                  File.join(path, entry['pathSuffix']) if entry['pathSuffix'] =~ /\.gz$/
                end
              rescue => e
                $log.warn("No events in #{path} at #{@hdfs.host}, ignoring")
                nil
              end
            end.flatten.compact
          rescue
            $log.error("#{@topic} -> input.camus must be an array of HDFS paths")
            exit 1
          end
        end
      end
    end
  end
end
