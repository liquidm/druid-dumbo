require 'json'
require 'time'
require 'thread'
require 'thread/pool'
require 'set'
require 'tempfile'

module Druid
  class HdfsScanner

    def initialize(opts = {})
      @file_pattern = opts[:file_pattern] || raise('Must pass :file_pattern param')
      @files = opts[:cache] || {}
      @enable_rescan = opts[:enable_rescan] || (ENV['DRUID_RESCAN'].to_i > 0)
      @lock = Mutex.new
    end

    def files_for(start, info)
      result = []
      @files.each do |name, hdfs_info|
        if info.nil?
          if (hdfs_info['start'] .. hdfs_info['end']).cover? start
            puts "No S3 segment for #{Time.at(start).utc}, need to work on #{name}"
            result.push(name)
          end
        elsif hdfs_info['start'] >= info['end'] or hdfs_info['end'] <= info['start']
          next
        elsif hdfs_info['created'] >= info['created']
          if @enable_rescan
            puts "HDFS is newer than S3 need to recheck #{Time.at(start).utc} using #{name}"
            result.push(name)
          else
            puts "HDFS is newer than S3 for #{Time.at(start).utc}, but rescan not enabled, skipping #{name}"
          end
        end
      end
      result
    end

    def scan
      pool = Thread::Pool.new(6)
      old_files = Set.new @files.keys

      puts 'Scanning HDFS, this may take a while'
      IO.popen("bash -c \"TZ=utc hadoop fs -ls #{@file_pattern}\" 2>/dev/null") do |pipe|
        while str = pipe.gets
          info = str.split(' ')

          size = info[4].to_i
          name = info[7]
          cdate = Time.parse("#{info[5]} #{info[6]} +0000").to_i

          old_files.delete name
          scan_ls_row(pool, name, size, cdate)
        end
      end

      old_files.each do |removed_file|        
        puts "Purging #{removed_file} from cache, it's not in HDFS anymore"
        @files.delete removed_file
      end

      broken_files = []
      @files.each do |name, info|
        if info['skip'] == true
          puts "#{name} is unparsable, removing from HDFS"
          broken_files.push name
          @files.delete name
        end
      end
      puts `hadoop fs -rm #{broken_files.join(' ')} 2> /dev/null` unless broken_files.length == 0

      pool.shutdown
    end

    def scan_ls_row(pool, name, size, cdate)
      existing_info = @files[name]
      if existing_info.nil? || (existing_info['size'].to_i != size)
        pool.process do
          begin
            first,last = pig_timestamps_in name

            puts "Scanned #{name}, found data between #{Time.at first} and #{Time.at last}"

            # WARNING: don't use symbols as keys, going through to_json
            @lock.synchronize do
              @files[name] = {
                'size' => size,
                'start' => first,
                'end' => last,
                'created' => cdate
              }
            end
            puts "Found #{name}, #{@files[name]}"
          rescue => e
            @lock.synchronize do
              @files[name] = {
                'size' => size,
                'skip' => true,
                'cause' => e.to_s
              }
            end
            puts "Skipping #{name} for #{e}"
          end
        end
      end
    end

    def pig_timestamps_in(name)
      pig_script = Dir.glob(File.join(File.dirname(__FILE__), '..', 'contrib', '*')).map{|jar| "register '#{File.expand_path(jar)}';"}.join("\n") + %Q[
        data = load '#{name}' using com.twitter.elephantbird.pig.load.JsonLoader() as (json: map[]);
        cleaned =  foreach data generate (double) json#'timestamp' as timestamp;
        grouped = GROUP cleaned ALL;
        result = FOREACH grouped GENERATE (long) MIN(cleaned.timestamp) as start, (long) MAX(cleaned.timestamp) as stop;
        dump result;
      ]
      script = Tempfile.new('dumbo')
      script.write pig_script
      script.close

      `pig #{script.path} 2>/dev/null`.match(/\((\d+),(\d+)\)/)[1..-1].map{|ts| ts.to_i}
    end

    def range
      start = Float::INFINITY
      stop = 0

      @files.each do |name, info|
        next if info['skip']
        start = [start, info['start']].min
        stop = [stop, info['end']].max
      end

      puts "First hour in HDFS is #{Time.at(start)}"
      start += 3600
      puts "Reporting start as #{Time.at(start)} to ensure full hour"

      puts "Last hour in HDFS is #{Time.at(stop)}"
      stop -= 3600
      puts "Reporting end as #{Time.at(stop)} to ensure full hour"

      return start, stop
    end

    def to_json
      @files.to_json
    end

  end
end
