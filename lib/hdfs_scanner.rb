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

      if @enable_rescan and not info.nil?
        segment_range = (info['start'] .. info['end'])
        must_rescan = @files.any? do |name, hdfs_info|
          if hdfs_info['skip']
            false
          else
            hdfs_range = (hdfs_info['start'] .. hdfs_info['end'])

            if hdfs_range.include?(segment_range.begin) || segment_range.include?(hdfs_range.begin)
              invalid = (hdfs_info['created'] >= info['created'])
              puts "HDFS #{name} (created #{hdfs_info['created']}) invalidates druid segment #{info.inspect}" if invalid
              invalid
            else
              false
            end
          end
        end

        unless must_rescan
          return result
        end
      elsif not @enable_rescan and not info.nil?
        return result
      end

      @files.each do |name, hdfs_info|
        next if hdfs_info['skip']

        hdfs_range = (hdfs_info['start'] .. hdfs_info['end'])

        segment_range = (start .. start + 3600)
        if hdfs_range.include?(segment_range.begin) || segment_range.include?(hdfs_range.begin)
          result.push(name)
        end
      end

      puts "No druid segment for #{Time.at(start).utc}, will create using \n#{result.join("\n")}\n" unless result.empty?

      result
    end

    def scan
      pool = Thread::Pool.new(12)
      old_files = Set.new @files.keys
      folders = Hash.new {|hash, key| hash[key] = []}

      puts 'Scanning HDFS, this may take a while'

      IO.popen("hadoop fs -ls #{@file_pattern} 2> /dev/null") do |pipe|
        while str = pipe.gets
          info = str.split(' ')
          fullname = info[-1]
          parts = fullname.split('/')
          dir = parts[0...-1].join('/')
          name = parts[-1]
          size = info[4].to_i

          if name == "_SUCCESS" || name == "_temporary"
            folders[dir].unshift name
          elsif name.start_with? "part"
            folders[dir] << {
              :name => fullname,
              :size => size
            }
          end
        end
      end

      puts 'Scan completed, now checking content'

      folders.each do |dir, files|
        if files[0] == "_temporary"
          puts "#{dir} not completed yet, ignoring"
        elsif files[0] != "_SUCCESS"
          puts "#{dir} does not contain _SUCCESS, ignoring"
        else
          files[1..-1].each do |file|
            old_files.delete file[:name]
            scan_ls_row(pool, file[:name], file[:size])
          end
        end
      end

      old_files.each do |removed_file|
        puts "Purging #{removed_file} from cache, it's not in HDFS anymore"
        @files.delete removed_file
      end

      broken_files = []
      @files.each do |name, info|
        if info['skip'] == true
          puts "WARNING: #{name} is (marked) unparsable"
          broken_files.push name
          @files.delete name
        end
      end
      #uncomment this line to purge HDFS, otherwise the files will be rescanned next time
      #puts `hadoop fs -rm #{broken_files.join(' ')} 2> /dev/null` unless broken_files.length == 0

      pool.shutdown
    end

    def scan_ls_row(pool, name, size)
      existing_info = @files[name]
      if existing_info.nil? || (existing_info['size'].to_i != size)
        pool.process do
          begin
            first,last = pig_timestamps_in name

            # WARNING: don't use symbols as keys, going through to_json
            @lock.synchronize do
              @files[name] = {
                'size' => size,
                'start' => first,
                'end' => last,
                'created' => Time.now.to_i
              }
              puts "Found #{name}, #{@files[name]}"
            end
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
      start = (start / 3600.0).ceil * 3600
      puts "Reporting start as #{Time.at(start)} to ensure full hour"

      puts "Last hour in HDFS is #{Time.at(stop)}"
      stop = (stop / 3600.0).floor * 3600
      puts "Reporting end as #{Time.at(stop)} to ensure full hour"

      return start, stop
    end

    def to_json
      @files.to_json
    end

  end
end
