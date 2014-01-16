#!/usr/bin/env ruby
require 'set'
require 'json'
require 'erb'
require './lib/hdfs_scanner.rb'
require './lib/mysql_scanner.rb'
require 'time'

MAX_JOBS=24

base_dir = File.dirname(__FILE__)

state_file_name = File.join(base_dir, 'hadoop_state.json')

hadoop_state = JSON.parse(IO.read(state_file_name)) rescue {}

hadoop_state.each do  |key,value|
  if value.nil? or value['skip']
    puts "Ignoring #{key}"
    hadoop_state.delete(key)
  end
end

hdfs = Druid::HdfsScanner.new :file_pattern => (ENV['DRUID_HDFS_FILEPATTERN'] || '/events/*/*/*/*'), :cache => hadoop_state
hdfs.scan

raw_start, raw_end = hdfs.range

# save hdfs state early...
IO.write(state_file_name, hdfs.to_json)
puts "We got raw data from #{Time.at raw_start} to #{Time.at raw_end}"

raw_start = [raw_start, (Date.today - 90).to_time.to_i].max
raw_end = [raw_end, (Time.now.to_i / 3600 * 3600)].min

raw_start = Time.parse(ENV['DRUID_OVERRIDE_START']).to_i if ENV['DRUID_OVERRIDE_START']
raw_end = Time.parse(ENV['DRUID_OVERRIDE_END']).to_i if ENV['DRUID_OVERRIDE_END']

puts "Ensuring completeness of #{Time.at raw_start} to #{Time.at raw_end}"

segments = {}

ii = raw_start
while ii < raw_end
  segments[ii] = nil
  ii += 3600
end

data_sources = ENV['DRUID_DATASOURCE'].split(',')
segment_output_path = ENV['DRUID_OUTPUT_PATH'] || "/druid/deepstorage"


whitelist = Set.new

(ENV['DRUID_WHITELIST'] || "").split(',').each do |white_range|
  start, stop = white_range.split('/').map {|t| Time.parse(t).to_i / 3600 * 3600 }
  start.step(stop-1, 3600) do |hour| # right hand side non-inclusive, hence -1
    whitelist << Time.at(hour).to_i
  end
end

data_sources.each do |data_source|
  template_file = File.join(base_dir, "#{data_source}-importer.template")
  template = ERB.new(IO.read(template_file))
  mysql = Druid::MysqlScanner.new :data_source => data_source

  mysql.scan.each do |mysql_segment|
    start = mysql_segment['start']
    segments[start] = mysql_segment if segments.include? start
  end

  jobs = []

  segments.keys.reverse.each do |start|
    if jobs.size < MAX_JOBS
      info = segments[start]
      hdfs_files = hdfs.files_for start, info
      if (hdfs_files.length > 0)
        if (whitelist.include? start)
          puts "Skipping whitelisted #{Time.at(start).utc}"
        else
          puts "Scheduling rescan of #{Time.at(start).utc} on #{data_source}"
          jobs << {
            'start' => start,
            'files' => hdfs_files,
          }
        end
      elsif info.nil?
        puts "No raw data available for #{Time.at(start).utc}, laggy HDFS importer?"
      end
    else
      puts "Enough jobs for today"
      break
    end
  end

  puts "Saving jobs..."
  IO.write('jobs.json', jobs.to_json)
  puts "done"

  jobs.inject(Hash.new {|hash, key| hash[key] = []}) do |stack, job|
    slice = Time.at(job['start']).strftime("%Y-%m-%d-%H")
    stack[slice] << job
    stack
  end.each do |slice, day_jobs|
    rescan_hours = Set.new
    rescan_files = Set.new
    day_jobs.each do |job|
      rescan_hours.add job['start']
      rescan_files.merge job['files']
    end
    intervals = rescan_hours.map do |time|
      "#{Time.at(time).utc.iso8601}/#{Time.at(time+3600).utc.iso8601}"
    end
    files = rescan_files.to_a

    conf = "druidimport-#{data_source}-#{slice}.conf"
    puts "Writing #{conf} for batch ingestion"

    IO.write(File.join(base_dir, conf), template.result(binding))
  end
end
