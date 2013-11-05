#!/usr/bin/env ruby
require 'set'
require 'json'
require 'erb'
require './lib/hdfs_scanner.rb'
require './lib/mysql_scanner.rb'

base_dir = File.dirname(__FILE__)

state_file_name = File.join(base_dir, 'hadoop_state.json')
template_file = File.join(base_dir, 'importer.template')

hadoop_state = JSON.parse(IO.read(state_file_name)) rescue {}
template = ERB.new(IO.read(template_file))

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

raw_start = [raw_start, (Date.today - 60).to_time.to_i].max
puts "Ensuring completeness of #{Time.at raw_start} to #{Time.at raw_end}"

segments = {}

ii = raw_start
while ii < raw_end
  segments[ii] = nil
  ii += 3600
end

data_source = ENV['DRUID_DATASOURCE']
segment_output_path = ENV['DRUID_OUTPUT_PATH'] || "/druid/deepstorage"

mysql = Druid::MysqlScanner.new :data_source => data_source

mysql.scan.each do |mysql_segment|
  start = mysql_segment['start']
  segments[start] = mysql_segment if segments.include? start
end


max_hours = ENV['DRUID_MAX_HOURS_PER_JOB'].to_i


jobs = []

segments.keys.reverse.each do |start|
  info = segments[start]
  hdfs_files = hdfs.files_for start, info
  if (hdfs_files.length > 0)
    jobs << {
    start: start,
    files: hdfs_files,
    }
  elsif info.nil?
    puts "No raw data available for #{Time.at(start). utc}, laggy HDFS importer?"
  end
end

puts "Saving jobs..."
IO.write('jobs.json', jobs.to_json)
puts "done"


jobs.inject(Hash.new {|hash, key| hash[key] = []}) do |stack, job|
  day = Time.at(job['start']).to_s.split(' ')[0]
  stack[day] << job
  stack
end.each do |day, day_jobs|
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

  conf = "druidimport-#{day}.conf"
  puts "Writing #{conf} for batch ingestion"

  IO.write(File.join(base_dir, conf), template.result(binding))
end
