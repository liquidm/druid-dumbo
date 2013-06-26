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
    puts "#{value.inspect} doesn't look right, rescanning #{key}"
    hadoop_state.delete key
  end
end

hdfs = Druid::HdfsScanner.new :file_pattern => (ENV['DRUID_HDFS_FILEPATTERN'] || '/events/*/*/*/*/part*'), :cache => hadoop_state
hdfs.scan

raw_start, raw_end = hdfs.range

# save hdfs state early...
IO.write(state_file_name, hdfs.to_json)
puts "We got raw data from #{Time.at raw_start} to #{Time.at raw_end}"

segments = {}

ii = raw_start
while ii < raw_end
  segments[ii] = nil
  ii += 3600
end

data_source = ENV['DRUID_DATASOURCE']
s3_bucket = ENV['DRUID_S3_BUCKET']
s3_prefix = ENV['DRUID_S3_PREFIX']
s3_prefix = s3_prefix[1..-1] if s3_prefix[0] == '/' # Postel's law

segment_output_path = "s3n://#{s3_bucket}/#{s3_prefix}"

mysql = Druid::MysqlScanner.new :data_source => data_source


mysql.scan.each do |mysql_segment|
  start = mysql_segment['start']
  segments[start] = mysql_segment if segments.include? start
end

rescan_hours = Set.new
rescan_files = Set.new

max_hours = ENV['DRUID_MAX_HOURS_PER_JOB'].to_i

segments.keys.reverse.each do |start|
  info = segments[start]
  hdfs_files = hdfs.files_for start, info
  if (hdfs_files.length > 0)
    if (max_hours == 0 or rescan_hours.length < max_hours)
      rescan_hours.add start
      rescan_files.merge hdfs_files
    else
      puts "Job queue already worth #{max_hours}h, not scheduling #{start} in this run"
    end
  elsif info.nil?
    puts "No raw data available for #{Time.at(start). utc}, laggy HDFS importer?"
  end
end

intervals = rescan_hours.map do |time|
  "#{Time.at(time).utc.iso8601}/#{Time.at(time+3600).utc.iso8601}"
end
files = rescan_files.to_a

puts 'Writing druidimport.conf for batch ingestion'

IO.write(File.join(base_dir, 'druidimport.conf'), template.result(binding))

if rescan_files.empty?
  puts 'Nothing to scan, will exit 1 now.'
  exit 1
else
  puts 'And we are out. Hadoop, start your engines!'
end
