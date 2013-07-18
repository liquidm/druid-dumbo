#!/usr/bin/env ruby
require 'json'
require './lib/hdfs_scanner.rb'
require './lib/mysql_scanner.rb'

base_dir = File.dirname(__FILE__)

state_file_name = File.join(base_dir, 'hadoop_state.json')
hadoop_state = JSON.parse(IO.read(state_file_name)) rescue {}

hadoop_state.each do  |key,value|
  if value.nil? or value['skip']

    empty_file = value && value['size'] <= 20
    if empty_file
      puts "#{value.inspect} looks like an empty file, ignoring #{key}"
    else
      puts "Something seems wrong about #{key}, rescanning"
      hadoop_state.delete(key) unless empty_zip
    end
  end
end

hdfs = Druid::HdfsScanner.new :file_pattern => (ENV['DRUID_HDFS_FILEPATTERN'] || '/events/*/*/*/*/part*'), :cache => hadoop_state
hdfs.scan

raw_start, raw_end = hdfs.range

# save hdfs state early...
IO.write(state_file_name, hdfs.to_json)
puts "We got raw data from #{Time.at raw_start} to #{Time.at raw_end}"
