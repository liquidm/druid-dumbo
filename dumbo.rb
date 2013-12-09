#!/usr/bin/env ruby
require 'bundler/setup'
require 'set'
require 'yaml'
require 'active_support'
require 'druid'
require 'erb'
require 'ostruct'
require 'active_support'
require 'active_support/core_ext'
require 'i18n/core_ext/hash'

def render(template, data_source, counter_name, conf, intervals, files)
  template.result(binding)
end

base_dir = File.dirname(__FILE__)
conf_name = File.join(base_dir, 'dumbo.conf')
conf = YAML::load_file(conf_name).deep_symbolize_keys

template_file = File.join(base_dir, 'importer.template')
template = ERB.new(IO.read(template_file))

druid = Druid::Client.new(conf[:zookeeper_uri], conf[:druid_options])

def scan_hdfs(path, delta)
  results = Hash.new{|hash, key| hash[key] = {files: [], events: 0} }
  now = Time.now
  allowed_delta = delta.days

  IO.popen("hadoop fs -ls #{path} 2> /dev/null") do |pipe|
    while str = pipe.gets
      fullname = str.split(' ')[-1]
      info = fullname.split('/')

      year = info[4].to_i
      month = info[5].to_i
      day = info[6].to_i
      hour = info[7].to_i

      event_count = info[-1].split('.')[3].to_i

      target_time = DateTime.new(year, month, day, hour) # assumes UTC

      if now - target_time < allowed_delta
        result = results[target_time]
        result[:files] << fullname
        result[:events] += event_count
      end
    end
  end

  results
end

conf[:db].each do |db_name, options|
  puts "Scanning #{db_name}"
  intervals = []
  files = []

  hdfs_content = scan_hdfs(options[:input][:path], conf[:check_window_days])

  hdfs_intervals = hdfs_content.keys.sort[1...-1].map{|check_time| [check_time, check_time + 1.hour]} # skip first and last hour 

  begin
    query = druid.query(db_name.to_s)
              .time_series
              .long_sum(conf[:counter_name])
              .granularity(:hour)
              .intervals(hdfs_intervals)

    delta = query.send.each do |druid_numbers|
      delta_time = DateTime.parse(druid_numbers.timestamp)

      hdfs_count  = hdfs_content[delta_time][:events]
      druid_count = druid_numbers[conf[:counter_name]]

      if hdfs_count != druid_count
        puts "Detected delta of #{hdfs_count - druid_count} events for #{db_name} at #{delta_time}"
        intervals << [delta_time, delta_time + 1.hour]
        files << hdfs_content[delta_time][:files]
      end
    end
  rescue => e
    if conf[:allow_full_rescan]
      puts "Doing a full rescan for #{db_name} as it doesn't seem to exist"
      intervals = hdfs_intervals
      files = hdfs_content.values.map{|c| c[:files]}.flatten
    else
      puts "Skipping #{db_name} thanks to #{e}"
    end
  end

  conf_file = File.join(base_dir, "#{db_name.to_s.sub('/', '_')}.druid")

  if intervals.length > 0
    IO.write(conf_file, render(
      template,
      db_name.to_s.split('/')[-1],
      conf[:counter_name],
      options,
      intervals,
      files
    ))
  else
    puts "Looks like #{db_name} is correct :)"
    File.delete(conf_file) if File.exist?(conf_file)
  end
end
