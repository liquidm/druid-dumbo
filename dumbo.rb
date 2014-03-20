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

def render(template, data_source, conf, intervals, files)
  template.result(binding)
end

base_dir = File.expand_path(File.dirname(__FILE__))

conf_file = File.join(base_dir, 'dumbo.conf')
unless File.exist?(conf_file)
  conf_file = "/etc/druid/dumbo.conf"
end
puts "Reading conf from #{conf_file}"
conf = YAML::load_file(conf_file).deep_symbolize_keys

template_file = File.join(base_dir, 'importer.template')
unless File.exist?(template_file)
  template_file = "/etc/druid/importer.template"
end
puts "Reading template from #{conf_file}"
template = ERB.new(IO.read(template_file))

def scan_hdfs(paths, delta, ignore_lag)
  results = Hash.new{|hash, key| hash[key] = {files: [], counter: 0} }
  now = Time.now
  allowed_delta = delta.days

  max_time = {}

  paths.split(',').each do |path|
    puts "Scanning HDFS at #{path}"
    IO.popen("hadoop fs -ls #{path} 2> /dev/null") do |pipe|
      while str = pipe.gets
        next if str.start_with?("Found")

        fullname = str.split(' ')[-1]
        info = fullname.split('/')

        year = info[4].to_i
        month = info[5].to_i
        day = info[6].to_i
        hour = info[7].to_i

        event_count = info[-1].split('.')[3].to_i

        target_time = DateTime.new(year, month, day, hour) # assumes UTC

        unless ignore_lag.include? path
          max_time[path] = [max_time[path], target_time].compact.max
        end

        if (now - target_time < allowed_delta)
          result = results[target_time]
          result[:files] << fullname
          result[:counter] += event_count
        end
      end
    end
  end

  max_time = max_time.values.min

  results.select do |result_time, result|
    if max_time && result_time > max_time
      puts "Ignoring #{result_time.to_time}, lag is currently at #{max_time.to_time}"
      false
    else
      true
    end
  end
end

delta_sum = 0
delta_count = 0

conf[:db].each do |db_name, options|
  #db_name as symbol sucks
  db_name = db_name.to_s

  # these params override defaults
  options[:zookeeper_uri] ||= conf[:default][:zookeeper_uri]  ||= "localhost:2181"
  options[:metrics]       ||= conf[:default][:metrics]        ||= {}
  options[:dimensions]    ||= conf[:default][:dimensions]     ||= []

  # these params augment defaults
  [
    :druid_client,
    :raw_input,
    :segment_output,
    :database,
  ].each do |option_group|
    options[option_group] = (conf[:default][option_group] || {}).merge(options[option_group] || {})
  end

  puts "Scanning #{db_name} on #{options[:zookeeper_uri]}"
  druid = Druid::Client.new(options[:zookeeper_uri], options[:druid_client])

  # this is what we want to populate
  intervals = []
  files = []

  # scan for camus files
  hdfs_content = scan_hdfs(options[:raw_input][:hdfs_path], options[:raw_input][:check_window_days], (options[:raw_input][:ignore_lag] || "").split(','))

  # skip first and last hour as they are usually incomplete
  hdfs_intervals = hdfs_content.keys.sort[1...-1].map{|check_time| [check_time, check_time + 1.hour]}

  hdfs_interval = [[hdfs_intervals[0][0], hdfs_intervals[-1][1]]]

  begin
    query = druid.query(db_name)
              .time_series
              .long_sum(options[:segment_output][:counter_name])
              .granularity(:hour)
              .intervals(hdfs_interval)

    puts query.to_json
    delta = query.send.each do |druid_numbers|
      druid_count = druid_numbers[options[:segment_output][:counter_name]] rescue 0
      delta_time = DateTime.parse(druid_numbers.timestamp)

      hdfs_count  = hdfs_content[delta_time][:counter] rescue 0

      if (hdfs_count - druid_count).abs > 10 && hdfs_count > 0 # druid seems buggy
        puts "DELTA_DETECTED #{({ dataSource: db_name, segment: delta_time, delta: druid_count - hdfs_count, druid: druid_count, hdfs: hdfs_count}.to_json)}"

        delta_sum += (druid_count - hdfs_count).abs
        delta_count += 1
        segment_file = File.join(base_dir, "#{db_name.sub('/', '_')}-#{Time.at(delta_time).strftime("%Y-%m-%d-%H")}.druid")
        IO.write(segment_file, render(
          template,
          db_name.split('/')[-1],
          options,
          [[delta_time, delta_time + 1.hour].join('/')],
          hdfs_content[delta_time][:files]
        ))

        job_config = JSON.parse(IO.read(segment_file))
        job_config.delete('partitionsSpec')
        IO.write(segment_file + ".fallback", job_config.to_json)
      else
        "puts NO_DELTA #{({ dataSource: db_name, segment: delta_time}.to_json)}"
      end
    end
  rescue => e
    if options[:raw_input][:allow_full_rescan]
      puts "Doing a full rescan for #{db_name} as it doesn't seem to exist"
      intervals = hdfs_interval
      files = hdfs_content.values.map{|c| c[:files]}.flatten
    else
      puts "Skipping #{db_name} thanks to #{e}"
    end
  end

  if intervals.length > 0
    job_file = File.join(base_dir, "#{db_name.sub('/', '_')}.druid")
    IO.write(job_file, render(
      template,
      db_name.split('/')[-1],
      options,
      intervals.map{|ii| ii.join('/')},
      files
    ))
  end
end

puts "DELTA_SCAN_COMPLETED, CURRENTLY OFF BY #{delta_sum}"
puts "DELTA_COUNT #{delta_count}"
