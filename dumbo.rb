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

def scan_hdfs(path, delta)
  results = Hash.new{|hash, key| hash[key] = {files: [], counter: 0} }
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

      if (now - target_time < allowed_delta)
        result = results[target_time]
        result[:files] << fullname
        result[:counter] += event_count
      end
    end
  end

  results
end

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
  hdfs_content = scan_hdfs(options[:raw_input][:hdfs_path], options[:raw_input][:check_window_days])

  # skip first and last hour as they are usually incomplete
  hdfs_intervals = hdfs_content.keys.sort[1...-1].map{|check_time| [check_time, check_time + 1.hour]}

  begin
    query = druid.query(db_name)
              .time_series
              .long_sum(options[:segment_output][:counter_name])
              .granularity(:hour)
              .intervals(hdfs_intervals)

    delta = query.send.each do |druid_numbers|
      druid_count = druid_numbers[options[:segment_output][:counter_name]]
      delta_time = DateTime.parse(druid_numbers.timestamp)

      hdfs_count  = hdfs_content[delta_time][:counter]

      if hdfs_count != druid_count
        puts "DELTA_DETECTED #{({ dataSource: db_name, segment: delta_time, delta: hdfs_count - druid_count }.to_json)}"
        segment_file = File.join(base_dir, "#{db_name.sub('/', '_')}-#{Time.at(delta_time).strftime("%Y-%m-%d-%H")}.druid")
        puts "DELTA_JOBFILE #{segment_file}"
        IO.write(segment_file, render(
          template,
          db_name.split('/')[-1],
          options,
          [[delta_time, delta_time + 1.hour].join('/')],
          hdfs_content[delta_time][:files]
        ))

        job_config = JSON.parse(IO.read(segment_file))
        job_config.delete('partitionsSpec')
        IO.write(segment_file + ".fallback", job_config)

      end
    end
  rescue => e
    if options[:raw_input][:allow_full_rescan]
      puts "Doing a full rescan for #{db_name} as it doesn't seem to exist"
      intervals = hdfs_intervals
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

puts "DELTA_SCAN_COMPLETED"
