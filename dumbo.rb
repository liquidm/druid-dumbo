#!/usr/bin/env ruby
require 'bundler/setup'
require 'druid'
require 'set'

require './lib/conf_loader'
require './lib/raw_locater'
require './lib/mysql_scanner'

def render(template, data_source, conf, intervals, files)
  template.result(binding)
end

def files_for_timerange(raw_info, time_range)
  file_counter = 0
  files_in_range = Set.new

  raw_info.each do |file_name, file_info|
    segment_range = (file_info['start']..file_info['end'])

    if time_range.include?(segment_range.first) || segment_range.include?(time_range.first)
      file_counter += 1
      if file_name.end_with?('.gz')
        file_parts = file_name.split('/')
        file_parts[-1] = "*.gz"
        files_in_range << file_parts.join('/')
      else
        files_in_range << file_name
      end
    end
  end

  puts "#{file_counter} files, compacting to #{files_in_range.size} patterns"

  files_in_range.to_a
end

configs = load_config

base_dir = File.expand_path(File.dirname(__FILE__))
template_file = File.join(base_dir, 'importer.template')
unless File.exist?(template_file)
  template_file = "/etc/druid/importer.template"
end
puts "Reading template from #{template_file}"
template = ERB.new(IO.read(template_file))


delta_sum = 0
delta_files = 0
allowed_delta = 10

configs.each do |db, options|
  max_rescan_jobs = 4

  druid = Druid::Client.new(options[:zookeeper_uri], options[:druid_client])
  camus_current = Hash.new(0)
  hdfs_counters = Hash.new(0)
  raw_info = locate_raw_data(options)

  raw_info.each do |file_name, file_info|
    if file_info['source'] == 'camus'
      camus_current[file_info['location']] = [camus_current[file_info['location']], file_info['start']].max
    end
    if file_info['event_count']
      hdfs_counters[Time.at(file_info['start']).floor(1.hour)] += file_info['event_count']
    end
  end

  start_time = (Time.now - options[:raw_input][:check_window_days].days).floor
  end_time = Time.at(camus_current.values.min).floor(1.hour)

  puts "Scanning from #{start_time} to #{end_time}"
  puts "Max skew: #{Time.at(camus_current.values.max) - end_time}"

  query = druid.query(db)
    .time_series
    .long_sum(options[:segment_output][:counter_name])
    .granularity(:hour)
    .interval(start_time, end_time)
  puts query.to_json
  query.send.reverse.each do |druid_numbers|
    segment_start = Time.parse(druid_numbers.timestamp)
    segment_end = segment_start + 1.hour

    segment_start_string = druid_numbers.timestamp
    segment_end_string = segment_end.iso8601

    time_range = (segment_start.to_i...segment_end.to_i)

    druid_count = druid_numbers[options[:segment_output][:counter_name]] rescue 0
    hdfs_count  = hdfs_counters[segment_start]
    delta_count = (hdfs_count - druid_count).abs
    delta_sum += delta_count

    must_rescan = false

    if delta_count <= allowed_delta
      unless valid_segment_exist?(db, options[:database], options, options[:segment_output][:counter_name], segment_start_string, segment_end_string)
        puts "SCHEMA_MISMATCH #{{ dataSource: db, segment: segment_start_string}.to_json}"
        must_rescan = options[:reschema].size == 0 # reimport if no reschema is configured
      end
    else
      puts "DELTA_DETECTED #{({ dataSource: db, segment: segment_start_string, percent: (((druid_count * 100.0) / hdfs_count) - 100).round(2), delta: druid_count - hdfs_count, druid: druid_count, hdfs: hdfs_count}.to_json)}"
      must_rescan = true
    end

    if must_rescan
      delta_files += 1
      if max_rescan_jobs > 0
        max_rescan_jobs -= 1
        segment_file = File.expand_path(File.join("~","#{db.sub('/', '_')}-#{segment_start.strftime("%Y-%m-%d-%H")}.druid"))
        puts "Writing #{segment_file}"
        IO.write(segment_file, render(
            template,
            db.split('/')[-1],
            options,
            [[segment_start_string, segment_end_string].join('/')],
            files_for_timerange(raw_info, time_range)
          ))
      else
       puts "Skipping, too many jobs already"
      end
    end
  end

  max_reschema_jobs = 2
  options[:reschema].each do |label, config|
    puts "#{db} #{label}:\t#{config[:start_time]} - #{config[:end_time]}"

    config[:end_time].to_i.step(config[:start_time].to_i - 1.day, -1.day).each do |day|
      segment_start = [day, config[:start_time].to_i].max
      segment_end = day + 1.day

      segment_start_string = Time.at(segment_start).utc.iso8601
      segment_end_string = Time.at(segment_end).utc.iso8601
      time_range = (segment_start...segment_end)
      files_in_range = files_for_timerange(raw_info, time_range)

      if files_in_range.size == 0
        puts "No raw files found! Ignoring #{segment_start_string}."
      elsif segment_start == segment_end
        puts "Skipping #{day}, before start time"
      elsif not valid_segment_exist?(db, options[:database], config, options[:segment_output][:counter_name], segment_start_string, segment_end_string)
        if max_reschema_jobs > 0
          max_reschema_jobs -= 1


          if time_range.include?(options[:seed][:epoc]) and options[:seed][:seed_file]
            puts "Adding seed data to this segment"
            files_in_range += options[:seed][:seed_file]
          end

          segment_file = File.expand_path(File.join("~","#{db.to_s.tr('/', '-')}-#{label.to_s.tr('.','_')}-#{Time.at(segment_start).utc.strftime("%Y-%m-%d-%H")}.druid"))
          puts "Writing #{segment_file}"

          rescan_options = {}.merge(options)
          rescan_options[:segment_output][:segment_granularity] = :day
          rescan_options[:segment_output][:index_granularity] = config[:granularity]
          rescan_options[:metrics] = config[:metrics]
          rescan_options[:dimensions] = config[:dimensions]
          rescan_options[:spatialDimensions] = config[:spatialDimensions]

          IO.write(segment_file, render(
            template,
            db.split('/')[-1],
            rescan_options,
            [[segment_start_string, segment_end_string].join('/')],
            files_in_range
          ))
        else
          puts "Maximal number of jobs reached"
          break
        end
      else
        puts "Segment ok"
      end
    end
  end
end

puts "DELTA_SCAN_COMPLETED, CURRENTLY OFF BY #{delta_sum}"
puts "DELTA_COUNT #{delta_files}"
