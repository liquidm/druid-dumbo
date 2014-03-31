#!/usr/bin/env ruby
require 'bundler/setup'
require 'set'

require './lib/conf_loader'
require './lib/raw_locater'
require './lib/mysql_scanner'

base_dir = File.expand_path(File.dirname(__FILE__))
raw_file = File.join(base_dir, 'known_files.json')
raw_info = nil

configs = load_config
if File.exist?(raw_file)
  puts "Reading #{raw_file}"
  raw_info = JSON.parse(IO.read(raw_file))
else
  puts "Scanning HDFS"
  raw_info = locate_raw_data(config['madvertise/events'])
  IO.write(raw_file, raw_info.to_json)
end

puts "#{raw_info.size} known files in HDFS total"

def render(template, data_source, conf, intervals, files)
  template.result(binding)
end

template_file = File.join(base_dir, 'importer.template')
unless File.exist?(template_file)
  template_file = "/etc/druid/importer.template"
end
puts "Reading template from #{template_file}"
template = ERB.new(IO.read(template_file))

raw_info.each do |file_name, file_info|
  file_info['time_range'] = (file_info['start']..file_info['end'])
end

max_jobs = 14

configs.each do |db, options|
  options[:reschema].each do |label, config|
    puts "#{db} #{label}:\t#{config[:start_time]} - #{config[:end_time]}"

    config[:end_time].to_i.step(config[:start_time].to_i - 1.day, -1.day).each do |day|
      segment_start = [day, config[:start_time].to_i].max
      segment_end = day + 1.day

      segment_start_string = Time.at(segment_start).utc.iso8601
      segment_end_string = Time.at(segment_end).utc.iso8601
      time_range = (segment_start...segment_end)

      unless valid_segment_exist?(db, options[:database], config, segment_start_string, segment_end_string)
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

        if max_jobs > 0
          max_jobs -= 1
          puts "Time range needs rescan, #{file_counter} files, compacting to #{files_in_range.size} patterns"

          if time_range.include?(options[:seed][:start_time])
            puts "Adding seed data to this segment"
            files_in_range += options[:seed][:seed_file]
          end

          segment_file = File.expand_path(File.join("~","#{db.to_s.tr('/', '-')}-#{label.to_s.tr('.','_')}-#{Time.at(segment_start).utc.to_date.iso8601.split(':')[0]}.druid"))
          puts "Writing #{segment_file}"

          rescan_options = {}.merge(options)
          rescan_options[:segment_output][:segment_granularity] = :day
          rescan_options[:segment_output][:index_granularity] = config[:granularity]
          rescan_options[:metrics] = config[:metrics]
          rescan_options[:dimensions] = config[:dimensions]

          IO.write(segment_file, render(
            template,
            db.split('/')[-1],
            rescan_options,
            [[segment_start_string, segment_end_string].join('/')],
            files_in_range.to_a
          ))
        else
          puts "Maximal number of jobs reached, exiting"
          exit
        end
      else
        puts "Segment ok"
      end
    end
  end
end
