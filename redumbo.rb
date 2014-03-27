#!/usr/bin/env ruby
require 'bundler/setup'
require './lib/conf_loader'
require './lib/raw_locater'
require 'set'

base_dir = File.expand_path(File.dirname(__FILE__))
raw_file = File.join(base_dir, 'known_files.json')
raw_info = nil

config = load_config
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

config.each do |db, options|
  options[:reschema].each do |label, config|
    puts "#{db} #{label}:\t#{config[:start_time]} - #{config[:end_time]}"
    time_range = (config[:start_time].to_i..config[:end_time].to_i)

    file_counter = 0
    files_in_range = Set.new
    raw_info.each do |file_name, file_info|
      if (time_range.include? file_info['start']) or (time_range.include? file_info['end'])
        file_counter += 1
        if file_name.end_with?('.gz')
          file_parts = file_name.split('/')
          file_parts[-2] = "*"
          file_parts[-1] = "*.gz"
          files_in_range << file_parts.join('/')
        else
          files_in_range << file_name
        end
      end
    end

    puts "Time range has #{file_counter} files, compacting to #{files_in_range.size} patterns"

    segment_file = File.expand_path(File.join("~","#{db.to_s.tr('/', '-')}-#{label.to_s.tr('.','_')}.druid"))
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
      [[config[:start_time].iso8601, config[:end_time].iso8601].join('/')],
      files_in_range.to_a
    ))
  end
end
