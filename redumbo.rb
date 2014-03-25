#!/usr/bin/env ruby
require 'bundler/setup'
require './lib/conf_loader'


config = load_config

config.each do |db, options|
  options[:reschema].each do |label, config|
    puts "#{db} #{label}:\t#{config[:start_time]} - #{config[:end_time]}"
  end
end
