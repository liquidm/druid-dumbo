#!/usr/bin/env ruby
require 'bundler/setup'

require './lib/conf_loader'
require './lib/mysql_scanner'

configs = load_config

configs.each do |db, options|
  unused = used_segments(db, options[:database], false)

  puts "#{db}: #{unused.inject(0) { |sum, payload| sum + payload['size'] }}"
end
