#!/usr/bin/env ruby
require 'bundler/setup'
require './lib/conf_loader'

puts load_config.to_yaml
