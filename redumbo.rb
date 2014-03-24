#!/usr/bin/env ruby
require 'bundler/setup'
require './lib/conf_loader'

puts conf.to_yaml
