#!/usr/bin/env ruby
require 'bundler/setup'

require './lib/conf_loader'
require './lib/mysql_scanner'
require 'webhdfs'

configs = load_config

def backup_hdfs(folder, backup_root) 
  #hdfs_content = %x{hdfs dfs -ls #{folder} 2>/dev/null}.split("\n")

  hdfs_content = $hdfs.list(folder)

  hdfs_content.each do |source|
    next unless source['type'] == 'FILE'
    source_size = source['length']
    source_name = File.join(folder, source['pathSuffix'])

    target_name = File.join(backup_root, source_name)
    target_size = File.size(target_name) rescue 0

    if target_size != source_size
      if system(%Q{ mkdir -p #{File.expand_path('..', target_name)} && hdfs dfs -copyToLocal #{source_name} #{target_name} 2>/dev/null})
        puts "Added #{source_name}"
      else
        raise "Error copying #{source_name}"
      end
    else
      puts "Skipping #{source_name}, already exists!"
    end
  end
end

configs.each do |db, options|
  options[:hdfsNamenodes].each do |host|
    begin
      $hdfs = WebHDFS::Client.new(host, 50070)
      $hdfs.list('/')
      break
    rescue WebHDFS::IOError
      $hdfs = nil
    end
  end
  raise "no namenode is up and running" unless $hdfs

  segments = used_segments(db, options[:database], true)
  segments.each do |segment|
    type = segment['loadSpec']['type']

    if type == "hdfs"
      backup_hdfs(File.expand_path('..',  segment['loadSpec']['path']), options[:backupFolder])
    end

  end
end
