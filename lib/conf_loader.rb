require 'yaml'
require 'i18n/core_ext/hash'
require 'active_support'
require 'active_support/core_ext'

def conf
  base_dir = File.expand_path(File.join(File.dirname(__FILE__), '..'))

  conf_file = File.join(base_dir, 'dumbo.conf')
  unless File.exist?(conf_file)
    conf_file = "/etc/druid/dumbo.conf"
  end

  puts "Reading conf from #{conf_file}"
  raw_conf = YAML::load_file(conf_file).deep_symbolize_keys

  result = {}

  raw_conf[:db].each do |db_name, options|
    #db_name as symbol sucks
    db_name = db_name.to_s

    # these params override defaults
    options[:zookeeper_uri] ||= raw_conf[:default][:zookeeper_uri]  ||= "localhost:2181"
    options[:metrics]       ||= raw_conf[:default][:metrics]        ||= {}
    options[:dimensions]    ||= raw_conf[:default][:dimensions]     ||= []
    options[:reschema]      ||= raw_conf[:default][:reschema]       ||= {}

    # reschema overrides defaults *and* is rewritten
    reschema_raw = options[:reschema] || raw_conf[:default][:reschema] || {}
    reschema = []

    # rewrite keys like '2.weeks' and '6.months' into seconds
    reschema_raw.each do |timestamp, timed_schema|
      timestamp_pattern = timestamp.to_s.match(/(\d+)\.(.+)/)
      if timestamp_pattern
        offset = timestamp_pattern[1].to_i.send(timestamp_pattern[2])
        timed_schema[:offset] = offset
        reschema << timed_schema
      else
        puts "WARNING: Ignoring reschema #{timestamp} for #{db_name}"
      end
    end

    now = Time.now
    reschema.sort!{ |a,b| a[:offset] <=> b[:offset] }.reverse
    reschema[0][:start_time] = now - reschema[0][:offset]

    (1...reschema.size).each do |pos|
      reschema[pos][:start_time] = now - reschema[pos][:offset]
      reschema[pos][:end_time]= now - reschema[pos - 1][:offset]
    end

    # write it back as an array sorted by offset
    options[:reschema] = reschema

    # these params augment defaults
    [
      :druid_client,
      :raw_input,
      :segment_output,
      :database,
    ].each do |option_group|
      options[option_group] = (raw_conf[:default][option_group] || {}).merge(options[option_group] || {})
    end

    result[db_name] = options
  end

  return result
end
