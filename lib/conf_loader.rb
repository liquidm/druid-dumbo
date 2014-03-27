require 'yaml'
require 'i18n/core_ext/hash'
require 'active_support'
require 'active_support/core_ext'


class Time
  def floor(granularity = 1.day)
    secs = self.to_i
    offset = secs % granularity
    Time.at(secs - offset).utc
  end
end

def load_config
  base_dir = File.expand_path(File.join(File.dirname(__FILE__), '..'))

  conf_file = File.join(base_dir, 'dumbo.conf')
  unless File.exist?(conf_file)
    conf_file = "/etc/druid/dumbo.conf"
  end

  puts "Reading conf from #{conf_file}"
  raw_conf = YAML::load_file(conf_file).deep_symbolize_keys

  config = {
  }

  raw_conf[:db].each do |db_name, options|
    #db_name as symbol sucks
    db_name = db_name.to_s

    {
      dimensions: [],
      metrics: {},
      reschema: {},
      seed: raw_conf[:seed] || {},
      zookeeper_uri: "localhost:2181"
    }.each do |override_option, default_value|
      options[override_option] ||= (raw_conf[:default][override_option] || default_value)
    end

    # reschema overrides defaults *and* is rewritten
    reschema_raw = options[:reschema] || raw_conf[:default][:reschema] || {}
    reschema = []

    # rewrite keys like '2.weeks' and '6.months' into seconds
    reschema_raw.each do |timestamp, timed_schema|
      timestamp_pattern = timestamp.to_s.match(/(\d+)\.(.+)/)
      if timestamp_pattern
        offset = timestamp_pattern[1].to_i.send(timestamp_pattern[2])
        timed_schema[:offset] = offset
        timed_schema[:name] = timestamp
        reschema << timed_schema
      else
        puts "WARNING: Ignoring reschema #{timestamp} for #{db_name}"
      end
    end

    if reschema.size > 0
      now = Time.now
      reschema.sort!{ |a,b| a[:offset] <=> b[:offset] }

      reschema.each_with_index do |data_set, pos|
        unless reschema.size == pos + 1
          data_set[:start_time] = (now - (reschema[pos + 1])[:offset]).floor
          data_set[:end_time] = Time.at((now - data_set[:offset]).floor)
        else
          data_set[:start_time] = Time.at(options[:seed][:start_time])
          data_set[:end_time] = Time.at((now - data_set[:offset]).floor)
        end
      end
    end

    options[:reschema] = Hash[reschema.map{ |data_set| [data_set[:name], data_set] }]

    # these params augment defaults
    [
      :druid_client,
      :raw_input,
      :segment_output,
      :database,
    ].each do |option_group|
      options[option_group] = (raw_conf[:default][option_group] || {}).merge(options[option_group] || {})
    end

    config[db_name] = options
  end

  config
end
