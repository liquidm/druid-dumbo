require 'yaml'
require 'i18n/core_ext/hash'
require 'active_support'
require 'active_support/core_ext'

LIQUIDM = {
  offset: 1372543200,
  seed: %w{
    hdfs:///metrik/mysql.seed
  },
  legacy: %w{
    hdfs:///events
    hdfs:///history/ed_reports
  },
  camus: %w{
    /history/ed_bid_requests
    /history/ed_events
  },
}

class Time
  def floor(granularity = 1.hour)
    secs = self.to_i
    offset = secs % granularity
    Time.at(secs - offset)
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

    if reschema.size > 0
      now = Time.now
      reschema.sort!{ |a,b| a[:offset] <=> b[:offset] }


      reschema.each_with_index do |config, pos|

        unless reschema.size == pos + 1
          config[:start_time] = Time.at((now - config[:offset]).floor)
          config[:end_time] = (now - (reschema[pos + 1])[:offset]).floor
        else
          config[:start_time] = Time.at(LIQUIDM[:offset])
          config[:end_time] = Time.at((now - config[:offset]).floor)
        end
      end

      reschema << LIQUIDM
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
