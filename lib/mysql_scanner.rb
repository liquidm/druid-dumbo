require 'mysql'
require 'time'
require 'json'

module Druid
  class MysqlScanner
    def initialize(opts = {})
      @data_source = opts[:data_source] || ENV['DRUID_DATASOURCE'] || raise('Must pass a :data_source param')
      @host = opts[:host] || ENV['DRUID_MYSQL_HOST'] || 'localhost'
      @user = opts[:user] || ENV['DRUID_MYSQL_USER'] || 'druid'
      @password = opts[:password] || ENV['DRUID_MYSQL_PASSWORD']
      @db_name = opts[:db] || ENV['DRUID_MYSQL_DB'] || 'druid'
      @table_name = opts[:table] || ENV['DRUID_MYSQL_TABLE'] || 'segments'

      @db = Mysql::new(@host, @user, @password, @db_name)
    end

    def scan
      ranges = []
      marker = ''

      puts 'Scanning mysql...'
      @db.query("select payload from #{@table_name} where used = 1 and dataSource = #{@data_source.to_json} and payload like '%druid06%'").each do |row|
        descriptor = JSON.parse(row[0])
        if descriptor['dataSource'] == @data_source
          interval = descriptor['interval'].split('/')

          ranges.push({
            'start' => Time.parse(interval[0]).to_i,
            'end' => Time.parse(interval[1]).to_i,
            'created' => Time.parse(descriptor['version']).to_i
          })
        else
          puts "Skipping #{descriptor} because it does not match #{@data_source}"
        end
      end
      puts 'Scanning mysql completed'
      ranges
    end

  end
end
