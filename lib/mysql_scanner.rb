require "jdbc/mysql"
require "java"
require "json"


def valid_segment_exist?(db_name, database, config, start_time, end_time)
  Jdbc::MySQL.load_driver
  puts "Scanning mysql for #{db_name} #{start_time}/#{end_time}"

  dimensions = config[:dimensions].map{ |dimension| dimension.to_s}.sort
  metrics = (config[:metrics].keys + ['events']).map{ |metric| metric.to_s}.sort

  matches = false

  begin
    connection = java.sql.DriverManager.get_connection(database[:uri], database[:user], database[:password])
    statement  = connection.create_statement

    query = %Q{
      SELECT
        payload
      FROM
        #{database[:table]}
      WHERE
        dataSource = #{db_name.split('/')[-1].to_json}
      AND
        str_to_date(start, "%Y-%m-%dT%T") >= str_to_date(#{start_time.to_json}, "%Y-%m-%dT%T")
      AND
        str_to_date(end, "%Y-%m-%dT%T") <= str_to_date(#{end_time.to_json}, "%Y-%m-%dT%T")
      AND
        used = 1
      ORDER BY
        start;
    }

    result_set = statement.execute_query(query);

    while (result_set.next) do
      payload = JSON.parse(result_set.getObject("payload"))

      segment_metrics = payload['metrics'].split(',').sort
      segment_dimensions = payload['dimensions'].split(',').sort

      metrics_match = segment_metrics == metrics
      dimensions_match = segment_dimensions == dimensions

      if (metrics_match && dimensions_match)
        matches = true
      else
        return false
      end
    end
  ensure
    statement.close
    connection.close
  end

  matches
end
