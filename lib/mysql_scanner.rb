require "jdbc/mysql"
require "java"
require "json"

Jdbc::MySQL.load_driver

def valid_segment_exist?(db_name, database, config, counter_name, start_time, end_time)
  puts "Scanning mysql for #{db_name} #{start_time}/#{end_time}"

  dimensions = (config[:dimensions].map{ |dimension| dimension.to_s} + config[:spatialDimensions].map{|spatial| spatial[:dimName].to_s}).sort
  metrics = (config[:metrics].keys + [counter_name.to_s]).map{ |metric| metric.to_s}.sort

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

      unless metrics_match
        puts "METRICS IS\n#{segment_metrics}"
        puts "METRICS SHOULD BE\n#{metrics}"
      end

      unless dimensions_match
        puts "DIMENSIONS IS\n#{segment_dimensions}"
        puts "DIMENSIONS SHOULD BE\n#{dimensions}"
      end

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

def unused_segments(db_name, database)
  result = []
  begin
    connection = java.sql.DriverManager.get_connection(database[:uri], database[:user], database[:password])
    statement  = connection.create_statement

    query = %Q{
      SELECT
        id, payload
      FROM
        #{database[:table]}
      WHERE
        dataSource = #{db_name.split('/')[-1].to_json}
      AND
        used = 0
      ORDER BY
        start;
    }

    result_set = statement.execute_query(query);
    while (result_set.next) do
      segment = JSON.parse(result_set.getObject("payload"))
      segment['id'] = result_set.getObject("id")

      result << segment
    end
  ensure
    statement.close
    connection.close
  end

  result
end
