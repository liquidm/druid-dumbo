require "jdbc/mysql"
require "java"


def scan_mysql(db_name, database, config)
  Jdbc::MySQL.load_driver
  puts "Scanning mysql for valid segments"

  dimensions = config[:dimensions].sort
  metrics = config[:metrics].sort

  begin
    connection = java.sql.DriverManager.get_connection(database[:uri], database[:user], database[:password])
    statement  = connection.create_statement

    query = %Q{
      SELECT
        start, end, payload
      FROM
        #{database[:table]}
      WHERE
        start >= "#{config[:start_time].iso8601}"
      AND
        end <= "#{config[:end_time].iso8601}"
      AND
        used = 1
      SORT BY
        start;
    }

    result_set = statement.execute_query(query);

    while (result_set.next) do
      start_time = result_set.getObject("start")
      end_time = result_set.getObject("end")
      payload = JSON.parse(result_set.getObject("payload"))

      metrics_match = payload['metrics'].split(',').sort == metrics
      dimensions_match = payload['dimensions'].split(',').sort == dimensions

      unless (metrics_match && dimensions_match)
        puts "#{start_time}/#{end_time}"
      end
    end
  ensure
    statement.close
    connection.close
  end
end
