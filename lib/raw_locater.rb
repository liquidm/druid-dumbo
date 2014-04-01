require 'json'
require 'time'

def locate_raw_data(db_config)
  seed = db_config[:seed]

  result = {}

  seed[:legacy_data].each do |info_file|
    puts "Reading legacy info at #{info_file}"
    legacy_files = JSON.parse(%x{hadoop fs -cat #{info_file} 2>/dev/null})
    puts "Found #{legacy_files.size} files"
    result.merge!(legacy_files)
  end if seed[:legacy_data]

  %w{camus_legacy camus}.each do |source|
    seed[source.to_sym].each do |location|
      puts "Scanning folders at #{location}"
      location_counter = 0
      IO.popen("hadoop fs -ls #{location} 2> /dev/null") do |pipe|
        while str = pipe.gets
          next if str.start_with?("Found")

          location_counter += 1
          fullname = str.split(' ')[-1]
          info = fullname.split('/')

          year = info[4].to_i
          month = info[5].to_i
          day = info[6].to_i
          hour = info[7].to_i

          event_count = info[-1].split('.')[3].to_i

          target_time = DateTime.new(year, month, day, hour)

          result[fullname] = {
            'start' => target_time.to_time.to_i,
            'end' => target_time.to_time.to_i,
            'source' => source,
            'location' => location,
            'event_count' => event_count,
          }
        end
      end
      puts "Found #{location_counter} files"
    end if seed[source.to_sym]
  end

  result.each do |file_name, file_info|
    file_info['time_range'] = (file_info['start']..file_info['end'])
  end

  result
end
