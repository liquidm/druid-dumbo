module Dumbo
  class OverlordScanner
    def initialize(overlord)
      @overlord = overlord
      @current_jobs = nil
    end

    def current_jobs
      if @current_jobs
        $log.info("Using cached copy of current jobs")
      else
        @current_jobs = {}
        %w{running pending waiting}.each do |type|
          @current_jobs.merge! fetch_tasks("http://#{@overlord}/druid/indexer/v1/#{type}Tasks", type)
        end
      end
      @current_jobs
    end

    def fetch_tasks(url, type)
      uri = URI(url)
      req = Net::HTTP::Get.new(uri.path, { 'Content-Type' => 'application/json' })
      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = nil # we wait until druid is finished
        http.request(req)
      end
      if response.code.start_with? '3'
        fetch_tasks(response['Location'], type)
      elsif response.code.start_with? '2'
        Hash[JSON.parse(response.body).map do |task_info|
          fetch_task_info(uri.host, uri.port, task_info['id'], type)
        end.compact]
      else
        raise response
      end
    end

    def fetch_task_info(host, port, task_id, type)
      req = Net::HTTP::Get.new("/druid/indexer/v1/task/#{CGI.escape(task_id)}", { 'Content-Type' => 'application/json' })
      response = Net::HTTP.new(host, port).start do |http|
        http.read_timeout = nil # we wait until druid is finished
        http.request(req)
      end
      if response.code.start_with? '2'
        spec = JSON.parse(response.body)
        [task_id, {dataSource: spec['payload']['dataSource'], intervals: spec['payload']['spec']['dataSchema']['granularitySpec']['intervals'], type: type, shutdown: "http://#{host}:#{port}/druid/indexer/v1/task/#{CGI.escape(task_id)}/shutdown"}]
      else
        nil
      end
    end

    def kill_duplicates
      source_interval = {}
      current_jobs.each do |id, meta|
        key = "#{meta[:dataSource]}-#{meta[:intervals][0]}}"
        if source_interval[key]
          puts "#{key} is duplicate, currently #{meta[:type]}"
          uri = URI(meta[:shutdown])
          req = Net::HTTP::Post.new(uri.path)
          response = Net::HTTP.new(uri.host, uri.port).start do |http|
            http.read_timeout = nil # we wait until druid is finished
            http.request(req)
          end
          puts response.code
        else
          source_interval[key] = meta
        end
      end
      true
    end

    def kill_all
      current_jobs.each do |id, meta|
        uri = URI(meta[:shutdown])
        req = Net::HTTP::Post.new(uri.path)
        response = Net::HTTP.new(uri.host, uri.port).start do |http|
          http.read_timeout = nil # we wait until druid is finished
          http.request(req)
        end
        puts response.code
      end
      true
    end

    def exist?(dataSource, interval)
      check_key = "#{dataSource}-#{interval}"
      !current_jobs.any? do |id, meta|
        meta[:intervals].any? do |existing_interval|
          check_key == "#{meta[:dataSource]}-#{existing_interval}"
        end
      end
    end

  end
end
