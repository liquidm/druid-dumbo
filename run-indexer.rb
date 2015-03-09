#!/usr/bin/env ruby

require 'net/http'
require 'uri'
require 'json'

if ARGV.length < 2
  puts "#{__FILE__} spec.file overlord1 [...overlordn]"
  exit 1
end

puts "Reading #{ARGV[0]}"
content = File.read(ARGV[0])

overlords = ARGV[1..-1]

10.times do |n|
  overlord = overlords[n % overlords.length]
  puts "Trying overlord #{overlord}"

  uri = URI.parse("http://#{overlord}:8090/druid/indexer/v1/task")

  http = Net::HTTP.new(uri.host, uri.port)

  request = Net::HTTP::Post.new(uri.request_uri)
  request.body = content

  request["Content-Type"] = "application/json"

  response = http.request(request)

  if response.code != "200"
    puts "Got a #{response.code}, skipping"
    next
  end

  task = JSON.parse(response.body)["task"]

  task_uri = URI.parse(uri.to_s + "/" + task + "/status")
  puts "Spawned #{task_uri}"

  begin
    sleep 10
    task_status = JSON.parse(Net::HTTP.get_response(task_uri).body)["status"]
    puts task_status
  end while task_status["status"] == "RUNNING"

  puts "done"
  exit 0
end

puts "Giving up"
exit 1
