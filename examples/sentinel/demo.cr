require "uri"
require "../../src/sentinel_client"

sentinel_urls = ENV
  .fetch("REDIS_SENTINEL_URLS", "redis://172.28.0.20:26379,redis://172.28.0.21:26380,redis://172.28.0.22:26381")
  .split(',')
  .map { |u| URI.parse(u.strip) }

master_name = ENV.fetch("REDIS_SENTINEL_MASTER", "mymaster")

redis = Redis::SentinelClient.new(
  sentinels: sentinel_urls,
  master_name: master_name,
)

puts "Connected to sentinel cluster."
puts "Initial master : #{redis.master_uri.host}:#{redis.master_uri.port}"
puts "Sentinels known: #{redis.sentinel_count}"
puts ""
puts "Run './sentinel.sh failover' in a second terminal to trigger a forced failover."
puts "Press Ctrl+C to stop."
puts ""

last_master = "#{redis.master_uri.host}:#{redis.master_uri.port}"
counter = 0_i64

Signal::INT.trap do
  puts "\nStopped."
  redis.close
  exit
end

loop do
  counter += 1

  begin
    redis.set "sentinel:demo:counter", counter.to_s
    val = redis.get "sentinel:demo:counter"
    current = "#{redis.master_uri.host}:#{redis.master_uri.port}"

    if current != last_master
      puts ""
      puts "  *** MASTER CHANGED  #{last_master} → #{current} ***"
      puts ""
      last_master = current
    end

    puts "[#{Time.utc.to_s("%H:%M:%S")}] OK   counter=#{val.to_s.ljust(6)}  master=#{current}"
  rescue ex : IO::Error
    puts "[#{Time.utc.to_s("%H:%M:%S")}] ERR  #{ex.class}: #{ex.message}"
  rescue ex
    puts "[#{Time.utc.to_s("%H:%M:%S")}] ERR  #{ex.class}: #{ex.message}"
  end

  sleep 1.second
end
