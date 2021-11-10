require "json"
require "uuid/json"

require "../src/redis"
require "./streamer"

redis = Redis::Client.new
streamer = Streamer::Client.new(redis)

streamer.publish "chat", {
  id:            UUID.random,
  name:          "Jamie",
  registered_at: rand(1_000_000).minutes.ago,
}.to_json

puts "Published"
gets
