require "../src/redis"
require "../src/streaming"
require "./streamer"

redis = Redis::Client.new
streamer = Streamer::Client.new(redis, consumer: "hostname")

streamer.subscribe "chat", group: "stuff", timeout: 30.minutes do |msg|
  pp id: msg.id, person: Person.from_json(msg.body), timestamp: msg.timestamp, age: msg.age
end

gets

require "json"
require "uuid/json"
struct Person
  include JSON::Serializable

  getter id : UUID
  getter name : String
  getter registered_at : Time
end
