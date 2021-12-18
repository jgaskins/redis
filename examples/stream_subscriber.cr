require "../src/redis"
require "../src/streaming"
require "./streamer"

redis = Redis::Client.new
streamer = Streamer::Client.new(redis, group: "stuff", consumer: "hostname")

pp streamer.stream_names

streamer.subscribe "chat", timeout: 30.seconds do |msg|
  person = Person.from_json(msg.body)
  pp id: msg.id, person: person, timestamp: msg.timestamp, age: msg.age
end

# Hit Enter to exit
gets

require "json"
require "uuid/json"
struct Person
  include JSON::Serializable

  getter id : UUID
  getter name : String
  getter registered_at : Time
end
