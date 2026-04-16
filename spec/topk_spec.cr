require "./spec_helper"

require "../src/redis"
require "../src/topk"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

if test.server_version >= Version["8.0.0"]
  describe Redis::TopK do
    test "reserves a topk" do
      redis.topk.reserve(key, "10").should eq "OK"

      info = Redis.to_hash(redis.topk.info(key))
      # {"k" => 10, "width" => 8, "depth" => 7, "decay" => "0.9"}
      info["k"].should eq 10
    end

    test "reserves a topk with a width, depth, and decay" do
      redis.topk.reserve key, "10",
        width: "42",
        depth: "69",
        decay: "0.420"

      info = Redis.to_hash(redis.topk.info(key))
      info["k"].should eq 10
      info["width"].should eq 42
      info["depth"].should eq 69
      # RESP2 doesn't support floats, so we get it back as a string
      info["decay"].should eq "0.42"
    end

    test "adds an item to a topk" do
      redis.topk.reserve key, 2

      redis.topk.add key, %w[one two three four one]

      redis.topk.list(key).should eq %w[one four]
      redis.topk.list(key, withcount: true).should eq ["one", 2, "four", 1]
    end

    test "returns whether the items are in the topk" do
      redis.topk.reserve key, 2

      redis.topk.add key, %w[one two three two one]

      # one and two are in the topk, but three has been evicted
      redis.topk.query(key, %w[one two three]).should eq [1, 1, 0]
      redis.topk.query(key, "one", "two", "three").should eq [1, 1, 0]
    end

    test "increments the item in the topk by the given amount" do
      redis.topk.reserve key, 2

      redis.topk.incrby key, "one", 2

      redis.topk.list(key, withcount: true).should eq ["one", 2]
    end
  end
end
