require "uuid"
require "./spec_helper"

require "../src/redis"

# Do not use DB slot 15. That's used as the secondary DB for testing the ability
# to use DBs other than 0.
redis_uri = URI.parse("redis:///")
redis = Redis::Client.new(uri: redis_uri)

private def random_key
  UUID.random.to_s
end

private macro test(msg, &block)
  it {{msg}} do
    key = random_key

    begin
      {{yield}}
    ensure
      redis.del key
    end
  end
end

describe Redis::Client do
  it "can set, get, and delete keys" do
    known_key = random_key

    begin
      redis.get(random_key).should eq nil
      redis.set(known_key, "hello")
      redis.get(known_key).should eq "hello"
      redis.del(known_key).should eq 1
      redis.del(known_key).should eq 0
      redis.get(known_key).should eq nil
    ensure
      redis.del known_key
    end
  end

  it "can get keys" do
    key = random_key

    begin
      redis.set key, "yep"
      redis.keys.includes?(key).should eq true
    ensure
      redis.del key
    end
  end

  it "can increment and decrement" do
    key = random_key

    begin
      redis.incr(key).should eq 1
      redis.incr(key).should eq 2
      redis.get(key).should eq "2"
      redis.decr(key).should eq 1
      redis.decr(key).should eq 0
      redis.get(key).should eq "0"

      redis.incrby(key, 2).should eq 2
      redis.incrby(key, 3).should eq 5
      redis.decrby(key, 2).should eq 3
      redis.incrby(key, 1234567812345678).should eq 1234567812345678 + 3

    ensure
      redis.del key
    end
  end

  describe "sets" do
    it "can add and remove members of a set" do
      key = random_key

      begin
        redis.sadd(key, "a").should eq 1
        redis.sadd(key, "a").should eq 0
        redis.smembers(key).should eq %w[a]

        redis.sadd key, "b"
        redis.smembers(key).includes?("a").should eq true
        redis.smembers(key).includes?("b").should eq true

        redis.srem key, "a"
        redis.smembers(key).includes?("a").should eq false
        redis.smembers(key).includes?("b").should eq true
      ensure
        redis.del key
      end
    end

    test "can check whether a set has a value" do
      redis.sismember(key, "a").should eq 0
      redis.sadd key, "a"
      redis.sismember(key, "a").should eq 1
    end

    test "can find the difference in 2 sets" do
      first = key
      second = random_key

      redis.sadd first, "a", "b", "c"
      redis.sadd second, "b", "c", "d"

      # Just the elements of first that are not in second
      redis.sdiff(first, second).should eq %w[a]
    ensure
      redis.del second if second
    end

    test "can find the intersection of multiple sets" do
      first = key
      second = random_key
      third = random_key

      redis.sadd first, "a", "b", "c"
      redis.sadd second, "b", "c", "d"
      redis.sadd third, "c", "d", "e"

      redis.sinter(first, second, third).should eq %w[c]
    ensure
      redis.del second, third if second && third
    end

    test "can determine the number of members of a set" do
      redis.sadd key, "a"
      redis.scard(key).should eq 1
      redis.sadd key, "b", "c"
      redis.scard(key).should eq 3
    end
  end

  it "can pipeline commands" do
    key = random_key

    begin
      first_incr  = Redis::Future.new
      second_incr = Redis::Future.new
      first_decr  = Redis::Future.new
      second_decr = Redis::Future.new

      redis.pipeline do |redis|
        first_incr = redis.incr key
        second_incr = redis.incr key

        first_decr = redis.decr key
        second_decr = redis.decr key
      end.should eq [1, 2, 1, 0]

      first_incr.value.should eq 1
      second_incr.value.should eq 2
      first_decr.value.should eq 1
      second_decr.value.should eq 0
    ensure
      redis.del key
    end
  end

  test "checking for existence of keys" do
    redis.exists(key).should eq 0

    redis.incr key
    redis.exists(key).should eq 1

    redis.del key
    redis.exists(key).should eq 0
  end

  it "handles exceptions while pipelining" do
    key = random_key

    begin
      redis.pipeline do |redis|
        redis.incr key
        redis.incr key
        raise "lol"
      end
    rescue
      redis.get(key).should eq "2"
    ensure
      redis.del key
    end
  end

  it "can use different Redis DBs" do
    secondary_uri = redis_uri.dup
    secondary_uri.path = "/15"
    secondary_db = Redis::Client.new(uri: secondary_uri)
    key = random_key

    begin
      redis.set key, "42"
      redis.get(key).should eq "42"
      secondary_db.get(key).should eq nil
    ensure
      redis.del key
      secondary_db.close
    end
  end

  describe "streams" do
    it "can use streams" do
      key = random_key

      begin
        # entry_id = redis.xadd key, "*", {"foo" => "bar"}
        entry_id = redis.xadd key, "*", foo: "bar"
        range = redis.xrange(key, "-", "+")
        range.size.should eq 1
        range.each do |result|
          id, data = result.as(Array)
          id.as(String).should eq entry_id
          data.should eq %w[foo bar]
        end
      ensure
        redis.del key
      end
    end

    it "can cap streams" do
      key = random_key

      begin
        11.times { redis.xadd key, "*", maxlen: "10", foo: "bar" }
        redis.xlen(key).should eq 10
      ensure
        redis.del key
      end
    end

    it "can consume streams" do
      key = "my-stream"
      group = "my-group"

      begin
        entry_id = redis.xadd key, "*", foo: "bar"
        # Create a group to consume this stream starting at the beginning
        redis.xgroup "create", key, group, "0"
        consumer_id = UUID.random.to_s

        result = redis.xreadgroup group, consumer_id, count: "10", streams: {"my-stream": ">"}

      rescue ex
        pp ex
        raise ex
      ensure
        redis.xgroup "destroy", key, group
        redis.del key
      end
    end
  end

  it "can use transactions" do
    key = random_key

    begin
      redis.multi do |redis|
        redis.set key, "yep"
        redis.discard

        redis.get "fuck"
      end.should be_empty

      redis.get(key).should eq nil

      _, nope, _, yep = redis.multi do |redis|
        redis.set key, "nope"
        redis.get key
        redis.set key, "yep"
        redis.get key
      end

      nope.should eq "nope"
      yep.should eq "yep"

      redis.get(key).should eq "yep"
      redis.del key

      begin
        redis.multi do |redis|
          redis.set key, "lol"

          raise "oops"
        ensure
          redis.get(key).should eq nil
        end
      rescue
      end

      # Ensure we're still in the same state
      redis.get(key).should eq nil
      # Ensure we can still set the key
      redis.set key, "yep"
      redis.get(key).should eq "yep"
    ensure
      redis.del key
    end
  end

  it "works with lists" do
    key = random_key

    begin
      spawn do
        sleep 10.milliseconds
        redis.lpush key, "omg", "lol", "wtf", "bbq"
      end
      redis.brpop(key, timeout: 1).should eq [key, "omg"]
      redis.brpop(key, timeout: "1").should eq [key, "lol"]
      redis.brpop(key, timeout: 1.second).should eq [key, "wtf"]
      redis.brpop(key, timeout: 1.0).should eq [key, "bbq"]
    ensure
      redis.del key
    end

    left = random_key
    right = random_key

    begin
      redis.lpush left, "foo"
      redis.rpoplpush left, right
      redis.rpop(right).should eq "foo"
    ensure
      redis.del left, right
    end
  end

  it "can publish and subscribe" do
    ready = false
    spawn do
      until ready
        Fiber.yield
      end
      # Publishes happen on other connections
      spawn redis.publish "foo", "unsub"
      spawn redis.publish "bar", "unsub"
    end

    redis.subscribe "foo", "bar" do |subscription, conn|
      subscription.on_message do |channel, message|
        if message == "unsub"
          conn.unsubscribe channel
        end
      end

      subscription.on_subscribe do |channel, count|
        # Only set ready if *both* subscriptions have gone through
        ready = true if count == 2
      end
    end
  end
end
