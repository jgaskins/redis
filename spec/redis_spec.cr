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
  test "can set, get, and delete keys" do
    redis.get(random_key).should eq nil
    redis.set(key, "hello")
    redis.get(key).should eq "hello"
    redis.del(key).should eq 1
    redis.del(key).should eq 0
    redis.get(key).should eq nil
  end

  test "can set expiration timestamps on keys" do
    redis.set key, "foo", ex: 10.milliseconds.from_now
    redis.get(key).should eq "foo"
    sleep 10.milliseconds
    redis.get(key).should eq nil
  end

  test "can expire key after a specified number of seconds" do
    redis.set key, "foo"
    redis.expire key, 1
    sleep 1.5.seconds
    redis.get(key).should eq nil
  end

  test "can expire key at a given timestamp" do
    redis.set key, "foo"
    redis.expireat key, 1.second.from_now
    sleep 1.second
    redis.get(key).should eq nil
  end

  test "can expire key after a specified number of milliseconds" do
    redis.set key, "foo"
    redis.pexpire key, 10
    sleep 100.milliseconds
    redis.get(key).should eq nil
  end

  test "can expire key at a given milliseconds-timestamp" do
    redis.set key, "foo"
    redis.pexpireat key, 10.milliseconds.from_now
    sleep 100.milliseconds
    redis.get(key).should eq nil
  end

  test "can returns the remaining time to live of a key that has a timeout in seconds" do
    redis.set key, "foo", ex: 1
    redis.ttl(key).should eq 1
  end

  test "can returns the remaining time to live of a key that has a timeout in milliseconds" do
    redis.set key, "foo", px: 10
    result = redis.pttl(key)
    result.should be <= 10
    result.should be >= 1
  end

  test "can set a key only if it does not exist" do
    redis.set(key, "foo", nx: true).should eq "OK"
    redis.set(key, "foo", nx: true).should eq nil
  end

  test "can set a key only if it does exist" do
    redis.set(key, "foo", xx: true).should eq nil
    redis.set key, "foo"
    redis.set(key, "foo", xx: true).should eq "OK"
  end

  test "can get the list of keys" do
    redis.set key, "yep"
    redis.keys.includes?(key).should eq true
  end

  test "can increment and decrement" do
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
  end

  describe "lists" do
    test "can push and get a range" do
      redis.rpush key, "one"
      redis.rpush key, "two"
      redis.rpush key, "three"
      redis.lrange(key, 0, 0).should eq %w[one]
      redis.lrange(key, "-3", "2").should eq %w[one two three]
    end
  end

  describe "sets" do
    test "can add and remove members of a set" do
      redis.sadd(key, "a").should eq 1
      redis.sadd(key, "a").should eq 0
      redis.smembers(key).should eq %w[a]

      redis.sadd key, "b"
      redis.smembers(key).includes?("a").should eq true
      redis.smembers(key).includes?("b").should eq true

      redis.srem key, "a"
      redis.smembers(key).includes?("a").should eq false
      redis.smembers(key).includes?("b").should eq true
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

  describe "sorted sets" do
    test "can add and remove members of a sorted set" do
      redis.zadd(key, "1", "a").should eq 1
      redis.zadd(key, "1", "a").should eq 0
      redis.zrange(key, "0", "-1").should eq %w[a]

      redis.zadd key, "2", "b"
      redis.zrange(key, "0", "-1").as(Array).should contain "a"
      redis.zrange(key, "0", "-1").as(Array).should contain "b"

      redis.zrem key, "a"
      redis.zrange(key, "0", "-1").as(Array).should_not contain "a"
      redis.zrange(key, "0", "-1").as(Array).should contain "b"
    end

    test "counts the number of elements set at the key" do
      redis.zadd(key, "1", "one")
      redis.zadd(key, "2", "two")
      redis.zadd(key, "3", "three")
      redis.zcount(key, "0", "+inf").should eq(3)
      redis.zcount(key, "(1", "3").should eq(2)
    end
  end

  test "can pipeline commands" do
    first_incr = Redis::Future.new
    second_incr = Redis::Future.new
    first_decr = Redis::Future.new
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
  end

  test "checking for existence of keys" do
    redis.exists(key).should eq 0

    redis.incr key
    redis.exists(key).should eq 1

    redis.del key
    redis.exists(key).should eq 0
  end

  test "handles exceptions while pipelining" do
    begin
      redis.pipeline do |redis|
        redis.incr key
        redis.incr key
        raise "lol"
      end
    rescue
      redis.get(key).should eq "2"
    end
  end

  test "can use different Redis DBs" do
    secondary_uri = redis_uri.dup
    secondary_uri.path = "/15"
    secondary_db = Redis::Client.new(uri: secondary_uri)

    begin
      redis.set key, "42"
      redis.get(key).should eq "42"
      secondary_db.get(key).should eq nil
    ensure
      secondary_db.close
    end
  end

  describe "streams" do
    test "can use streams" do
      # entry_id = redis.xadd key, "*", {"foo" => "bar"}
      entry_id = redis.xadd key, "*", foo: "bar"
      range = redis.xrange(key, "-", "+")
      range.size.should eq 1
      range.each do |result|
        id, data = result.as(Array)
        id.as(String).should eq entry_id
        data.should eq %w[foo bar]
      end
    end

    test "can cap streams" do
      redis.pipeline do |pipe|
        11.times { pipe.xadd key, "*", maxlen: "10", foo: "bar" }
      end

      redis.xlen(key).should eq 10
    end

    test "can approximately cap streams" do
      redis.pipeline do |pipe|
        2_000.times { pipe.xadd key, "*", maxlen: {"~", "10"}, foo: "bar" }
      end

      redis.xlen(key).should be <= 100
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

  test "can use transactions" do
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
  end

  test "works with lists" do
    spawn do
      sleep 10.milliseconds
      redis.lpush key, "omg", "lol", "wtf", "bbq"
    end
    redis.brpop(key, timeout: 1).should eq [key, "omg"]
    redis.brpop(key, timeout: "1").should eq [key, "lol"]
    redis.brpop(key, timeout: 1.second).should eq [key, "wtf"]
    redis.brpop(key, timeout: 1.0).should eq [key, "bbq"]


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
