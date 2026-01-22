require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Client do
  it "can ping the server" do
    redis.ping.should eq "PONG"
    redis.ping("message").should eq "message"
  end

  test "can set, get, and delete keys" do
    redis.get(random_key).should eq nil
    redis.set(key, "hello").should eq "OK"
    redis.get(key).should eq "hello"
    redis.del(key).should eq 1
    redis.del(key).should eq 0
    redis.get(key).should eq nil
  end

  test "it returns 0 when passed no keys to delete" do
    redis.del([] of String).should eq 0
  end

  it "can get multiple keys" do
    a = random_key
    b = random_key

    begin
      redis.set a, "a"
      redis.set b, "b"

      redis.mget(a, b, random_key).should eq ["a", "b", nil]
      redis.mget([a, b, random_key]).should eq ["a", "b", nil]
      redis.mget([] of String).should eq [] of Redis::Value
    ensure
      redis.del a, b
    end
  end

  test "deletes a key and returns its value" do
    redis.getdel(key).should eq nil
    redis.set key, "value"
    redis.getdel(key).should eq "value"
    redis.get(key).should eq nil
  end

  test "sets a value and returns the previous value" do
    redis.set key, "value"
    redis.set(key, "new value", get: true).should eq "value"
    redis.get(key).should eq "new value"
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

    redis.del key
    redis.incrbyfloat(key, 0.1).to_f.should be_within 0.000001, of: 0.1
    redis.incrbyfloat(key, 0.2).to_f.should be_within 0.000001, of: 0.3
    redis.get(key).not_nil!.to_f.should be_within 0.000001, of: 0.3
  end

  describe "lists" do
    test "can push and get a range" do
      redis.rpush key, "one"
      redis.rpush key, "two"
      redis.rpush key, "three"
      redis.lrange(key, 0, 0).should eq %w[one]
      redis.lrange(key, "-3", "2").should eq %w[one two three]
    end

    test "can trim lists" do
      redis.rpush key, %w[0 1 2 3 4 5 6 7 8 9]

      # String indices
      redis.ltrim key, "0", "8"
      redis.lrange(key, 0, -1).should eq %w[0 1 2 3 4 5 6 7 8]

      # Int indices
      redis.ltrim key, 0, 7
      redis.lrange(key, 0, -1).should eq %w[0 1 2 3 4 5 6 7]

      # String range with inclusive end
      redis.ltrim key, "0".."6"
      redis.lrange(key, 0, -1).should eq %w[0 1 2 3 4 5 6]

      # String range with exclusive end
      redis.ltrim key, "0"..."5"
      redis.lrange(key, 0, -1).should eq %w[0 1 2 3 4 5]

      # Range with inclusive end
      redis.ltrim key, 0..4
      redis.lrange(key, 0, -1).should eq %w[0 1 2 3 4]

      # Range with exclusive end
      redis.ltrim key, 0...3
      redis.lrange(key, 0, -1).should eq %w[0 1 2]
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

    test "can scan a set" do
      values = Array.new(1_000, &.to_s).to_set

      redis.sadd key, values
      redis.sscan_each key do |key|
        values.delete key
      end

      values.should be_empty
    end

    test "can scan a set as an iterator" do
      values = Array.new(1000) { Random::Secure.hex }.to_set

      redis.sadd key, values
      redis.sscan_each(key, count: 10).each do |key|
        values.delete key
      end

      values.should be_empty
    end
  end

  describe "hash" do
    test "hset returns the number of new fields set on the given key" do
      redis.hset(key, one: "", two: "").should eq 2

      # Only "three" is added, the others already existed
      redis.hset(key, {"one" => "", "two" => "", "three" => ""}).should eq 1

      # "four" and "five" are both new
      redis.hset(key, %w[one yes two yes three yes four yes five yes]).should eq 2
    end

    test "hmget returns the given fields for the given key" do
      redis.hset key, one: "first", two: "second"

      redis.hget(key, "one").should eq "first"
      redis.hget(key, "nonexistent").should eq nil
      redis.hmget(key, "one", "nonexistent").should eq ["first", nil]
      redis.hmget(key, %w[one nonexistent]).should eq ["first", nil]
      redis.hmget(key, "nope", "lol").should eq [nil, nil]
      redis.hmget(key, %w[nope lol]).should eq [nil, nil]
    end

    test "hincrby increments the number stored at field in the hash" do
      redis.hset(key, {"field" => "5"})
      redis.hincrby(key, "field", 1).should eq 6
      redis.hincrby(key, "field", -1).should eq 5
      redis.hincrby(key, "field", -10).should eq -5
    end

    test "hdel deletes fields from hashes" do
      redis.hset key,
        name: "foo",
        splat_arg: "yes",
        array_arg: "also yes",
        array_arg2: "still yes"

      redis.hdel(key, "splat_arg", "nonexistent-field").should eq 1
      redis.hdel(key, %w[array_arg array_arg2 nonexistent-field]).should eq 2
    end

    test "hsetnx sets fields on a key only if they do not exist" do
      redis.hsetnx(key, "first", "lol").should eq 1
      redis.hsetnx(key, "first", "omg").should eq 0
      redis.hsetnx(key, "second", "lol").should eq 1
    end

    test "hscan yields each field/value pair" do
      values = Array
        .new(1_000) do |i|
          {i.to_s, rand.to_s}
        end
        .to_h

      redis.hset key, values
      redis.hscan_each key do |field, value|
        if values[field] == value
          values.delete field
        else
          raise "Yielded a field/value pair that does not exist: #{field.inspect} => #{value.inspect}"
        end
      end

      values.should be_empty
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

  test "does not raise on pipeline errors, returns them instead" do
    set_result, error_result, get_result = redis.pipeline do |redis|
      redis.set key, "lol"
      redis.set key, "", ex: -1.second
      redis.get key
    end

    set_result.should eq "OK"
    error_result.should be_a Redis::Error
    get_result.should eq "lol" # Still set to the original value we set it to
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
    secondary_uri = URI.parse(ENV.fetch("REDIS_URL", "redis:///"))
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

  context "transactions" do
    test "returns the results of the commands, like pipelines do" do
      # Returns
      redis.multi do |redis|
        redis.set key, "value"
        redis.get key
      end.should eq %w[OK value]
    end

    test "returns an empty array when the transaction is discarded" do
      redis.multi do |redis|
        redis.set key, "this gets discarded"
        redis.discard
        redis.get "this never actually does anything anyway"
      end.should be_empty
    end

    test "returns command errors, but does not raise" do
      redis.multi do |redis|
        redis.set key, "foo"
        redis.lpush key, "bar" # error, performing list operation on a string
        redis.get key
      end.should eq [
        "OK",
        Redis::Error.new("WRONGTYPE Operation against a key holding the wrong kind of value"),
        "foo",
      ]
    end

    test "allows the block to return/break" do
      value = redis.multi do |redis|
        redis.set key, "value"
        break 1
      end

      value.should eq 1
      redis.get(key).should eq "value"
    end

    test "does more transaction stuff" do
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

      expect_raises Exception do
        redis.multi do |redis|
          redis.set key, "lol"

          raise "oops"
        ensure
          redis.get(key).should eq nil
        end

        # Ensure we're still in the same state
        redis.get(key).should eq nil
        # Ensure we can still set the key
        redis.set key, "yep"
        redis.get(key).should eq "yep"
      end
    end
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
      redis.lmove left, right, :left, :right
      redis.rpop(right).should eq "foo"
    ensure
      redis.del left, right
    end
  end

  describe "scripts" do
    test "can load and evaluate scripts" do
      script = <<-LUA
      return {KEYS[1] or 1337, ARGV[1] or 42}
    LUA
      sha = redis.script_load script
      key_arg = UUID.random.to_s
      redis.set key_arg, "this is the key arg"

      begin
        # # EVALSHA
        redis.evalsha(sha, keys: [key_arg], args: %w[hi])
          .should eq [key_arg, "hi"]

        # Can we run it without providing args?
        redis.evalsha(sha, keys: [key_arg])
          .should eq [key_arg, 42]

        # Can we run it without providing keys?
        redis.evalsha(sha, args: %w[hi])
          .should eq [1337, "hi"]

        # Can we run it without providing anything?
        redis.evalsha(sha)
          .should eq [1337, 42]

        # # EVAL
        redis.eval(script, keys: [key_arg], args: %w[hi])
          .should eq [key_arg, "hi"]

        # Can we run it without providing args?
        redis.eval(script, keys: [key_arg])
          .should eq [key_arg, 42]

        # Can we run it without providing keys?
        redis.eval(script, args: %w[hi])
          .should eq [1337, "hi"]

        # Can we run it without providing anything?
        redis.eval(script)
          .should eq [1337, 42]
      ensure
        redis.unlink key_arg
      end
    end

    test "can manage scripts" do
      sha = redis.script_load "return 42"
      redis.script_exists(sha, "123").should eq [1, 0]

      # Check with an Enumerable
      redis.script_exists([sha, "123"]).should eq [1, 0]

      # Delete the scripts
      redis.script_flush :sync
      redis.script_exists(sha, "123").should eq [0, 0]
    end
  end

  # FIXME: These specs don't assert, which is confusing. The reason they still
  # validate that pubsub works is that they publish messages on a channel, which
  # unsubscribes. `redis.subscribe` and `redis.unsubscribe` both block the fiber
  # while those subscriptions are active, so if pubsub didn't work for any
  # reason these specs would simply stall which is not a good spec failure mode.
  context "pubsub" do
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

    it "can publish and subscribe to patterns" do
      ready = false
      spawn do
        until ready
          Fiber.yield
        end
        # Publishes happen on other connections
        spawn redis.publish "foo", "unsub"
        spawn redis.publish "bar", "unsub"
      end

      redis.psubscribe "f*", "b??" do |subscription, conn|
        subscription.on_message do |channel, message, pattern|
          if message == "unsub"
            conn.punsubscribe pattern
          end
        end

        subscription.on_subscribe do |channel, count|
          # Only set ready if *both* subscriptions have gone through
          ready = true if count == 2
        end
      end
    end
  end
end
