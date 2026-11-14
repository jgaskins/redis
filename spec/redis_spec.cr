require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

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

  describe "#get!" do
    test "gets a key, asserting that it exists" do
      redis.set key, {
        id:    123,
        items: [
          {
            id:               1,
            product_id:       1234,
            quantity:         1,
            unit_price_cents: 100_00,
          },
          {
            id:               2,
            product_id:       4321,
            quantity:         2,
            unit_price_cents: 200_00,
          },
        ],
      }.to_json

      cart = RedisSpec::Cart.from_json(redis.get!(key))
      cart.id.should eq 123
      cart.items.size.should eq 2
    end

    test "raises an error if the key does not exist" do
      expect_raises Redis::MissingKey do
        redis.get!(UUID.v7.to_s)
      end
    end
  end

  test "gets a Redis string value as bytes" do
    bytes = Random::Secure.random_bytes

    redis.set key, bytes

    redis.get_bytes(key).should eq bytes
    redis.get_bytes(UUID.v7.to_s).should eq nil

    redis.get_bytes!(key).should eq bytes
    expect_raises Redis::MissingKey do
      redis.get_bytes! UUID.v7.to_s
    end
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

  if test.server_version >= Version["6.2.0"]
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
  end

  test "can set expiration timestamps on keys" do
    redis.set key, "foo", ex: 1.hour.from_now
    redis.ttl(key).should be_within 1, of: 3600
  end

  test "can expire key after a specified number of seconds" do
    redis.set key, "foo"
    redis.expire key, 100
    redis.ttl(key).should be_within 1, of: 100
  end

  test "can expire key at a given timestamp" do
    redis.set key, "foo"
    redis.expireat key, 1.hour.from_now
    redis.ttl(key).should be_within 1, of: 3600
  end

  test "can expire key after a specified number of milliseconds" do
    redis.set key, "foo"
    redis.pexpire key, 1.hour
    redis.pttl(key).should be_within 1000, of: 3600_000
  end

  test "can expire key at a given milliseconds-timestamp" do
    redis.set key, "foo"
    redis.pexpireat key, 1.hour.from_now
    redis.pttl(key).should be_within 1000, of: 3600_000
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

  describe "mset" do
    it "sets multiple keys via a hash" do
      one = random_key
      two = random_key
      three = random_key

      begin
        redis.mset({
          one   => "1",
          two   => "2",
          three => "3",
        })

        redis.mget(one, two, three).should eq %w[1 2 3]
      ensure
        redis.del one, two, three
      end
    end

    it "sets multiple keys with an array" do
      one = random_key
      two = random_key
      three = random_key

      begin
        redis.mset([
          one, "1",
          two, "2",
          three, "3",
        ])

        redis.mget(one, two, three).should eq %w[1 2 3]
      ensure
        redis.del one, two, three
      end
    end

    it "sets multiple keys with variadic args" do
      one = random_key
      two = random_key
      three = random_key

      begin
        redis.mset(
          one, "1",
          two, "2",
          three, "3",
        )

        redis.mget(one, two, three).should eq %w[1 2 3]
      ensure
        redis.del one, two, three
      end
    end

    {% begin %}
      {% one = run "./random_key" %}
      {% two = run "./random_key" %}
      {% three = run "./random_key" %}

      it "sets multiple keys with a NamedTuple" do
        redis.mset({
          {{one}}:   "1",
          {{two}}:   "2",
          {{three}}: "3",
        })

        redis.mget({{one}}, {{two}}, {{three}}).should eq %w[1 2 3]
      ensure
        redis.del {{one}}, {{two}}, {{three}}
      end

      it "sets multiple keys with keyword args" do
        redis.mset(
          {{one}}:   "1",
          {{two}}:   "2",
          {{three}}: "3",
        )

        redis.mget({{one}}, {{two}}, {{three}}).should eq %w[1 2 3]
      ensure
        redis.del {{one}}, {{two}}, {{three}}
      end
    {% end %}
  end

  if test.server_version >= Version["8.4.0"]
    describe "#msetex" do
      {% begin %}
        {% one = run "./random_key" %}
        {% two = run "./random_key" %}
        {% three = run "./random_key" %}

        named_tuple = {
          {{one}}:   "1",
          {{two}}:   "2",
          {{three}}: "3",
        }

        [
          named_tuple,
          {
            {{one}} => "1",
            {{two}} => "2",
            {{three}} => "3",
          },
        ].each do |data|
          it "sets multiple keys with a #{data.class}" do
            redis.msetex(data)

            redis.mget({{one}}, {{two}}, {{three}}).should eq %w[1 2 3]
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} only if none of them exist" do
            redis.msetex(data, nx: true).should eq 1
            redis.del {{one}}
            redis.msetex(data, nx: true).should eq 0
            redis.del {{two}}
            redis.msetex(data, nx: true).should eq 0
            redis.del {{three}}
            redis.msetex(data, nx: true).should eq 1
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} only if all of them exist" do
            redis.msetex(data, xx: true).should eq 0
            redis.set {{one}}, ""
            redis.msetex(data, xx: true).should eq 0
            redis.set {{two}}, ""
            redis.msetex(data, xx: true).should eq 0
            redis.set {{three}}, ""
            redis.msetex(data, xx: true).should eq 1
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with a Time::Span TTL with EX" do
            redis.msetex(data, ex: 1.hour).should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with a Time::Span TTL with PX" do
            redis.msetex(data, px: 1.hour).should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with a String TTL with EX" do
            redis.msetex(data, ex: "3600").should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with a String TTL with PX" do
            redis.msetex(data, px: "3600000").should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with an Int TTL with EX" do
            redis.msetex(data, ex: 3600).should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with an Int TTL with PX" do
            redis.msetex(data, px: 3600_000).should eq 1
            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end

          it "can set keys via #{data.class} with KEEPTTL" do
            redis.msetex data, ex: 1.hour
            redis.msetex(data, keepttl: true).should eq 1

            redis.ttl({{one}}).should be_within 1, of: 3600
          ensure
            redis.del {{one}}, {{two}}, {{three}}
          end
        end
      {% end %}
    end
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

  if test.server_version >= Version["8.8.0"]
    describe "#increx" do
      test "can increment by an int" do
        redis.increx(key, byint: 3).should eq [3, 3]
        redis.increx(key, byint: 3).should eq [6, 3]

        redis.ttl(key).should eq -1
      end

      test "can increment by a float" do
        redis.set key, "6.0"

        new_value, increment = redis.increx(key, byfloat: "3.14")

        new_value.as(String).to_f.should be_within 0.0001, of: 9.14
        increment.as(String).to_f.should be_within 0.0001, of: 3.14
        redis.ttl(key).should eq -1
      end

      test "can increment with a lower bound" do
        redis.increx(key, byint: -50, lbound: -100).should eq [-50, -50]
        # Increment would exceed the lower bound, so it does not change
        redis.increx(key, byint: -1000, lbound: -100).should eq [-50, 0]
        redis.ttl(key).should eq -1
      end

      test "can increment with a lower bound, saturating" do
        redis.set key, "-50"
        redis.increx(key, byint: -1000, lbound: -100, saturate: true).should eq [-100, -50]
        redis.ttl(key).should eq -1
      end

      test "can increment with an upper bound" do
        redis.increx(key, byint: 50, ubound: 100).should eq [50, 50]
        # Increment would exceed the upper bound, so it does not change
        redis.increx(key, byint: 500, ubound: 100).should eq [50, 0]
        redis.ttl(key).should eq -1
      end

      test "can increment with an upper bound, saturating" do
        redis.set key, "50"
        redis.increx(key, byint: 1000, ubound: 100, saturate: true).should eq [100, 50]
        redis.ttl(key).should eq -1
      end

      test "can increment with both an upper and lower bound" do
        redis.increx(key, byint: 1000, lbound: -100, ubound: 100, saturate: true).should eq [100, 100]
        redis.increx(key, byint: -1000, lbound: -100, ubound: 100, saturate: true).should eq [-100, -200]
        redis.ttl(key).should eq -1
      end

      test "can set an expiration with a time span" do
        redis.increx(key, ex: 1.minute).should eq [1, 1]
        redis.ttl(key).should eq 60

        redis.del key
        redis.increx(key, px: 1.minute).should eq [1, 1]
        redis.pttl(key).should be_within 10, of: 60_000

        redis.del key
        redis.increx(key, exat: 1.minute.from_now).should eq [1, 1]
        redis.ttl(key).should be_in 59, 60

        redis.del key
        redis.increx(key, pxat: 1.minute.from_now).should eq [1, 1]
        redis.pttl(key).should be_within 10, of: 60_000
      end

      test "can remove an expiration" do
        redis.set key, "10", ex: 1.minute
        redis.ttl(key).should eq 60

        redis.increx key, persist: true

        redis.ttl(key).should eq -1
      end

      test "can set the expiration only if there is no expiration on the key" do
        redis.increx key, ex: 1.minute, enx: true
        # Does set the TTL
        redis.ttl(key).should be_in 59, 60

        redis.increx key, ex: 5.minutes, enx: true
        # Still the one we set above, didn't change to 5 minutes
        redis.ttl(key).should be_in 59, 60
      end
    end
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

  test "works with lists" do
    spawn do
      sleep 5.milliseconds
      redis.lpush key, "omg"
      sleep 5.milliseconds
      redis.lpush key, "lol"
      sleep 5.milliseconds
      redis.lpush key, "wtf"
      sleep 5.milliseconds
      redis.lpush key, "bbq"
    end
    redis.brpop(key, timeout: 1).should eq [key, "omg"]
    redis.brpop(key, timeout: "1").should eq [key, "lol"]
    redis.brpop(key, timeout: 1.second).should eq [key, "wtf"]
    redis.brpop(key, timeout: 1.0).should eq [key, "bbq"]

    left = random_key
    right = random_key

    if test.server_version >= Version["6.2.0"]
      begin
        redis.lpush left, "foo"
        redis.lmove left, right, :left, :right
        redis.rpop(right).should eq "foo"
      ensure
        redis.del left, right
      end
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
      if test.server_version >= Version["6.2.0"]
        redis.script_flush :sync
      else
        redis.script_flush
      end
      redis.script_exists(sha, "123").should eq [0, 0]
    end
  end

  if test.server_version >= Version["7.0.0"]
    describe "functions" do
      code = <<-FUNCTION
        #!lua name=mylib

        -- Called from our registered function
        local function transform(string)
          return string.upper(string)
        end

        local function myfunc(keys, args)
          return transform(args[1])
        end

        -- The only function we're making available outside of the script
        redis.register_function{
          function_name='myfunc',
          callback=myfunc,
          flags={'no-writes'}
        }
      FUNCTION

      it "loads and executes functions" do
        redis.function_load(code, replace: true).should eq "mylib"
      end

      it "calls a function" do
        redis.function_load code, replace: true

        redis.fcall("myfunc", %w[], %w[one two three])
          # The function returns the first arg
          .should eq "ONE"
      end

      it "calls a readonly function" do
        redis.function_load code, replace: true

        redis.fcall_ro("myfunc", %w[], %w[one two three])
          .should eq "ONE"
      end

      it "deletes a library and all its functions" do
        redis.function_load code, replace: true

        redis.function_delete "mylib"

        expect_raises Redis::Error do
          redis.fcall_ro("myfunc", %w[], %w[one two three])
        end
      end

      it "lists functions" do
        my_code = <<-LIB
          #!lua name=mylib

          redis.register_function('myfunc', function(keys, args)
            return args[0]
          end)
          LIB
        redis.function_load my_code, replace: true

        redis.function_list("mylib").should eq [
          [
            "library_name", "mylib",
            "engine", "LUA",
            "functions", [
              [
                "name", "myfunc",
                "description", nil,
                "flags", [] of Redis::Value,
              ],
            ],
          ],
        ]
        redis.function_list("mylib", withcode: true).should eq [
          [
            "library_name", "mylib",
            "engine", "LUA",
            "functions", [
              [
                "name", "myfunc",
                "description", nil,
                "flags", [] of Redis::Value,
              ],
            ],
            "library_code", my_code,
          ],
        ]
      end
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

    if test.server_version >= Version["7.0.0"]
      it "can publish and subscribe to sharded channels" do
        channel = random_key
        subscribed = Channel(Nil).new
        messages = Channel({String, String}).new
        unsubscribed = Channel({String, Int64}).new
        done = Channel(Exception?).new

        spawn do
          redis.ssubscribe channel do |subscription, connection|
            subscription.on_subscribe do
              subscribed.send nil
            end
            subscription.on_message do |received_channel, message|
              messages.send({received_channel, message})
              connection.sunsubscribe received_channel
            end
            subscription.on_unsubscribe do |unsubscribed_channel, count|
              unsubscribed.send({unsubscribed_channel, count})
            end
          end
          done.send nil
        rescue ex
          done.send ex
        end

        select
        when subscribed.receive
        when timeout(1.second)
          raise "Timed out while waiting for sharded subscription"
        end

        redis.spublish(channel, "hello").should eq 1

        select
        when message = messages.receive
          message.should eq({channel, "hello"})
        when timeout(1.second)
          raise "Timed out while waiting for sharded message"
        end

        select
        when event = unsubscribed.receive
          event.should eq({channel, 0_i64})
        when timeout(1.second)
          raise "Timed out while waiting for sharded unsubscription"
        end

        select
        when error = done.receive
          raise error if error
        when timeout(1.second)
          raise "Timed out while waiting for sharded subscription to finish"
        end
      end

      it "closes each subscription type with the matching command" do
        channel = random_key
        pattern = "#{random_key}*"
        shard_channel = random_key
        subscribed = Channel(String).new
        unsubscribed = Channel(String).new
        done = Channel(Exception?).new

        spawn do
          redis.subscribe channel do |subscription, connection|
            subscriptions = Set(String).new
            subscription.on_subscribe do |subscribed_channel|
              subscriptions << subscribed_channel
              subscribed.send subscribed_channel
              subscription.close if subscriptions.size == 3
            end
            subscription.on_unsubscribe do |unsubscribed_channel|
              unsubscribed.send unsubscribed_channel
            end
            connection.psubscribe pattern
            connection.ssubscribe shard_channel
          end
          done.send nil
        rescue ex
          done.send ex
        end

        subscriptions = Set(String).new
        3.times do
          select
          when subscribed_channel = subscribed.receive
            subscriptions << subscribed_channel
          when timeout(1.second)
            raise "Timed out while waiting for subscriptions"
          end
        end
        subscriptions.should eq Set{channel, pattern, shard_channel}

        unsubscriptions = Set(String).new
        3.times do
          select
          when unsubscribed_channel = unsubscribed.receive
            unsubscriptions << unsubscribed_channel
          when timeout(1.second)
            raise "Timed out while waiting for unsubscriptions"
          end
        end
        unsubscriptions.should eq Set{channel, pattern, shard_channel}

        select
        when error = done.receive
          raise error if error
        when timeout(1.second)
          raise "Timed out while waiting for subscriptions to finish"
        end
      end
    end
  end

  it "gets info about the Redis server" do
    redis.info.should be_a Hash(String, String)
    redis.info("modules").should be_a String
  end
end

module RedisSpec
  struct Cart
    include JSON::Serializable

    getter id : Int64
    getter items : Array(Item)

    struct Item
      include JSON::Serializable
    end
  end
end
