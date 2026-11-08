require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

describe Redis::Commands::Hash do
  test "hset returns the number of new fields set on the given key" do
    redis.hset(key, one: "", two: "").should eq 2

    # Only "three" is added, the others already existed
    redis.hset(key, {"one" => "", "two" => "", "three" => ""}).should eq 1

    # "four" and "five" are both new
    redis.hset(key, %w[one yes two yes three yes four yes five yes]).should eq 2
  end

  test "hget returns the given field for the given key" do
    redis.hset key, one: "1", two: "2"
    redis.hget(key, "one").should eq "1"
  end

  test "hgetall returns all fields for the given key" do
    redis.hset key, one: "1", two: "2"
    redis.hgetall(key).should eq %w[one 1 two 2]
  end

  test "hexists returns whether a field exists in a hash key" do
    redis.hset key, yep: "1"

    redis.hexists(key, "yep").should eq 1
    redis.hexists(key, "nope").should eq 0
  end

  if test.server_version >= Version["8.0.0"]
    test "hgetdel returns and deletes the given field from the given key" do
      redis.hset key, one: "1", two: "2", three: "3", four: "4"

      redis.hgetdel(key, "one", "two").should eq %w[1 2]
      redis.hgetdel(key, %w[three four]).should eq %w[3 4]

      redis.hlen(key).should eq 0
    end

    describe "#hsetex" do
      test "sets fields with a hash" do
        redis.hsetex(key, {"one" => "1", "two" => "2"}).should eq 1
        redis.hmget(key, "one", "two").should eq %w[1 2]
      end

      test "sets fields with a NamedTuple" do
        redis.hsetex(key, {one: "1", two: "2"}).should eq 1
        redis.hmget(key, "one", "two").should eq %w[1 2]
      end

      test "can set fields only if they don't exist" do
        redis.hsetex key, {one: "1"}
        redis.hsetex(key, {one: "1", two: "2"}, fnx: true).should eq 0
      end

      test "can set fields only if they do exist" do
        redis.hsetex key, {one: "1"}
        redis.hsetex(key, {one: "1", two: "2"}, fxx: true).should eq 0
      end

      test "can set fields with an expiration in seconds" do
        redis.hsetex key, {one: "1"}, ex: 1.minute

        redis.httl(key, "one").should eq [60]
      end

      test "can set fields with an expiration in milliseconds" do
        redis.hsetex key, {one: "1"}, px: 1.minute

        redis.hpttl(key, "one").first.as(Int64).should be_within 10, of: 60_000
      end

      test "can set fields with an expiration timestamp with 1-second precision" do
        redis.hsetex key, {one: "1"}, exat: 1.minute.from_now

        redis.httl(key, "one").first.as(Int64).should eq 60
      end

      test "can set fields with an expiration timestamp with 1ms precision" do
        redis.hsetex key, {one: "1"}, pxat: 1.minute.from_now

        redis.hpttl(key, "one").first.as(Int64).should be_within 10, of: 60_000
      end

      test "can set fields and keep the existing ttl" do
        redis.hsetex key, {one: "1"}, ex: 1.minute
        redis.hsetex(key, {one: "2"}, keepttl: true).should eq 1
        redis.httl(key, "one").should eq [60]
      end

      test "raises an ArgumentError with multiple expiration arguments" do
        {% for options in [
                            "ex: 1.minute, px: 1.minute",
                            "ex: 1.minute, exat: 1.minute.from_now",
                            "ex: 1.minute, pxat: 1.minute.from_now",
                            "ex: 1.minute, keepttl: true",
                            "px: 1.minute, exat: 1.minute.from_now",
                            "px: 1.minute, pxat: 1.minute.from_now",
                            "px: 1.minute, keepttl: true",
                            "exat: 1.minute.from_now, pxat: 1.minute.from_now",
                            "exat: 1.minute.from_now, keepttl: true",
                            "pxat: 1.minute.from_now, keepttl: true",
                          ] %}
          expect_raises ArgumentError do
            redis.hsetex key, {one: "1"}, {{options.id}}
          end
        {% end %}
      end
    end

    describe "#hexpire" do
      test "removes a key after the expiration time" do
        redis.hset key, one: "1", two: "2", three: "3"

        redis.hexpire(key, 1.second, fields: %w[one]).should eq [1]
        redis.hexpire(key, 10.seconds, fields: %w[two three]).should eq [1, 1]

        redis.httl(key, "one").should eq [1]
      end

      test "sets expiration only when there is no expiration" do
        redis.hset key, one: "1"
        redis.hexpire(key, 1.hour, nx: true, fields: %w[one]).should eq [1]
        redis.httl(key, "one").should eq [3600]
        redis.hexpire(key, 1.hour, nx: true, fields: %w[one]).should eq [0]
      end

      test "sets expiration only when the field is already set to expire" do
        redis.hset key, one: "1"
        redis.hexpire(key, 1.hour, xx: true, fields: %w[one]).should eq [0]
        redis.hexpire key, 1.hour, fields: %w[one]
        redis.hexpire(key, 1.hour, xx: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being extended" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hexpire(key, 1.second, gt: true, fields: %w[one]).should eq [0]
        redis.hexpire(key, 1.minute, gt: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being reduced" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hexpire(key, 1.minute, lt: true, fields: %w[one]).should eq [0]
        redis.hexpire(key, 1.second, lt: true, fields: %w[one]).should eq [1]
      end
    end

    describe "#hpexpire" do
      test "removes a key after the expiration time" do
        redis.hset key, one: "1", two: "2", three: "3"

        redis.hpexpire(key, 1.second, fields: %w[one]).should eq [1]
        redis.hpexpire(key, 10.seconds, fields: %w[two three]).should eq [1, 1]

        redis.httl(key, "one").should eq [1]
      end

      test "sets expiration only when there is no expiration" do
        redis.hset key, one: "1"
        redis.hpexpire(key, 1.hour, nx: true, fields: %w[one]).should eq [1]
        redis.httl(key, "one").should eq [3600]
        redis.hpexpire(key, 1.hour, nx: true, fields: %w[one]).should eq [0]
      end

      test "sets expiration only when the field is already set to expire" do
        redis.hset key, one: "1"
        redis.hpexpire(key, 1.hour, xx: true, fields: %w[one]).should eq [0]
        redis.hpexpire key, 1.hour, fields: %w[one]
        redis.hpexpire(key, 1.hour, xx: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being extended" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hpexpire(key, 1.second, gt: true, fields: %w[one]).should eq [0]
        redis.hpexpire(key, 1.minute, gt: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being reduced" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hpexpire(key, 1.minute, lt: true, fields: %w[one]).should eq [0]
        redis.hpexpire(key, 1.second, lt: true, fields: %w[one]).should eq [1]
      end
    end

    describe "#hexpireat" do
      test "removes a key after the expiration time" do
        redis.hset key, one: "1", two: "2", three: "3"

        redis.hexpireat(key, 1.second.from_now, fields: %w[one]).should eq [1]
        redis.hexpireat(key, 10.seconds.from_now, fields: %w[two three]).should eq [1, 1]

        redis.httl(key, "one").should eq [1]
      end

      test "sets expiration only when there is no expiration" do
        redis.hset key, one: "1"
        redis.hpexpireat(key, 1.hour.from_now, nx: true, fields: %w[one]).should eq [1]
        redis.httl(key, "one").should eq [3600]
        redis.hpexpireat(key, 1.hour.from_now, nx: true, fields: %w[one]).should eq [0]
      end

      test "sets expiration only when the field is already set to expireat" do
        redis.hset key, one: "1"
        redis.hexpireat(key, 1.hour.from_now, xx: true, fields: %w[one]).should eq [0]
        redis.hexpireat key, 1.hour.from_now, fields: %w[one]
        redis.hexpireat(key, 1.hour.from_now, xx: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being extended" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hexpireat(key, 1.second.from_now, gt: true, fields: %w[one]).should eq [0]
        redis.hexpireat(key, 1.minute.from_now, gt: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being reduced" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hexpireat(key, 1.minute.from_now, lt: true, fields: %w[one]).should eq [0]
        redis.hexpireat(key, 1.second.from_now, lt: true, fields: %w[one]).should eq [1]
      end
    end

    describe "#hpexpireat" do
      test "removes a key after the expiration time" do
        redis.hset key, one: "1", two: "2", three: "3"

        redis.hpexpireat(key, 1.second.from_now, fields: %w[one]).should eq [1]
        redis.hpexpireat(key, 10.seconds.from_now, fields: %w[two three]).should eq [1, 1]

        redis.httl(key, "one").should eq [1]
      end

      test "sets expiration only when there is no expiration" do
        redis.hset key, one: "1"
        redis.hpexpireat(key, 1.hour.from_now, nx: true, fields: %w[one]).should eq [1]
        redis.hpttl(key, "one").first.as(Int).should be_within 1_000, of: 3600_000
        redis.hpexpireat(key, 1.hour.from_now, nx: true, fields: %w[one]).should eq [0]
      end

      test "sets expiration only when the field is already set to expireat" do
        redis.hset key, one: "1"
        redis.hpexpireat(key, 1.hour.from_now, xx: true, fields: %w[one]).should eq [0]
        redis.hpexpireat key, 1.hour.from_now, fields: %w[one]
        redis.hpexpireat(key, 1.hour.from_now, xx: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being extended" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hpexpireat(key, 1.second.from_now, gt: true, fields: %w[one]).should eq [0]
        redis.hpexpireat(key, 1.minute.from_now, gt: true, fields: %w[one]).should eq [1]
      end

      test "sets expiration only when the field's ttl is being reduced" do
        redis.hsetex key, ex: 30.seconds, fields: {one: "1"}

        redis.hpexpireat(key, 1.minute.from_now, lt: true, fields: %w[one]).should eq [0]
        redis.hpexpireat(key, 1.second.from_now, lt: true, fields: %w[one]).should eq [1]
      end
    end

    describe "#hpersist" do
      test "removes the ttl from a field" do
        redis.hsetex key, ex: 1.day, fields: {one: "1", two: "2", three: "3"}

        redis.hpersist(key, "one").should eq [1]
        redis.hpersist(key, %w[two three four]).should eq [1, 1, -2]
        redis.httl(key, "one", "two", "three").should eq [-1, -1, -1]
      end
    end
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

  test "hstrlen returns the size of the value in the given field" do
    redis.hset key, short: "." * 3, medium: "." * 100, long: "." * 8192

    redis.hstrlen(key, "short").should eq 3
    redis.hstrlen(key, "medium").should eq 100
    redis.hstrlen(key, "long").should eq 8192
  end
end
