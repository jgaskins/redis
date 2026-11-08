require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

describe Redis::Commands::Set do
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

      redis.sadd key, %w[a b c]
      redis.srem(key, %w[a b nope]).should eq 2
      redis.srem(key, "c", "nope").should eq 1
    end

    test "can check whether a set has a value" do
      redis.sismember(key, "a").should eq 0
      redis.sadd key, "a"
      redis.sismember(key, "a").should eq 1
    end

    test "can find the difference in multiple sets" do
      first = key
      second = random_key

      redis.sadd first, %w[a b c]
      redis.sadd second, %w[b c d]

      # Just the elements of first that are not in second
      redis.sdiff(first, second).should eq %w[a]
    ensure
      redis.del second if second
    end

    test "can store the difference in multiple sets" do
      first = key
      second = random_key
      destination = random_key

      begin
        redis.sadd first, %w[a b c]
        redis.sadd second, %w[c d e]
        redis.sdiffstore(destination, first, second).should eq 2
        redis.sdiffstore(destination, [first, second]).should eq 2
      ensure
        redis.del first, second, destination
      end
    end

    test "can find the intersection of multiple sets" do
      first = key
      second = random_key
      third = random_key

      redis.sadd first, %w[a b c]
      redis.sadd second, %w[b c d]
      redis.sadd third, %w[c d e]

      redis.sinter(first, second, third).should eq %w[c]
      redis.sinter([first, second, third]).should eq %w[c]
    ensure
      redis.del second, third if second && third
    end

    test "can store the intersection of multiple sets into a destination set" do
      first = key
      second = random_key
      third = random_key
      destination = random_key

      begin
        redis.sadd first, %w[a b c]
        redis.sadd second, %w[b c d]
        redis.sadd third, %w[c d e]

        redis.sinterstore(destination, first, second, third).should eq 1
        redis.sinterstore(destination, [first, second, third]).should eq 1
      ensure
        redis.del second, third, destination
      end
    end

    test "can get the union of multiple sets" do
      first = key
      second = random_key
      third = random_key

      begin
        redis.sadd first, %w[a b c]
        redis.sadd second, %w[b c d]
        redis.sadd third, %w[c d e]

        union = redis.sunion(first, second, third)
        union.should contain "a"
        union.should contain "b"
        union.should contain "c"
        union.should contain "d"
        union.should contain "e"

        union = redis.sunion([first, second, third])
        union.should contain "a"
        union.should contain "b"
        union.should contain "c"
        union.should contain "d"
        union.should contain "e"
      ensure
        redis.del first, second, third
      end
    end

    test "can store the union of multiple sets" do
      first = key
      second = random_key
      third = random_key
      destination = random_key

      begin
        redis.sadd first, %w[a b c]
        redis.sadd second, %w[b c d]
        redis.sadd third, %w[c d e]

        redis.sunionstore(destination, first, second, third).should eq 5
        redis.sunionstore(destination, [first, second, third]).should eq 5
      ensure
        redis.del first, second, third, destination
      end
    end

    test "can determine the number of members of a set" do
      redis.sadd key, "a"
      redis.scard(key).should eq 1
      redis.sadd key, "b", "c"
      redis.scard(key).should eq 3
    end

    test "can move a member from one set to another" do
      source = key
      destination = random_key

      begin
        redis.sadd source, %w[a b c]
        redis.smove(source, destination, "a").should eq 1
        redis.smove(source, destination, "doesn't exist").should eq 0
      ensure
        redis.del source, destination
      end
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
end
