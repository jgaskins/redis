require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Commands::SortedSet do
  test "can add and remove members of a sorted set" do
    redis.zadd(key, "1", "a").should eq 1
    redis.zadd(key, "1", "a").should eq 0
    redis.zrange(key, "0", "-1").should eq %w[a]

    redis.zadd key, "2", "b"
    redis.zrange(key, "0", "-1").as(Array).should contain "a"
    redis.zrange(key, "0", "-1").as(Array).should contain "b"

    redis.zrem key, "a"
    redis.zrange(key, "0", "-1").as(Array).should_not contain "a"
    redis.zrange(key, 0, -1).as(Array).should contain "b"
  end

  test "can run ZRANGE BYLEX" do
    redis.zadd key,
      "0", "c",
      "0", "b",
      "0", "a"

    redis.zrange(key, "-", "+", by: :lex, limit: {0, 2}).should eq %w[a b]
  end

  test "can run ZRANGE BYSCORE" do
    redis.zadd key,
      "1", "one",
      "2", "two",
      "3", "three"

    redis.zrange(key, "+inf", "-inf", by: :score, rev: true, with_scores: true)
      .should eq %w[three 3 two 2 one 1]

    redis.zrangebyscore(key, "-inf", "+inf")
      .should eq %w[one two three]
    redis.zrangebyscore(key, "-inf", "+inf", with_scores: true)
      .should eq %w[one 1 two 2 three 3]
  end

  test "counts the number of elements set at the key" do
    redis.zadd(key, "1", "one")
    redis.zadd(key, "2", "two")
    redis.zadd(key, "3", "three")
    redis.zcount(key, "0", "+inf").should eq(3)
    redis.zcount(key, "(1", "3").should eq(2)
  end

  test "returns the score of a member in a sorted set at key" do
    redis.zadd(key, "1", "one")
    redis.zscore(key, "one").should eq("1")
  end

  test "can scan a sorted set" do
    values = Array.new(1_000, &.to_s).to_set
    scores = values.flat_map { |value| [value, rand.to_s] }

    redis.zadd key, scores
    redis.zscan_each key do |score, member|
      if values.includes? member
        values.delete member
      else
        raise "Yielded a member that does not exist: #{member}"
      end
    end

    values.should be_empty
  end

  test "can add a value only if it doesnt exist" do
    redis.zscore(key, "value").should eq nil
    redis.zadd(key, {"1", "value"}, nx: true).should eq 1
    redis.zscore(key, "value").should eq "1"
    redis.zadd(key, {"2", "value"}, nx: true).should eq 0
    redis.zscore(key, "value").should eq "1"
  end

  test "can add a value only if it DOES exist" do
    redis.zadd(key, {"1", "value"}, xx: true).should eq 0
    redis.zscore(key, "value").should eq nil
    redis.zadd key, {"1", "value"}
    redis.zadd(key, {"2", "value"}, xx: true).should eq 0 # Wasn't added, just changed
    redis.zscore(key, "value").should eq "2"
  end

  test "can add a value only if it is less than the current score" do
    redis.zadd key, {"1", "value"}
    redis.zadd(key, {"2", "value"}, lt: true).should eq 0
    redis.zscore(key, "value").should eq "1"
    redis.zadd(key, {"0.5", "value"}, lt: true).should eq 0
    redis.zscore(key, "value").should eq "0.5"
  end

  test "can add a value only if it is greater than the current score" do
    redis.zadd key, {"1", "value"}
    redis.zadd(key, {"0.5", "value"}, gt: true).should eq 0
    redis.zscore(key, "value").should eq "1"
    redis.zadd(key, {"2", "value"}, gt: true).should eq 0
    redis.zscore(key, "value").should eq "2"
  end

  test "can return the number of keys that changed, not just added" do
    redis.zadd key, {"1", "first", "2", "second"}
    # Using GT and CH to illustrate combining arguments
    redis.zadd(key, {"0.5", "first", "2.5", "second"}, gt: true, ch: true).should eq 1
  end

  test "can treat scores as increments" do
    redis.zadd key, {"4321", "first"}
    redis.zadd key, {"1234", "first"}, incr: true
    redis.zscore(key, "first").should eq "5555"
  end
end
