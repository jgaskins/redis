require "./spec_helper"

require "../src/redis"
require "../src/bloom_filter"

redis = Redis::Client.new
define_test redis

describe Redis::BloomFilter do
  test "reserves a bloom filter" do
    redis.bf.reserve key,
      error_rate: 0.01,
      capacity: 10

    Redis.to_hash(redis.bf.info(key).as(Array))["Capacity"].should eq 10
  end

  test "inserts an item into a bloom filter, creating it if needed" do
    result = redis.bf.insert key,
      error: 0.01,
      capacity: 10,
      items: ["foo"]
    result.should eq [1]

    result = redis.bf.insert key,
      error: 0.01,
      capacity: 10,
      items: ["foo"]
    result.should eq [0]
  end

  test "checks whether an item exists in a bloom filter" do
    redis.bf.insert key, error: 0.01, capacity: 10, items: %w[foo]

    redis.bf.exists(key, "foo").should eq 1
    redis.bf.exists(key, "doesn't exist").should eq 0
  end

  test "gets the cardinality of a bloom filter" do
    redis.bf.insert key, error: 0.01, capacity: 10, items: %w[
      one
      two
      three
    ]

    redis.bf.card(key).should eq 3
  end

  test "adds an item to an existing bloom filter" do
    redis.bf.reserve key,
      error_rate: 0.01,
      capacity: 10

    redis.bf.add(key, "one").should eq 1
    redis.bf.add(key, "one").should eq 0

    redis.bf.card(key).should eq 1
  end

  test "adds multiple items to a bloom filter" do
    redis.bf.reserve key,
      error_rate: 0.01,
      capacity: 10

    redis.bf.madd(key, %w[one two three]).should eq [1, 1, 1]
    redis.bf.madd(key, %w[one two three four]).should eq [0, 0, 0, 1]

    redis.bf.card(key).should eq 4
  end
end
