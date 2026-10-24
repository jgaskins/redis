require "./spec_helper"

require "../src/redis"
require "../src/bloom_filter"

redis = Redis::Client.new
define_test redis

runner = TestRunner.new(redis)

if runner.has_module? "bf"
  describe Redis::BloomFilter do
    test "reserves a bloom filter" do
      redis.bf.reserve key,
        error_rate: 0.01,
        capacity: 10

      redis.bf.info(key).as(Hash)["Capacity"].should eq 10
    end

    test "inserts an item into a bloom filter, creating it if needed" do
      result = redis.bf.insert key,
        error: 0.01,
        capacity: 10,
        items: ["foo"]
      result.should eq [true]

      result = redis.bf.insert key,
        error: 0.01,
        capacity: 10,
        items: ["foo"]
      result.should eq [false]
    end

    test "checks whether an item exists in a bloom filter" do
      redis.bf.insert key, error: 0.01, capacity: 10, items: %w[foo]

      redis.bf.exists(key, "foo").should eq true
      redis.bf.exists(key, "doesn't exist").should eq false
    end

    test "checks whether many items exist in a bloom filter" do
      redis.bf.insert key, error: 0.01, capacity: 10, items: %w[one two]

      redis.bf.mexists(key, %w[one two three]).should eq [true, true, false]
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

      redis.bf.add(key, "one").should eq true
      redis.bf.add(key, "one").should eq false

      redis.bf.card(key).should eq 1
    end

    test "adds multiple items to a bloom filter" do
      redis.bf.reserve key,
        error_rate: 0.01,
        capacity: 10

      redis.bf.madd(key, %w[one two three]).should eq [true, true, true]
      redis.bf.madd(key, %w[one two three four]).should eq [false, false, false, true]

      redis.bf.card(key).should eq 4
    end
  end
end
