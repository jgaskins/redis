require "./spec_helper"

require "../src/redis"
require "../src/count_min_sketch"

redis = Redis::Client.new
define_test redis
runner = TestRunner.new(redis)

if runner.has_module? "bf"
  describe Redis::CountMinSketch do
    test "initializes a CMS by probability" do
      redis.cms.initbyprob(key, error: 0.01, probability: 0.01).should eq "OK"

      redis.type(key).should eq "CMSk-TYPE"
    end

    test "initializes a CMS by dimensions" do
      redis.cms.initbydim(key, width: 1_000, depth: 1_000).should eq "OK"

      redis.type(key).should eq "CMSk-TYPE"
    end

    test "increments an item by an amount" do
      redis.pipeline do |redis|
        redis.cms.initbyprob key, error: 0.01, probability: 0.01

        redis.cms.incrby key, "one", 1
        redis.cms.incrby key, "two", 2
      end

      redis.cms.query(key, "one", "two").should eq [1, 2]
    end

    it "merges two CMSes into a third" do
      a = UUID.v7.to_s
      b = UUID.v7.to_s
      c = UUID.v7.to_s
      merged = UUID.v7.to_s

      begin
        redis.pipeline do |redis|
          redis.cms.initbyprob a, error: 0.01, probability: 0.01
          redis.cms.initbyprob b, error: 0.01, probability: 0.01
          redis.cms.initbyprob c, error: 0.01, probability: 0.01
          redis.cms.initbyprob merged, error: 0.01, probability: 0.01

          redis.cms.incrby a, "one", 1
          redis.cms.incrby b, "two", 2
          redis.cms.incrby c, "three", 3

          redis.cms.merge merged, [a, b, c]
        end

        redis.cms.query(merged, "one", "two", "three").should eq [1, 2, 3]
      ensure
        redis.del a, b, c, merged
      end
    end

    test "returns info about the CMS" do
      redis.cms.initbyprob key, error: 0.01, probability: 0.01

      Redis.to_hash(redis.cms.info(key)).should eq({
        "width" => 200,
        "depth" => 7,
        "count" => 0,
      })
    end
  end
end
