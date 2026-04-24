require "./spec_helper"

require "../src/redis"
require "../src/tdigest"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

if test.has_module? "bf"
  describe Redis::TDigest do
    test "creates a t-digest sketch" do
      redis.tdigest.create(key, compression: 100).should eq "OK"
      redis.type(key).should eq "TDIS-TYPE"

      redis.del key

      redis.tdigest.create(key).should eq "OK"
      redis.type(key).should eq "TDIS-TYPE"
    end

    test "adds observations" do
      redis.tdigest.create key

      redis.tdigest.add(key, "12").should eq "OK"
      redis.tdigest.add(key, "12", "34").should eq "OK"
      redis.tdigest.add(key, ["12", "34"]).should eq "OK"
    end

    test "gets the value for a quantile" do
      redis.tdigest.create key

      redis.tdigest.add key, (0..10).map(&.to_s)

      redis.tdigest.quantile(key, "0.5").should eq %w[5]
      redis.tdigest.quantile(key, "0.1", "0.2", "0.3").should eq %w[1 2 3]
      redis.tdigest.quantile(key, %w[0.1 0.2 0.3]).should eq %w[1 2 3]
    end

    test "gets the rank of a value" do
      redis.tdigest.create key

      # All values are -2 if the sketch is empty
      redis.tdigest.rank(key, "0").should eq [-2]

      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.rank(key, "0").should eq [-1] # Smaller than all values
      redis.tdigest.rank(key, "1").should eq [0]  # Equal to min value
      redis.tdigest.rank(key, "3").should eq [1]  # Equal to second value
      redis.tdigest.rank(key, "12").should eq [5] # Larger than largest value
      # All together now
      redis.tdigest.rank(key, "0", "1", "3", "12").should eq [-1, 0, 1, 5]
      redis.tdigest.rank(key, %w[0 1 3 12]).should eq [-1, 0, 1, 5]
    end

    test "resets the t-digest sketch" do
      redis.tdigest.create key
      redis.tdigest.add key, Array.new(100) { rand.to_s }

      redis.tdigest.reset(key).should eq "OK"

      redis.tdigest.rank(key, "1").should eq [-2]
    end

    test "gets the reverse rank of a value" do
      redis.tdigest.create key

      # All values are -2 if the sketch is empty
      redis.tdigest.revrank(key, "0").should eq [-2]

      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.revrank(key, "0").should eq [5]   # Smaller than all values
      redis.tdigest.revrank(key, "1").should eq [4]   # Equal to min value
      redis.tdigest.revrank(key, "3").should eq [3]   # Equal to second value
      redis.tdigest.revrank(key, "12").should eq [-1] # Larger than largest value
      # All together now
      redis.tdigest.revrank(key, "0", "1", "3", "12").should eq [5, 4, 3, -1]
      redis.tdigest.revrank(key, %w[0 1 3 12]).should eq [5, 4, 3, -1]
    end

    test "gets the max" do
      redis.tdigest.create key
      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.max(key).should eq "9"
    end

    test "gets the min" do
      redis.tdigest.create key
      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.min(key).should eq "1"
    end

    test "gets the cumulative distribution function of each value" do
      redis.tdigest.create key
      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.cdf(key, "5").should eq %w[0.5]
      redis.tdigest.cdf(key, "5", "10").should eq %w[0.5 1]
      redis.tdigest.cdf(key, %w[5 10]).should eq %w[0.5 1]
    end

    test "gets the value of each rank" do
      redis.tdigest.create key
      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.byrank(key, "3").should eq %w[7]
      redis.tdigest.byrank(key, "0", "1", "2").should eq %w[1 3 5]
      redis.tdigest.byrank(key, %w[0 1 2]).should eq %w[1 3 5]
    end

    test "gets the value of each reverse rank" do
      redis.tdigest.create key
      redis.tdigest.add key, %w[1 3 5 7 9].shuffle

      redis.tdigest.byrevrank(key, "3").should eq %w[3]
      redis.tdigest.byrevrank(key, "0", "1", "2").should eq %w[9 7 5]
      redis.tdigest.byrevrank(key, %w[0 1 2]).should eq %w[9 7 5]
    end

    test "merges multiple t-digests into a single key" do
      a = UUID.v7.to_s
      b = UUID.v7.to_s

      begin
        redis.tdigest.create a, compression: 1000
        redis.tdigest.create b, compression: 1000
        redis.tdigest.add a, %w[1 3 5 7 9]
        redis.tdigest.add b, %w[2 4 6 8 10]

        redis.tdigest.merge key, source_keys: [a, b]

        redis.tdigest.byrank(key, %w[0 1 2 3 4 5 6 7 8 9])
          .should eq %w[1 2 3 4 5 6 7 8 9 10]
      ensure
        redis.del a, b
      end
    end

    test "returns the trimmed mean" do
      redis.tdigest.create key
      redis.tdigest.add key, (1..100).map(&.to_s).shuffle

      redis.tdigest.trimmed_mean(key, "0.25", "0.75").should eq "50.5"
    end
  end
end
