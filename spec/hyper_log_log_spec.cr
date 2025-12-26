require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Commands::HyperLogLog do
  test "can add members and count them" do
    redis.pipeline do |redis|
      3.times { |i| redis.pfadd key, i.to_s }
    end

    redis.pfcount(key).should eq 3
  end

  test "can add multiple members at a time through both variadic args and enumerables" do
    redis.pfadd key, "one", "two", "three"
    redis.pfadd key, %w[four five six]

    redis.pfcount(key).should eq 6
  end

  it "can count multiple hyperloglogs using variadic args or enumerables of keys" do
    a = UUID.v4.to_s
    b = UUID.v4.to_s

    begin
      redis.pipeline do |redis|
        redis.pfadd a, %w[one two three]
        redis.pfadd b, %w[four five six]
      end

      redis.pfcount(a, b).should eq 6
      redis.pfcount([a, b]).should eq 6
    ensure
      redis.unlink a, b
    end
  end

  it "can merge multiple hyperloglogs using variadic args" do
    a = UUID.v4.to_s
    b = UUID.v4.to_s
    merged = UUID.v4.to_s

    begin
      redis.pipeline do |redis|
        redis.pfadd a, %w[one two three]
        redis.pfadd b, %w[four five six]
        redis.pfmerge merged, a, b
      end

      redis.pfcount(merged).should eq 6
    ensure
      redis.unlink a, b, merged
    end
  end

  it "can merge multiple hyperloglogs using enumerables of source keys" do
    a = UUID.v4.to_s
    b = UUID.v4.to_s
    merged = UUID.v4.to_s

    begin
      redis.pipeline do |redis|
        redis.pfadd a, %w[one two three]
        redis.pfadd b, %w[four five six]
        redis.pfmerge merged, [a, b]
      end

      redis.pfcount(merged).should eq 6
    ensure
      # Clean up the keys we just created
      redis.unlink a, b, merged
    end
  end
end
