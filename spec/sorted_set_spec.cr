require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis
test = TestRunner.new(redis)

describe Redis::Commands::SortedSet do
  describe "zadd" do
    test "adds members" do
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

    test "can add a member only if it doesnt exist" do
      redis.zscore(key, "value").should eq nil
      redis.zadd(key, {"1", "value"}, nx: true).should eq 1
      redis.zscore(key, "value").should eq "1"
      redis.zadd(key, {"2", "value"}, nx: true).should eq 0
      redis.zadd(key, "3", "value", nx: true).should eq 0 # variadic
      redis.zscore(key, "value").should eq "1"
    end

    test "can add a member only if it DOES exist" do
      redis.zadd(key, {"1", "value"}, xx: true).should eq 0
      redis.zadd(key, "1", "value", xx: true).should eq 0
      redis.zscore(key, "value").should eq nil
      redis.zadd key, {"1", "value"}
      redis.zadd(key, {"2", "value"}, xx: true).should eq 0 # Wasn't added, just changed
      redis.zscore(key, "value").should eq "2"
      redis.zadd(key, "3", "value", xx: true).should eq 0 # variadic
      redis.zscore(key, "value").should eq "3"
    end

    if test.server_version >= Version["6.2.0"]
      test "can add a member only if it is less than the current score" do
        redis.zadd key, {"1", "value"}
        redis.zadd(key, {"2", "value"}, lt: true).should eq 0
        redis.zscore(key, "value").should eq "1"
        redis.zadd(key, "2", "value", lt: true).should eq 0 # variadic
        redis.zscore(key, "value").should eq "1"
        redis.zadd(key, {"0.5", "value"}, lt: true).should eq 0
        redis.zscore(key, "value").not_nil!.to_f.should be_within 0.000001, of: 0.5
        redis.zadd(key, "0.1", "value", lt: true).should eq 0 # variadic
        redis.zscore(key, "value").not_nil!.to_f.should be_within 0.000001, of: 0.1
      end

      test "can add a member only if it is greater than the current score" do
        redis.zadd key, {"1", "value"}
        redis.zadd(key, {"0.5", "value"}, gt: true).should eq 0
        redis.zscore(key, "value").should eq "1"
        redis.zadd(key, "0.5", "value", gt: true).should eq 0 # variadic
        redis.zscore(key, "value").should eq "1"
        redis.zadd(key, {"2", "value"}, gt: true).should eq 0
        redis.zscore(key, "value").should eq "2"
        redis.zadd(key, "3", "value", gt: true).should eq 0 # variadic
        redis.zscore(key, "value").should eq "3"
      end

      test "can return the number of members that changed, not just added" do
        redis.zadd key, {"1", "first", "2", "second"}
        # Using GT and CH to illustrate combining arguments
        redis.zadd(key, {"0.5", "first", "2.5", "second"}, gt: true, ch: true).should eq 1
        redis.zadd(key, "-0.5", "first", "3.5", "second", gt: true, ch: true).should eq 1
      end
    end

    test "can treat scores as increments" do
      redis.zadd key, {"4321", "first"}
      redis.zadd key, {"1234", "first"}, incr: true
      redis.zscore(key, "first").should eq "5555"
      redis.zadd key, "2222", "first", incr: true
      redis.zscore(key, "first").should eq "7777"
    end
  end

  test "zcard gets the size of a sorted set" do
    redis.zadd key, %w[
      1 one
      2 two
      3 three
      4 four
      5 five
    ]

    redis.zcard(key).should eq 5

    redis.zadd key, %w[6 six]

    redis.zcard(key).should eq 6
  end

  if test.server_version >= Version["6.2.0"]
    describe "#zdiff" do
      test "gets the difference between two sorted sets" do
        first = key
        second = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three 4 lol]
          redis.zadd second, %w[3 lol 4 four]

          redis.zdiff([first, second]).should eq %w[one two three]
          redis.zdiff(first, second).should eq %w[one two three]
        ensure
          redis.del first, second
        end
      end

      test "gets the difference between two sorted sets with their scores" do
        first = key
        second = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three 4 lol]
          redis.zadd second, %w[3 lol 4 four]

          redis.zdiff([first, second], withscores: true).should eq %w[one 1 two 2 three 3]
          redis.zdiff(first, second, withscores: true).should eq %w[one 1 two 2 three 3]
        ensure
          redis.del first, second
        end
      end
    end

    describe "#zdiffstore" do
      test "stores the difference between two sorted sets in a new zset" do
        first = key
        second = random_key
        destination = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three 4 lol]
          redis.zadd second, %w[3 lol 4 four]

          redis.zdiffstore(destination, [first, second]).should eq 3
          redis.zdiffstore(destination, first, second).should eq 3
        ensure
          redis.del first, second, destination
        end
      end
    end

    describe "#zinter" do
      test "gets the intersection between multiple sorted sets" do
        first = key
        second = random_key
        third = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three]
          redis.zadd second, %w[2 two 3 three 4 four]
          redis.zadd third, %w[3 three 4 four 5 five]

          redis.zinter(first, second, third).should eq %w[three]
          redis.zinter([first, second, third]).should eq %w[three]
        ensure
          redis.del first, second, third
        end
      end

      # Honestly, this test doesn't do anything more than illustrate that
      # `weights` can be passed. It doesn't change the return value. I don't
      # know what the point of this test is, tbh, but this is a valid way to
      # run `ZINTER`, so I guess we'll support it.
      test "uses weights for each sorted set" do
        first = key
        second = random_key
        third = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three]
          redis.zadd second, %w[2 two 3 three 4 four]
          redis.zadd third, %w[3 three 4 four 5 five]

          redis.zinter(
            keys: [first, second, third],
            weights: %w[1 2 3],
          ).should eq %w[three]
        ensure
          redis.del first, second, third
        end
      end

      test "uses weights with aggregates" do
        first = key
        second = random_key
        third = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three]
          redis.zadd second, %w[2 two 3 three 4 four]
          redis.zadd third, %w[3 three 4 four 5 five]

          redis.zinter(
            keys: [first, second, third],
            weights: %w[1 2 3],
            aggregate: :sum,
          ).should eq %w[three]
        ensure
          redis.del first, second, third
        end
      end

      test "uses weights with aggregates and fetches scores" do
        first = key
        second = random_key
        third = random_key

        begin
          redis.zadd first, %w[1 one 2 two 3 three]
          redis.zadd second, %w[2 two 3 three 4 four]
          redis.zadd third, %w[3 three 4 four 5 five]

          redis
            .zinter(
              keys: [first, second, third],
              weights: %w[2 4 6],
              aggregate: :max,
              withscores: true
            )
            # The max is 3, and with a 6x weight it becomes 18
            .should eq %w[three 18]
        ensure
          redis.del first, second, third
        end
      end
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
  end

  describe "#zinterstore" do
    test "stores the intersection of multiple sets in a destination key" do
      first = key
      second = random_key
      destination = random_key

      begin
        redis.zadd first, %w[1 one 2 two 3 three]
        redis.zadd second, %w[2 two 3 three 4 four]

        redis.zinterstore(destination, first, second).should eq 2
        redis.zinterstore(destination, [first, second]).should eq 2
      ensure
        redis.del first, second, destination
      end
    end

    test "stores the intersection of multiple sets in a destination key with weights and aggregates" do
      first = key
      second = random_key
      destination = random_key

      begin
        redis.zadd first, %w[1 one 2 two 3 three]
        redis.zadd second, %w[2 two 3 three 4 four]

        redis
          .zinterstore(
            destination: destination,
            keys: [first, second],
            weights: %w[2 4],
            aggregate: :max
          )
          .should eq 2
        redis.zrange(destination, "0", "-1", with_scores: true).should eq %w[
          two 4
          three 6
        ]
      ensure
        redis.del first, second, destination
      end
    end
  end

  test "zincrby increments the score of the given member by the given amount" do
    redis.zadd key, %w[1 member]
    redis.zincrby(key, "1", "member").should eq "2"
  end

  describe "zrank" do
    test "returns the rank of the given member" do
      redis.zadd key, %w[
        1 one
        2 two
        3 three
        4 four
      ]

      redis.zrank(key, "one").should eq 0
      redis.zrank(key, "one", withscore: true).should eq [0, "1"]
    end
  end

  describe "zremrangebyrank" do
    test "removes with a range" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      # Removing all but the first and last
      redis.zremrangebyrank(key, 1...99).should eq 98

      redis.zrange(key, 0, -1).should eq %w[entry-0 entry-99]
    end

    test "removes with start and stop as ints" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyrank(key, 0i8, 0i8).should eq 1
      redis.zremrangebyrank(key, 0i16, 0i16).should eq 1
      redis.zremrangebyrank(key, 0i32, 0i32).should eq 1
      redis.zremrangebyrank(key, 0i64, 0i64).should eq 1
    end

    test "removes with start and stop as strings" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyrank(key, "0", "0").should eq 1
    end
  end

  describe "zremrangebyscore" do
    test "removes with a range" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyscore(key, 0...10).should eq 10
      redis.zremrangebyscore(key, 10.0...20.0).should eq 10
      redis.zremrangebyscore(key, ...30).should eq 10
      redis.zremrangebyscore(key, ..40).should eq 11
    end

    test "removes with min and max as floats" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyscore(key, min: 0.0, max: 9.9).should eq 10
      redis.zremrangebyscore(key, min: 10.0_f32, max: 19.9_f32).should eq 10
    end

    test "removes with min and max as ints" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyscore(key, min: 0i8, max: 9i8).should eq 10
      redis.zremrangebyscore(key, min: 10i16, max: 19i16).should eq 10
      redis.zremrangebyscore(key, min: 20i32, max: 29i32).should eq 10
      redis.zremrangebyscore(key, min: 30i64, max: 39i64).should eq 10
    end

    test "removes with min and max as strings" do
      redis.zadd key, Array.new(100) { |i| [i.to_s, "entry-#{i}"] }.flatten

      redis.zremrangebyscore(key, min: "0.0", max: "9.9").should eq 10
    end
  end

  describe "zremrangebylex" do
    test "removes with a range" do
      redis.zadd key, Array.new(100) { |i| ["0", "entry-%02d" % i] }.flatten

      redis.zremrangebylex(key, "entry-0"..."entry-10").should eq 10
      redis.zremrangebylex(key, "entry-10".."entry-20").should eq 11
    end

    test "removes with min and max as strings" do
      redis.zadd key, Array.new(100) { |i| ["0", "entry-%02d" % i] }.flatten

      redis.zremrangebylex(key, "[entry-0", "(entry-10").should eq 10
      redis.zremrangebylex(key, "[entry-10", "[entry-20").should eq 11
    end

    test "requires that min and max be properly formatted" do
      expect_raises(ArgumentError) { redis.zremrangebylex(key, "min", "[max") }
      expect_raises(ArgumentError) { redis.zremrangebylex(key, "[min", "max") }
    end
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

  if test.server_version >= Version["7.0.0"]
    describe "#bzmpop" do
      test "removes and returns from the bottom of the sorted set" do
        spawn do
          # We time out after 1 second below, so we delay a little bit here to test the blocking
          sleep 10.milliseconds
          redis.zadd key, %w[
            1 one
            2 two
            3 three
            4 four
            5 five
            6 six
          ]
        end

        redis.bzmpop(1.second, [key], :min).should eq [key, [%w[one 1]]]
        redis.bzmpop(1.second, [key], :min, count: 2).should eq [key, [%w[two 2], %w[three 3]]]
        redis.bzmpop(1.second, [key], :max).should eq [key, [%w[six 6]]]
        redis.bzmpop(1.second, [key], :max, count: 2).should eq [key, [%w[five 5], %w[four 4]]]
      end
    end
  end

  describe "#bzpopmax" do
    test "removes and returns from the top of the sorted set" do
      spawn do
        # We time out after 1 second below, so we delay a little bit here to test the blocking
        sleep 10.milliseconds
        redis.zadd key, %w[
          1 one
          2 two
          3 three
          4 four
          5 five
          6 six
        ]
      end

      redis.bzpopmax([key], 1.second).should eq [key, "six", "6"]
    end
  end

  describe "#bzpopmin" do
    test "removes and returns from the bottom of the sorted set" do
      spawn do
        # We time out after 1 second below, so we delay a little bit here to test the blocking
        sleep 10.milliseconds
        redis.zadd key, %w[
          1 one
          2 two
          3 three
          4 four
          5 five
          6 six
        ]
      end

      redis.bzpopmin([key], 1.second).should eq [key, "one", "1"]
    end
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
end
