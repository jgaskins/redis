require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Commands::Stream do
  test "can use streams" do
    entry_ids = [
      redis.xadd(key, "*", {"foo" => "bar"}),
      redis.xadd(key, "*", {foo: "bar"}),
    ]
    range = redis.xrange(key, "-", "+")
    range.size.should eq 2
    range.each_with_index do |result, index|
      id, data = result.as(Array)
      id.should eq entry_ids[index]
      data.should eq %w[foo bar]
    end
  end

  describe "#xread" do
    it "can read from multiple streams" do
      one_id = redis.xadd "first_stream", "*", fields: {one: "1"}
      two_id = redis.xadd "first_stream", "*", fields: {two: "2"}
      three_id = redis.xadd "second_stream", "*", fields: {three: "3"}
      four_id = redis.xadd "second_stream", "*", fields: {four: "4"}

      expected = [
        [
          "first_stream", [
            [one_id, ["one", "1"]],
            [two_id, ["two", "2"]],
          ],
        ],
        [
          "second_stream", [
            [three_id, ["three", "3"]],
            [four_id, ["four", "4"]],
          ],
        ],
      ]
      redis.xread(streams: {first_stream: "0", second_stream: "0"}).should eq expected
      redis.xread(streams: {"first_stream" => "0", "second_stream" => "0"}).should eq expected
    ensure
      redis.del "first_stream", "second_stream"
    end

    it "can specify a maximum number of messages to read per stream" do
      3.times do |i|
        redis.xadd "my-stream", "*", fields: {id: i.to_s}
      end

      [
        {"my-stream" => "0"},
        {"my-stream": "0"},
      ].each do |streams|
        redis.xread(count: 2, streams: streams).as(Array)
          .first.as(Array)[1].as(Array)
          .size
          .should eq 2
      end
    ensure
      redis.del "my-stream"
    end

    [
      {"my-stream" => "0"},
      {"my-stream": "0"},
    ].each do |streams|
      it "can specify a duration to block waiting for messages to be added to the stream (#{streams.class})" do
        spawn do
          sleep 10.milliseconds
          redis.xadd "my-stream", "*", {id: "1"}
        end

        redis.xread(block: 100.milliseconds, streams: streams).as(Array)
          .first.as(Array)[1].as(Array)
          .size
          .should eq 1
      ensure
        redis.del "my-stream"
      end
    end
  end

  describe "#xrevrange" do
    test "traverses streams in reverse" do
      entry_ids = [
        redis.xadd(key, "*", {"foo" => "bar"}),
        redis.xadd(key, "*", {foo: "bar"}),
      ].reverse
      range = redis.xrevrange(key, "+", "-")
      range.size.should eq 2
      range.each_with_index do |result, index|
        id, data = result.as(Array)
        id.should eq entry_ids[index]
        data.should eq %w[foo bar]
      end
    end
  end

  test "can cap streams by event count" do
    redis.pipeline do |pipe|
      11.times do
        pipe.xadd key, "*",
          maxlen: {"=", "10"},
          fields: {foo: "bar"}
      end
    end

    redis.xlen(key).should eq 10
  end

  test "can cap streams by id" do
    results = redis.pipeline do |pipe|
      minid = 10.seconds.ago.to_unix_ms.to_s

      10.times do |i|
        pipe.xadd key,
          "#{(11 - i).seconds.ago.to_unix_ms.to_s}-#{i}",
          {foo: "bar"}
      end
      pipe.xadd key, "*",
        minid: {"=", minid},
        fields: {foo: "bar"}
    end

    redis.xlen(key).should eq 10
  end

  test "can approximately cap streams" do
    redis.pipeline do |pipe|
      2_000.times { pipe.xadd key, "*", maxlen: {"~", "10"}, fields: {foo: "bar"} }
    end

    redis.xlen(key).should be <= 100
  end

  describe "#xreadgroup" do
    it "can consume streams" do
      key = "my-stream"
      group = "my-group"

      begin
        entry_id = redis.xadd key, "*", {foo: "bar"}
        # Create a group to consume this stream starting at the beginning
        redis.xgroup "create", key, group, "0"
        consumer_id = UUID.random.to_s

        result = redis.xreadgroup group, consumer_id, count: "10", streams: {"my-stream": ">"}
      rescue ex
        raise ex
      ensure
        redis.xgroup "destroy", key, group
        redis.del key
      end
    end
  end
end
