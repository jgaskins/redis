require "./spec_helper"

require "../src/client"
require "../src/streaming"

module Redis::Streaming
  redis = Redis::Client.new
  define_test redis

  describe Redis::Streaming do
    describe XReadGroupResponse do
      test "parses the result array" do
        group = UUID.v4.to_s
        consumer = UUID.v4.to_s
        redis.xgroup_create key, group, mkstream: true
        redis.xgroup_create_consumer key, group, consumer
        one = redis.xadd(key, "*", fields: {one: "1"}).not_nil!
        two = redis.xadd(key, "*", fields: {two: "2"}).not_nil!
        response = redis
          .xreadgroup(
            group: group,
            consumer: consumer,
            streams: {key => ">"},
          )         # Array?
          .not_nil! # Array

        response = XReadGroupResponse.new(response)

        response.results.size.should eq 1
        response.results.first.key.should eq key
        response.results.first.messages.size.should eq 2
        response.results.first.messages.should eq [
          Message.new(one, {"one" => "1"}),
          Message.new(two, {"two" => "2"}),
        ]
      end
    end

    describe XReadResponse do
      it "parses the result array" do
        one_id = redis.xadd "first_stream", "*", fields: {one: "1"}
        two_id = redis.xadd "first_stream", "*", fields: {two: "2"}
        three_id = redis.xadd "second_stream", "*", fields: {three: "3"}
        four_id = redis.xadd "second_stream", "*", fields: {four: "4"}

        response = redis
          .xread(streams: {
            first_stream:  "0",
            second_stream: "0",
          })        # Array | Nil
          .not_nil! # Array
        response = XReadResponse.new(response)

        response.dig("first_stream", 0, "one").should eq "1"
        response.dig("first_stream", 1, "two").should eq "2"
        response.dig("second_stream", 0, "three").should eq "3"
        response.dig("second_stream", 1, "four").should eq "4"
      ensure
        redis.del "first_stream", "second_stream"
      end
    end
  end
end
