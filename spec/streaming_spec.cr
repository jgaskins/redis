require "./spec_helper"

require "../src/client"
require "../src/streaming"

module StreamingSpec
  include Redis::Streaming

  redis = Redis::Client.new
  define_test redis
  test = TestRunner.new(redis)

  describe Redis::Streaming do
    if test.server_version >= Version["6.2.0"]
      describe XReadGroupResponse do
        test "parses the result array" do
          group = UUID.v4.to_s
          consumer = UUID.v4.to_s
          redis.xgroup_create key, group, mkstream: true
          redis.xgroup_create_consumer key, group, consumer
          one = redis.xadd(key, "*", fields: {one: "1", two: "2"}).not_nil!
          two = redis.xadd(key, "*", fields: {three: "3"}).not_nil!
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
            Message.new(one, {"one" => "1", "two" => "2"}),
            Message.new(two, {"three" => "3"}),
          ]
        end
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

    describe XInfoStreamResponse do
      test "parses info about a stream" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "consumer"
        id = redis.xadd key, "*", fields: {id: "0"}
        redis.xreadgroup "group", "consumer", streams: {key => ">"}

        info = XInfoStreamResponse.new(redis.xinfo_stream(key))

        info.entries_added.should eq 1 # XADD has only been called once
        info.groups.should eq 1        # We added a group above
        info.length.should eq 1

        redis.xreadgroup "group", "consumer", streams: {key => ">"}
        redis.xgroup_create key, "another group"
        XInfoStreamResponse.new(redis.xinfo_stream(key)).groups.should eq 2
      end
    end

    describe XInfoStreamFullResponse do
      test "parses extended info about a stream" do
        results = redis.pipeline do |redis|
          redis.xgroup_create key, "group", mkstream: true
          redis.xgroup_create_consumer key, "group", "one"
          redis.xgroup_create_consumer key, "group", "two"
          50.times do |i|
            redis.xadd key, "*", fields: {id: i.to_s}
          end
          redis.xreadgroup "group", "one", streams: {key => ">"}, count: 3
          redis.xreadgroup "group", "two", streams: {key => ">"}, count: 5
        end
        from_one = XReadGroupResponse.new(results[-2].as(Array))
        from_two = XReadGroupResponse.new(results[-1].as(Array))
        redis.xackdel key, "group", from_two.each_message.first(3).map(&.id).to_a

        info = XInfoStreamFullResponse.new(redis.xinfo_stream_full(key, count: 10))

        info.entries_added.should eq 50
        info.groups.size.should eq 1
        info.length.should eq 47
      end
    end

    describe XInfoGroupsResponse do
      test "parses the groups for a stream" do
        redis.xgroup_create key, "one", mkstream: true
        redis.xgroup_create key, "two"
        redis.xgroup_create_consumer key, "one", "consumer"
        redis.xadd key, "*", fields: {id: "0"}
        redis.xreadgroup "one", "consumer", streams: {key => ">"}

        info = XInfoGroupsResponse.new(redis.xinfo_groups(key))

        one, two = info.groups
        one.name.should eq "one"
        one.consumers.should eq 1
        one.last_delivered_id.should_not eq "0-0"
        two.name.should eq "two"
        two.consumers.should eq 0
        two.last_delivered_id.should eq "0-0"
      end
    end

    describe XInfoConsumersResponse do
      test "parses the consumers for a group" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "consumer"

        consumer = XInfoConsumersResponse.new(redis.xinfo_consumers(key, "group")).consumers.first

        consumer.name.should eq "consumer"
        # It could be either value depending on how fast we query it, so we just
        # check that it's one of them.
        consumer.idle.should be_in 0, 1
        consumer.inactive.should eq -1
        consumer.pending.should eq 0
      end
    end

    describe XPendingResponse do
      test "parses the PEL for a group" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "one"
        redis.xgroup_create_consumer key, "group", "two"
        # Four total messages ...
        first = redis.xadd key, "*", fields: {id: "1"}
        second = redis.xadd key, "*", fields: {id: "2"}
        third = redis.xadd key, "*", fields: {id: "3"}
        fourth = redis.xadd key, "*", fields: {id: "4"}
        # ... and two different consumers each read some
        redis.xreadgroup "group", "one", streams: {key => ">"}, count: 1
        redis.xreadgroup "group", "two", streams: {key => ">"}, count: 2

        pending = XPendingResponse.new(redis.xpending(key, "group"))

        pending.count.should eq 3
        pending.earliest.should eq first
        pending.latest.should eq third
        one, two = pending.data
        one.consumer.should eq "one"
        one.pending_count.should eq 1
        two.consumer.should eq "two"
        two.pending_count.should eq 2
      end
    end

    describe XPendingExtendedResponse do
      test "parses the PEL for a group" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "one"
        redis.xgroup_create_consumer key, "group", "two"
        # Four total messages ...
        first_id = redis.xadd key, "*", fields: {id: "1"}
        second_id = redis.xadd key, "*", fields: {id: "2"}
        third_id = redis.xadd key, "*", fields: {id: "3"}
        fourth_id = redis.xadd key, "*", fields: {id: "4"}
        # ... and two different consumers each read some
        redis.xreadgroup "group", "one", streams: {key => ">"}, count: 1
        redis.xreadgroup "group", "two", streams: {key => ">"}, count: 2

        pending = XPendingExtendedResponse.new(
          redis.xpending key, "group",
            start: "0",
            end: "+",
            count: "4",
        )

        pending.messages.size.should eq 3
        first, second, third = pending.messages
        first.consumer.should eq "one"
        first.id.should eq first_id
        second.consumer.should eq "two"
        second.id.should eq second_id
        third.consumer.should eq "two"
        third.id.should eq third_id
      end
    end
  end

  describe Redis::Commands::Stream do
    describe "creating and deleting groups and consumers" do
      test "creates and deletes a group" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xinfo_groups(key).should_not be_empty

        redis.xgroup_destroy(key, "group")
        redis.xinfo_groups(key).should be_empty
      end

      test "creates and deletes consumers" do
        redis.xgroup_create key, "group", mkstream: true

        redis.xgroup_create_consumer key, "group", "consumer"
        redis.xinfo_consumers(key, "group").should_not be_empty

        redis.xgroup_del_consumer key, "group", "consumer"
        redis.xinfo_consumers(key, "group").should be_empty
      end
    end

    describe "acknowledging messages in a consumer group" do
      test "acks a message" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "consumer"
        id = redis.xadd key, "*", {id: "0"}
        get_pending = -> do
          groups = redis.xinfo_groups(key)
          Redis.to_hash(groups.first.as(Array))["pending"]
        end

        get_pending.call.should eq 0 # Nothing in the PEL yet

        redis.xreadgroup "group", "consumer", streams: {key => ">"}
        get_pending.call.should eq 1 # Reading a msg puts it into the PEL

        redis.xack key, "group", [id.not_nil!]
        get_pending.call.should eq 0 # XACKing a msg removes it from the PEL
      end

      test "acks and deletes a message in a single call" do
        redis.xgroup_create key, "group", mkstream: true
        redis.xgroup_create_consumer key, "group", "consumer"
        first = redis.xadd key, "*", {id: "0"}
        second = redis.xadd key, "*", {id: "1"}
        redis.xreadgroup "group", "consumer", streams: {key => ">"}, count: 2

        # https://redis.io/docs/latest/commands/xackdel/#optional-arguments
        # Without a DeleteMode (server defaults to KEEPREF)
        redis.xackdel key, "group", [first.as(String)]
        # With an explicit DeleteMode
        redis.xackdel key, "group", :acked, [second.as(String)]

        redis.xlen(key).should eq 0
      end
    end

    describe "trimming" do
      test "trims a stream to an approximate size with MAXLEN" do
        redis.multi do |redis|
          200.times do |i|
            redis.xadd key, "*", {id: i.to_s}
          end
        end

        redis.xtrim(key, maxlen: {"~", "90"}).should eq 100
      end

      test "trims a stream to an approximate minimum id with MINID" do
        redis.multi do |redis|
          200.times do |i|
            i += 1 # Can't use an id of `0`
            redis.xadd key, i.to_s, {id: i.to_s}
          end
        end

        redis.xtrim(key, minid: {"~", "110"}).should eq 100
      end

      test "accepts a delete mode" do
        redis.multi do |redis|
          redis.xgroup_create key, "group", mkstream: true
          redis.xgroup_create_consumer key, "group", "consumer"

          200.times do |i|
            i += 1 # Can't use an id of `0`
            redis.xadd key, i.to_s, {id: i.to_s}
          end

          redis.xreadgroup "group", "consumer", streams: {key => ">"}, count: 200
          redis.xack key, "group", Array.new(200) { |i| (i + 1).to_s }
        end

        redis.xtrim(key, minid: {"~", "100"}, delete_mode: :acked).should be_within 10, of: 100

        redis.xlen(key).should be_within 10, of: 100
      end
    end

    if test.server_version >= Version["8.0.0"]
      test "publishes and reads per-group" do
        redis.xgroup_create key, "group1", id: "$", mkstream: true
        redis.xgroup_create_consumer key, "group1", "consumer"
        redis.xgroup_create key, "group2", id: "$"
        redis.xgroup_create_consumer key, "group2", "consumer"

        redis.xadd key, "*", fields: {id: "1"}

        redis.xreadgroup("group1", "consumer", streams: {key => ">"}).should_not be_nil
        redis.xreadgroup("group1", "consumer", streams: {key => ">"}).should be_nil

        redis.xreadgroup("group2", "consumer", streams: {key => ">"}).should_not be_nil
        redis.xreadgroup("group2", "consumer", streams: {key => ">"}).should be_nil
      end

      if test.server_version >= Version["8.4.0"]
        test "reads with a claim time" do
          redis.xgroup_create key, "group", id: "$", mkstream: true
          redis.xgroup_create_consumer key, "group", "consumer1"
          redis.xgroup_create_consumer key, "group", "consumer2"

          redis.xadd key, "*", fields: {id: "1"}

          redis.xreadgroup("group", "consumer1", streams: {key => ">"}).should_not be_nil
          sleep 1.millisecond
          redis.xreadgroup("group", "consumer2", streams: {key => ">"}, claim: 1.millisecond).should_not be_nil
        end
      end

      if test.server_version >= Version["8.8.0"]
        test "negatively acknowledges messages" do
          redis.xgroup_create key, "group", id: "$", mkstream: true
          redis.xgroup_create_consumer key, "group", "consumer1"
          redis.xgroup_create_consumer key, "group", "consumer2"
          redis.xadd key, "*", fields: {foo: "asdf"}
          response = XReadGroupResponse.hash(
            redis.xreadgroup("group", "consumer1", streams: {key => ">"}).not_nil!
          )

          redis.xnack(key, "group", :fail, response[key].map(&.id)).should eq 1

          # Even though we're specifying a 30-second min claim, we get the
          # message immediately because of the XNACK command above
          redis
            .xreadgroup(
              group: "group",
              consumer: "consumer2",
              streams: {key => ">"},
              claim: 30.seconds,
            )
            .should_not be_nil
        end
      end
    end
  end
end
