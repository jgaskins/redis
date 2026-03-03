require "uuid"
require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Client do
  context "transactions" do
    test "returns the results of the commands, like pipelines do" do
      redis.multi do |redis|
        redis.set key, "value"
        redis.get key
      end.should eq %w[OK value]
    end

    test "returns an empty array when the transaction is discarded" do
      result = redis.multi do |redis|
        redis.set key, "this gets discarded"
        redis.discard
        redis.get "this never actually does anything anyway"
      end.as(Array).should be_empty
    end

    test "returns command errors, but does not raise" do
      redis.multi do |redis|
        redis.set key, "foo"
        redis.lpush key, "bar" # error, performing list operation on a string
        redis.get key
      end.should eq [
        "OK",
        Redis::Error.new("WRONGTYPE Operation against a key holding the wrong kind of value"),
        "foo",
      ]
    end

    test "allows the block to return/break" do
      value = redis.multi do |redis|
        redis.set key, "value"
        break 1
      end

      value.should eq 1
      redis.get(key).should eq "value"
    end

    test "allows watching a key for changes" do
      redis.set key, "old"

      redis.watch key do |connection|
        # Since the `watch` block checks out a connection, this will check out a
        # second connection. This way we don't run the `WATCH` command and the
        # transaction on two different connections.
        redis.set key, "conflict"

        txn_result = connection.multi do |redis|
          redis.set key, "new"
        end

        # The transaction wasn't committed and returns nothing
        txn_result.should eq nil
      end

      # Even though "new" comes after "conflict", it doesn't get set because we
      # set the value in a separate connection after a `watch` on the
      # transaction's connection.
      redis.get(key).should eq "conflict"
    end

    test "does more transaction stuff" do
      redis.get(key).should eq nil

      _, nope, _, yep = redis.multi do |redis|
        redis.set key, "nope"
        redis.get key
        redis.set key, "yep"
        redis.get key
      end.as(Array)

      nope.should eq "nope"
      yep.should eq "yep"

      redis.get(key).should eq "yep"
      redis.del key

      expect_raises Exception do
        redis.multi do |redis|
          redis.set key, "lol"

          raise "oops"
        ensure
          redis.get(key).should eq nil
        end

        # Ensure we're still in the same state
        redis.get(key).should eq nil
        # Ensure we can still set the key
        redis.set key, "yep"
        redis.get(key).should eq "yep"
      end
    end
  end
end
