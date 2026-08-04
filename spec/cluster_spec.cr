require "./spec_helper"
require "uuid"

require "../src/cluster"

describe Redis::Cluster do
  # We don't want to run cluster specs against non-clusters
  next unless ENV["REDIS_CLUSTER_URL"]?

  # We can provide connection details for a single node in the cluster and it
  # will discover the rest of it.
  cluster = Redis::Cluster.new

  it "reads and writes" do
    key = UUID.v7.to_s

    begin
      cluster.set key, "bar"
      cluster.get(key).should eq "bar"
    ensure
      cluster.del key
    end
  end

  # Run this a bunch of times so we can be sure that a green spec isn't a false
  # positive. It's fast enough that it shouldn't make it take long.
  500.times do |i|
    it "reads and writes a sub-hashed key (#{i})" do
      # The way to check this is to use a command that writes to multiple keys.
      # Both keys *must* exist on the same shard in order to do this atomically,
      # and if both keys are not on the same shard the server will error out.
      # Subhashed keys wrap the part they want to hash inside curly braces. For
      # example, to make sure "user:1234" and "user:1234:cart_items" are stored
      # on the same shard, you must use "{user:1234}" on the second key.
      source = UUID.random.to_s
      target = "{#{source}}:pending"

      begin
        cluster.lpush source, "value"
        cluster.lmove source, target, :left, :right
      ensure
        cluster.del source
        cluster.del target
      end
    end
  end

  it "gets keys across the whole cluster" do
    cluster.set "a", "a"
    cluster.set "b", "b"
    cluster.set "c", "c"

    keys = cluster.keys

    keys.should contain "a"
    keys.should contain "b"
    keys.should contain "c"
  end

  it "deletes all keys in all nodes" do
    cluster.set "a", "1"
    cluster.set "b", "1"
    cluster.set "c", "1"

    cluster.flushdb

    cluster.keys.should be_empty
  end

  # Example taken from https://redis.io/topics/cluster-spec#overview-of-redis-cluster-main-components
  it "hashes to the correct value" do
    cluster.slot_for("123456789").should eq 0x31C3
  end

  describe "pub/sub" do
    it "publishes only to a single shard" do
      messages = Channel(String).new
      subscribed = Channel(Nil).new
      done = Channel(Exception?).new

      spawn do
        cluster.ssubscribe "a" do |subscription|
          subscription.on_subscribe do
            subscribed.send nil
          end
          subscription.on_message do |_channel, msg|
            messages.send msg
            subscription.close
          end
        end
        done.send nil
      rescue ex
        done.send ex
      end

      select
      when subscribed.receive
      when timeout(1.second)
        raise "Timed out while waiting for sharded subscription"
      end

      # "a" goes to hash slot 15495
      # "b" goes to hash slot 3300
      # These are on opposite ends of the hash-slot spectrum, so they should go
      # to different nodes in the cluster.
      expect_raises Redis::Cluster::CrossSlot do
        cluster.ssubscribe "a", "b" { }
      end

      cluster.spublish("a", "yep").should eq 1
      cluster.spublish("b", "nope").should eq 0

      select
      when msg = messages.receive
        msg.should eq "yep"
      when timeout(1.second)
        raise "Timed out while waiting for message"
      end

      select
      when error = done.receive
        raise error if error
      when timeout(1.second)
        raise "Timed out while waiting for sharded subscription to finish"
      end
    end

    it "publishes to all shards" do
      messages = Channel({String, String}).new
      subscribed = Channel(Nil).new
      done = Channel(Exception?).new

      spawn do
        cluster.subscribe "a" do |subscription|
          subscription.on_subscribe do
            subscribed.send nil
          end
          subscription.on_message do |channel, msg|
            messages.send({channel, msg})
            subscription.close
          end
        end
        done.send nil
      rescue ex
        done.send ex
      end

      spawn do
        # This has to be in a separate fiber because it's going to a different
        # node in the cluster.
        cluster.subscribe "b" do |subscription|
          subscription.on_subscribe do
            subscribed.send nil
          end
          subscription.on_message do |channel, msg|
            messages.send({channel, msg})
            subscription.close
          end
        end
        done.send nil
      rescue ex
        done.send ex
      end

      2.times do
        select
        when subscribed.receive
        when timeout(1.second)
          raise "Timed out while waiting for subscription"
        end
      end

      cluster.publish "a", "1"
      cluster.publish "b", "2"

      received = Set({String, String}).new
      2.times do
        select
        when msg = messages.receive
          received << msg
        when timeout(100.milliseconds)
          raise "Timed out while waiting for message"
        end
      end

      received.should contain({"a", "1"})
      received.should contain({"b", "2"})

      2.times do
        select
        when error = done.receive
          raise error if error
        when timeout(1.second)
          raise "Timed out while waiting for subscription to finish"
        end
      end
    end
  end
end
