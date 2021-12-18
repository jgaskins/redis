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
    cluster.set "foo", "bar"
    cluster.get("foo").should eq "bar"
  ensure
    cluster.del "foo"
  end

  # Run this a bunch of times so we can be sure that a green spec isn't a false
  # positive. It's fast enough that it shouldn't make it take long.
  500.times do
    it "reads and writes a sub-hashed key" do
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
        cluster.rpoplpush source, target
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
end
