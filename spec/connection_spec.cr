require "./spec_helper"
require "uuid"

require "../src/connection"

module Redis
  describe Connection do
    it "reconnects after being disconnected" do
      redis = Connection.new

      redis.get "foo"

      redis.@socket.close # THE INTERNET BROKE!

      redis.get "foo"
    end

    it "retries transactions" do
      redis = Connection.new
      key = UUID.random.to_s

      second_pass = false

      result = redis.multi do |txn|
        txn.get key
        txn.incr key

        unless second_pass
          redis.close
          second_pass = true
        end

        txn.get key
      end

      result.should eq [nil, 1, "1"]
      second_pass.should be_true

      redis.del key
    end

    context "with keepalive" do
      it "does nothing if nothing passed" do
        redis = Connection.new(URI.parse("redis://localhost:6379"))

        redis.get "foo"

        redis.@socket.as(TCPSocket).keepalive?.should eq(false)
        # system default settings
        redis.@socket.as(TCPSocket).tcp_keepalive_count.should eq(8)
        redis.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(7200)
        redis.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(75)
      end

      it "accepts settings" do
        redis = Connection.new(URI.parse("redis://localhost:6379?keepalive=true&keepalive_count=5&keepalive_idle=10&keepalive_interval=15"))

        redis.get "foo"

        redis.@socket.as(TCPSocket).keepalive?.should eq(true)
        redis.@socket.as(TCPSocket).tcp_keepalive_count.should eq(5)
        redis.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(10)
        redis.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(15)
      end

      it "uses default shard keepalive settings" do
        redis = Connection.new(URI.parse("redis://localhost:6379?keepalive=true"))

        redis.get "foo"

        redis.@socket.as(TCPSocket).keepalive?.should eq(true)
        redis.@socket.as(TCPSocket).tcp_keepalive_count.should eq(3)
        redis.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(60)
        redis.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(30)
      end
    end
  end
end
