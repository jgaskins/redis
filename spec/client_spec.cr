require "./spec_helper"
require "uuid"

require "../src/client"

module Redis
  describe Client do
    it "allows kitchen sink params" do
      pool_params = "?initial_pool_size=2&max_pool_size=10&checkout_timeout=10&retry_attempts=2&retry_delay=0.5&max_idle_pool_size=50"
      keepalive_params = "&keepalive=true&keepalive_count=5&keepalive_idle=10&keepalive_interval=15"
      redis = Client.new(URI.parse("redis://localhost:6379/0#{pool_params}#{keepalive_params}"))

      redis.get "foo"

      redis.@pool.@initial_pool_size.should eq(2)
      redis.@pool.@max_pool_size.should eq(10)
      redis.@pool.@checkout_timeout.should eq(10)
      redis.@pool.@retry_attempts.should eq(2)
      redis.@pool.@retry_delay.should eq(0.5)
      redis.@pool.@max_idle_pool_size.should eq(50)

      redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(true)
      redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(5)
      redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(10)
      redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(15)
    end

    context "with pool params" do
      it "uses standard pool args" do
        redis = Client.new

        redis.get "foo"

        redis.@pool.@initial_pool_size.should eq(1)
        redis.@pool.@max_pool_size.should eq(0)
        redis.@pool.@checkout_timeout.should eq(5.0)
        redis.@pool.@retry_attempts.should eq(1)
        redis.@pool.@retry_delay.should eq(0.2)
        redis.@pool.@max_idle_pool_size.should eq(25)
      end

      it "allowing standard pool args" do
        pool_params = "?initial_pool_size=2&max_pool_size=10&checkout_timeout=10&retry_attempts=2&retry_delay=0.5&max_idle_pool_size=50"
        redis = Client.new(URI.parse("redis://localhost:6379/0#{pool_params}"))

        redis.get "foo"

        redis.@pool.@initial_pool_size.should eq(2)
        redis.@pool.@max_pool_size.should eq(10)
        redis.@pool.@checkout_timeout.should eq(10)
        redis.@pool.@retry_attempts.should eq(2)
        redis.@pool.@retry_delay.should eq(0.5)
        redis.@pool.@max_idle_pool_size.should eq(50)
      end
    end

    context "with keepalive" do
      it "does nothing if nothing passed" do
        redis = Client.new(URI.parse("redis://localhost:6379"))

        redis.get "foo"

        redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(false)
        # system default settings
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(8)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(7200)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(75)
      end

      it "accepts settings" do
        redis = Client.new(URI.parse("redis://localhost:6379?keepalive=true&keepalive_count=5&keepalive_idle=10&keepalive_interval=15"))

        redis.get "foo"

        redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(true)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(5)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(10)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(15)
      end

      it "uses default shard keepalive settings" do
        redis = Client.new(URI.parse("redis://localhost:6379?keepalive=true"))

        redis.get "foo"

        redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(true)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(3)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(60)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(30)
      end
    end

    it "retries on failure" do
      redis = Client.new
      key = UUID.v7.to_s

      redis.get(key).should eq nil

      redis.@pool.checkout { |c| c.@socket.close } # THE INTERNET BROKE

      redis.get(key).should eq nil
    end
  end
end
