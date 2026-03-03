require "./spec_helper"
require "uuid"

require "../src/client"

module Redis
  describe Client do
    default_uri = URI.parse(ENV.fetch("REDIS_URL", "redis:///"))
    default_pool_params = URI::Params{
      "initial_pool_size"  => "2",
      "max_pool_size"      => "10",
      "checkout_timeout"   => "10",
      "retry_attempts"     => "2",
      "retry_delay"        => "0.5",
      "max_idle_pool_size" => "50",
    }
    default_keepalive_params = URI::Params{
      "keepalive"          => "true",
      "keepalive_count"    => "5",
      "keepalive_idle"     => "10",
      "keepalive_interval" => "15",
    }

    it "allows kitchen sink params" do
      uri = default_uri.dup
      uri.query_params = default_pool_params.merge(default_keepalive_params)
      redis = Client.new(uri)

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
        uri = default_uri.dup
        uri.query_params = default_pool_params
        redis = Client.new(uri)

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
        redis = Client.new(default_uri)

        redis.get "foo"

        redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(false)
        # system default settings
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(default_keepalive_count)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(7200)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(75)
      end

      it "accepts settings" do
        uri = default_uri.dup
        uri.query_params = default_keepalive_params
        redis = Client.new(uri)

        redis.get "foo"

        redis.@pool.checkout.@socket.as(TCPSocket).keepalive?.should eq(true)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_count.should eq(5)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_idle.should eq(10)
        redis.@pool.checkout.@socket.as(TCPSocket).tcp_keepalive_interval.should eq(15)
      end

      it "uses default shard keepalive settings" do
        uri = default_uri.dup
        uri.query_params = URI::Params{"keepalive" => "true"}
        redis = Client.new(uri)

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
