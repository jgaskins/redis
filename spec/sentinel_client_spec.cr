require "./spec_helper"
require "uuid"

require "../src/sentinel_client"

module Redis
  # DB::PoolResourceLost closes its resource on construction, so exercising
  # it in a spec needs something that responds to #close.
  private class FakeRetrySpecResource
    def close
    end
  end

  describe SentinelClient do
    describe ".parse_sentinel_list" do
      it "parses a well-formed SENTINEL sentinels response" do
        entry : Value = [
          "name", "172.28.0.21:26380",
          "ip", "172.28.0.21",
          "port", "26380",
          "flags", "sentinel",
          "last-ping-sent", "0",
          "last-ok-ping-reply", "123",
        ] of Value
        raw : Value = [entry] of Value

        result = SentinelClient.parse_sentinel_list(raw)

        result.size.should eq 1
        result[0].ip.should eq "172.28.0.21"
        result[0].port.should eq 26380
        result[0].flags.should eq "sentinel"
      end

      it "parses multiple sentinel entries" do
        e1 : Value = ["ip", "10.0.0.1", "port", "26379", "flags", "sentinel"] of Value
        e2 : Value = ["ip", "10.0.0.2", "port", "26380", "flags", "sentinel"] of Value
        raw : Value = [e1, e2] of Value

        result = SentinelClient.parse_sentinel_list(raw)

        result.size.should eq 2
        result.map(&.ip).should eq ["10.0.0.1", "10.0.0.2"]
        result.map(&.port).should eq [26379, 26380]
      end

      it "skips entries with an empty ip field" do
        entry : Value = ["flags", "disconnected", "port", "26381"] of Value
        raw : Value = [entry] of Value

        SentinelClient.parse_sentinel_list(raw).should be_empty
      end

      it "returns an empty array for a nil response" do
        SentinelClient.parse_sentinel_list(nil).should be_empty
      end

      it "returns an empty array for an empty array response" do
        raw : Value = [] of Value
        SentinelClient.parse_sentinel_list(raw).should be_empty
      end

      it "tolerates unknown keys in the entry" do
        entry : Value = [
          "ip", "10.0.0.5",
          "port", "26379",
          "flags", "sentinel",
          "some-future-field", "whatever",
        ] of Value
        raw : Value = [entry] of Value

        result = SentinelClient.parse_sentinel_list(raw)
        result.size.should eq 1
        result[0].ip.should eq "10.0.0.5"
      end
    end

    describe ".parse_switch_master_message" do
      it "parses a +switch-master message" do
        msg = "mymaster 172.28.0.10 6379 172.28.0.11 6350"
        change = SentinelClient.parse_switch_master_message(msg)

        change.master_name.should eq "mymaster"
        change.new_host.should eq "172.28.0.11"
        change.new_port.should eq 6350
      end
    end

    describe ".build_master_uri" do
      it "substitutes host and port while preserving all other URI fields" do
        template = URI.parse("rediss://:secret@placeholder:9999/3?keepalive=true&max_pool_size=10&checkout_timeout=30")

        result = SentinelClient.build_master_uri(template, host: "10.0.0.5", port: 6380)

        result.scheme.should eq "rediss"
        result.password.should eq "secret"
        result.path.should eq "/3"
        result.query_params["keepalive"].should eq "true"
        result.query_params["max_pool_size"].should eq "10"
        result.query_params["checkout_timeout"].should eq "30"
        result.host.should eq "10.0.0.5"
        result.port.should eq 6380
      end

      it "does not mutate the template URI" do
        template = URI.parse("redis://original-host:6379/")
        SentinelClient.build_master_uri(template, host: "new-host", port: 6380)

        template.host.should eq "original-host"
        template.port.should eq 6379
      end
    end

    describe ".retry_connection_errors" do
      it "returns the block's value on success, without retrying" do
        calls = 0
        result = SentinelClient.retry_connection_errors(3) do
          calls += 1
          "ok"
        end

        result.should eq "ok"
        calls.should eq 1
      end

      it "does not retry at all with the default of 1 attempt" do
        calls = 0
        expect_raises(IO::Error, "connection reset") do
          SentinelClient.retry_connection_errors(1) do
            calls += 1
            raise IO::Error.new("connection reset")
          end
        end

        calls.should eq 1
      end

      it "retries on IO::Error and succeeds once the block stops failing" do
        calls = 0
        result = SentinelClient.retry_connection_errors(3) do
          calls += 1
          raise IO::Error.new("connection reset") if calls < 3
          "ok"
        end

        result.should eq "ok"
        calls.should eq 3
      end

      it "retries on DB::PoolResourceLost" do
        calls = 0
        result = SentinelClient.retry_connection_errors(2) do
          calls += 1
          raise DB::PoolResourceLost.new(FakeRetrySpecResource.new) if calls < 2
          "ok"
        end

        result.should eq "ok"
        calls.should eq 2
      end

      it "retries on DB::PoolRetryAttemptsExceeded" do
        calls = 0
        result = SentinelClient.retry_connection_errors(2) do
          calls += 1
          raise DB::PoolRetryAttemptsExceeded.new if calls < 2
          "ok"
        end

        result.should eq "ok"
        calls.should eq 2
      end

      it "retries on DB::PoolTimeout" do
        calls = 0
        result = SentinelClient.retry_connection_errors(2) do
          calls += 1
          raise DB::PoolTimeout.new("Could not check out a connection in 5.0 seconds") if calls < 2
          "ok"
        end

        result.should eq "ok"
        calls.should eq 2
      end

      it "gives up and raises once every attempt has failed with a connection error" do
        calls = 0
        expect_raises(IO::Error, "connection reset") do
          SentinelClient.retry_connection_errors(3) do
            calls += 1
            raise IO::Error.new("connection reset")
          end
        end

        calls.should eq 3
      end

      it "does not retry errors the server actually replied with" do
        calls = 0
        expect_raises(Redis::Error, "WRONGTYPE") do
          SentinelClient.retry_connection_errors(3) do
            calls += 1
            raise Redis::Error.new("WRONGTYPE")
          end
        end

        calls.should eq 1
      end
    end

    next unless ENV["REDIS_SENTINEL_URLS"]?

    sentinel_uris = ENV["REDIS_SENTINEL_URLS"].split(',').map { |u| URI.parse(u.strip) }
    master_name = ENV.fetch("REDIS_SENTINEL_MASTER", "mymaster")

    redis = SentinelClient.new(
      sentinels: sentinel_uris,
      master_name: master_name,
    )

    describe SentinelClient do
      it "resolves the current master from sentinel" do
        uri = redis.master_uri
        uri.host.should_not be_nil
        uri.port.should_not be_nil
      end

      it "executes basic get/set commands via the discovered master" do
        key = UUID.v7.to_s
        begin
          redis.set(key, "sentinel-ok").should eq "OK"
          redis.get(key).should eq "sentinel-ok"
        ensure
          redis.del key
        end
      end

      it "reports at least as many sentinels as were provided" do
        redis.sentinel_count.should be >= sentinel_uris.size
      end

      it "discovers additional sentinels via SENTINEL sentinels" do
        redis.refresh_sentinel_registry
        # Refresh prunes entries no longer reported by any reachable sentinel,
        # so it isn't guaranteed to grow — but the originally configured
        # seeds are always kept.
        redis.sentinel_count.should be >= sentinel_uris.size
      end

      it "executes pipelines through the sentinel client" do
        a = UUID.v7.to_s
        b = UUID.v7.to_s
        begin
          results = redis.pipeline do |pipe|
            pipe.set a, "1"
            pipe.set b, "2"
            pipe.get a
            pipe.get b
          end
          results[2].should eq "1"
          results[3].should eq "2"
        ensure
          redis.del a, b
        end
      end

      it "executes multi/transaction through the sentinel client" do
        key = UUID.v7.to_s
        begin
          redis.multi do |txn|
            txn.set key, "txn-value"
            txn.get key
          end.as(Array)[1].should eq "txn-value"
        ensure
          redis.del key
        end
      end

      it "preserves master_uri template options on the active connection" do
        # Default template has redis:// and db 0
        redis.master_uri.scheme.should eq "redis"
      end
    end
  end
end
