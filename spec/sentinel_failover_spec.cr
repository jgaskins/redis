require "./spec_helper"

require "../src/sentinel_client"

# Failover integration test — kept in a separate file so it can be run
# independently of the main sentinel spec suite.
#
# Requires a running sentinel cluster:
#   cd examples/sentinel && ./sentinel.sh up
#
# Run with:
#   REDIS_SENTINEL_URLS="redis://172.28.0.20:26379,..." crystal spec spec/sentinel_failover_spec.cr
#
# Or via Docker (works on macOS too):
#   cd examples/sentinel && ./sentinel.sh test-failover

module Redis
  describe SentinelClient do
    next unless ENV["REDIS_SENTINEL_URLS"]?

    sentinel_uris = ENV["REDIS_SENTINEL_URLS"].split(',').map { |u| URI.parse(u.strip) }
    master_name = ENV.fetch("REDIS_SENTINEL_MASTER", "mymaster")

    redis = SentinelClient.new(
      sentinels: sentinel_uris,
      master_name: master_name,
    )

    it "follows the master after a forced failover" do
      initial_host = redis.master_uri.host.not_nil!
      initial_port = redis.master_uri.port.not_nil!

      begin
        redis.set("sentinel:prefailover", "ok").should eq "OK"

        trigger_conn = Connection.new(sentinel_uris.first)
        begin
          trigger_conn.run({"sentinel", "failover", master_name})
        ensure
          trigger_conn.close rescue nil
        end

        deadline = Time.utc + 30.seconds
        loop do
          break if redis.master_uri.host != initial_host || redis.master_uri.port != initial_port
          raise "Timeout waiting for sentinel failover" if Time.utc > deadline
          sleep 500.milliseconds
        end

        new_host = redis.master_uri.host.not_nil!
        new_port = redis.master_uri.port.not_nil!
        "#{new_host}:#{new_port}".should_not eq "#{initial_host}:#{initial_port}"

        redis.set("sentinel:postfailover", "ok").should eq "OK"
        redis.get("sentinel:postfailover").should eq "ok"
      ensure
        redis.del "sentinel:prefailover", "sentinel:postfailover"
      end
    end
  end
end
