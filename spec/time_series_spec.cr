require "./spec_helper"
require "uuid"

require "../src/time_series"

private macro test(name)
  it {{name}} do
    key = UUID.random.to_s

    begin
      {{yield}}
    ensure
      redis.unlink key
    end
  end
end

redis = Redis::Client.new

module Redis
  describe TimeSeries do
    test "creates a TimeSeries key" do
      redis.ts.create key,
        retention: 1.day,
        encoding: :compressed,
        chunk_size: 128, # what does this mean?
        duplicate_policy: :first,
        labels: {"sensor_id" => 123}

      # I really wish these were real hashes
      redis.ts.info(key).should eq [
        "totalSamples", 0,
        "memoryUsage", 246,
        "firstTimestamp", 0,
        "lastTimestamp", 0,
        "retentionTime", 86400000,
        "chunkCount", 1,
        "chunkSize", 128,
        "chunkType", "compressed",
        "duplicatePolicy", "first",
        "labels", [
          ["sensor_id", "123"],
        ],
        "sourceKey", nil,
        "rules", [] of String,
      ]
    end

    test "gets the last datapoint for a key" do
      ts = 1.second.ago
      redis.ts.create key,
        retention: 1.week,
        duplicate_policy: :first,
        labels: {"test" => UUID.random.to_s}

      redis.ts.add key, ts, 1i64

      redis.ts.get(key).should eq [ts.to_unix_ms, "1"]
    end

    test "gets a range for keys" do
      2.times do
        redis.ts.add "mrange-test:foo=included", 1i64,
          retention: 1.minute,
          on_duplicate: :sum,
          labels: {"foo" => "included"}
        redis.ts.add "mrange-test:foo=excluded", 1i64,
          retention: 1.minute,
          on_duplicate: :sum,
          labels: {"foo" => "excluded"}
      end

      result = redis.ts.mrange 1.day.ago...,
        filter: "foo=included",
        # TODO: Aggregating mitigates flakiness here, but does not 100% fix it
        aggregation: redis.ts.aggregation(:sum, 1.minute)

      response = Redis::TimeSeries::MRangeResponse.new(result)
      response["mrange-test:foo=included"]
        .datapoints
        .first
        .value.should eq 2
    ensure
      redis.del "mrange-test:foo=included", "mrange-test:foo=excluded"
    end

    test "gets a range aligned" do
      now = Time.unix_ms Time.utc.to_unix_ms
      start = now - 15.seconds
      included = now - 1.millisecond
      labels = {"name" => "mrange-alignment-test"}
      redis.ts.add key, value: 1i64, timestamp: start, labels: labels
      redis.ts.add key, value: 2i64, timestamp: included, labels: labels
      redis.ts.add key, value: 4i64, timestamp: now, labels: labels

      result = redis.ts.mrange start..now,
        aggregation: redis.ts.aggregation(
          aggregator: :sum,
          bucket_duration: 10.seconds,
          align: :end,
        ),
        filter: "name=mrange-alignment-test"
      response = Redis::TimeSeries::MRangeResponse.new(result)
      response[key].datapoints.should eq [
        Redis::TimeSeries::Datapoint.new(
          timestamp: now - 20.seconds,
          value: 1.0,
        ),
        Redis::TimeSeries::Datapoint.new(
          timestamp: now - 10.seconds,
          value: 2.0,
        ),
        Redis::TimeSeries::Datapoint.new(
          timestamp: now,
          value: 4.0,
        ),
      ]
    end

    it "gets the index names for a given filter" do
      included_key = UUID.random.to_s
      excluded_key = UUID.random.to_s
      name_label = UUID.random.to_s
      redis.ts.create included_key, labels: { "name" => name_label, }
      redis.ts.create excluded_key, labels: { "name" => UUID.random.to_s, }

      names = redis.ts.queryindex("name=#{name_label}").as(Array)

      names.should contain included_key
      names.should_not contain excluded_key
    end
  end
end
