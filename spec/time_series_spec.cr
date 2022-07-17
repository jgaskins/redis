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
  end
end
