require "./redis"
require "./cluster" # TODO: remove this

module Redis
  # Time-series support for Redis using the
  # [RedisTimeSeries module](https://oss.redis.com/redistimeseries/).
  #
  # ```
  # require "redis"
  # require "redis/time_series"
  #
  # 
  # ```
  @[Experimental("Support for the Redis TimeSeries module is subject to change.")]
  struct TimeSeries(Runnable)
    # :nodoc:
    def initialize(@redis : Runnable)
    end

    def create(
      key : String,
      retention : Time::Span? = nil,
      encoding : Encoding? = nil,
      chunk_size : Int64? = nil,
      duplicate_policy : DuplicatePolicy? = nil,
      labels : Hash(String, String | Int32 | Int64)? = nil
    )
      command = Array(String).new(initial_capacity: 11 + (labels.try(&.size) || 0))
      command << "ts.create" << key
      if retention
        command << "retention" << retention.total_milliseconds.to_i.to_s
      end
      if encoding
        command << "encoding" << encoding.to_s
      end
      if chunk_size
        command << "chunk_size" << chunk_size.to_s
      end
      if duplicate_policy
        command << "duplicate_policy" << duplicate_policy.to_s
      end

      if labels && labels.any?
        command << "labels"
        labels.each do |key, value|
          command << key << value.to_s
        end
      end

      @redis.run command
    end

    def add(
      key : String,
      value : Float64 | Int64,
      retention : Time::Span? = nil,
      encoding : Encoding? = nil,
      chunk_size : Int64? = nil,
      on_duplicate duplicate_policy : DuplicatePolicy? = nil,
      labels : Hash(String, String | Int32 | Int64)? = nil
    )
      command = Array(String).new(initial_capacity: 12 + (labels.try(&.size) || 0))
      command << "ts.add" << key << "*" << value.to_s
      if retention
        command << "retention" << retention.total_milliseconds.to_i.to_s
      end
      if encoding
        command << "encoding" << encoding.to_s
      end
      if chunk_size
        command << "chunk_size" << chunk_size.to_s
      end
      if duplicate_policy
        command << "duplicate_policy" << duplicate_policy.to_s
      end

      if labels && labels.any?
        command << "labels"
        labels.each do |key, value|
          command << key << value.to_s
        end
      end

      @redis.run(command)
    end

    def add(
      key : String,
      timestamp : Time,
      value : Float64 | Int64,
      retention : Time::Span? = nil,
      encoding : Encoding? = nil,
      chunk_size : Int64? = nil,
      on_duplicate duplicate_policy : DuplicatePolicy? = nil,
      labels : Hash(String, String | Int32 | Int64)? = nil
    )
      command = Array(String).new(initial_capacity: 13 + (labels.try(&.size) || 0))
      command << "ts.add" << key << timestamp.to_unix_ms.to_s << value.to_s
      if retention
        command << "retention" << retention.total_milliseconds.to_i.to_s
      end
      if encoding
        command << "encoding" << encoding.to_s
      end
      if chunk_size
        command << "chunk_size" << chunk_size.to_s
      end
      if duplicate_policy
        command << "duplicate_policy" << duplicate_policy.to_s
      end

      if labels && labels.any?
        command << "labels"
        labels.each do |key, value|
          command << key << value.to_s
        end
      end

      @redis.run command
    end

    def range(key : String, time_range : ::Range(Time, Time), & : RangeOptions ->)
      command = Array(String).new(initial_capacity: 14)
      options = RangeOptions.new

      command << "ts.range" << key << time_range.begin.to_unix_ms.to_s << time_range.end.to_unix_ms.to_s

      yield options

      if agg = options.aggregation
        command << "aggregation" << agg.type.to_s << agg.time_bucket.total_milliseconds.to_i.to_s
      end

      @redis.run command
    end

    def info(key : String)
      @redis.run({"ts.info", key})
    end

    struct Range
      def initialize(raw_data : Array)
        @data = Hash(Int64, Float64).new(initial_capacity: raw_data.size)
        raw_data.each do |row|
          ts, value = row.as(Array)
          @data[ts.as(Int64)] = value.as(String).to_f64
        end
      end
    end

    class RangeOptions
      getter aggregation : Aggregation?

      def aggregation(type : AggregationType, bucket : Time::Span)
        @aggregation = Aggregation.new(type, bucket)
      end

      record Aggregation, type : AggregationType, time_bucket : Time::Span

      enum AggregationType
        AVG
        SUM
        MIN
        MAX
        RANGE
        COUNT
        FIRST
        LAST
        STD_P
        STD_S
        VAR_P
        VAR_S
      end
    end

    enum Encoding
      COMPRESSED
      UNCOMPRESSED
    end

    enum DuplicatePolicy
      # An error will occur for any out-of-order sample
      BLOCK

      # Ignore the new value
      FIRST

      # Override with the latest value
      LAST

      # Only override if the new value is lower than the existing value
      MIN

      # Only override if the new value is higher than the existing value
      MAX

      # If a previous sample exists, add the new sample to it so that the
      # updated value is equal to `previous + new`. If no previous sample
      # exists, set the updated value equal to the new value.
      SUM
    end
  end

  module Commands
    # Return a `Redis::TimeSeries` that wraps the current `Redis::Client` or
    # `Redis::Cluster`.
    def ts
      TimeSeries.new(self)
    end
  end
end
