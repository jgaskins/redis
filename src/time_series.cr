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
      command = Array(String).new(initial_capacity: 11 + (labels.try(&.size) || 0) * 2)
      command << "ts.create" << key
      if retention
        command << "retention" << retention.total_milliseconds.to_i64.to_s
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
      command = Array(String).new(initial_capacity: 12 + (labels.try(&.size) || 0) * 2)
      command << "ts.add" << key << "*" << value.to_s
      if retention
        command << "retention" << retention.total_milliseconds.to_i64.to_s
      end
      if encoding
        command << "encoding" << encoding.to_s
      end
      if chunk_size
        command << "chunk_size" << chunk_size.to_s
      end
      if duplicate_policy
        command << "on_duplicate" << duplicate_policy.to_s
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
      command = Array(String).new(initial_capacity: 13 + (labels.try(&.size) || 0) * 2)
      command << "ts.add" << key << timestamp.to_unix_ms.to_s << value.to_s
      if retention
        command << "retention" << retention.total_milliseconds.to_i64.to_s
      end
      if encoding
        command << "encoding" << encoding.to_s
      end
      if chunk_size
        command << "chunk_size" << chunk_size.to_s
      end
      if duplicate_policy
        command << "on_duplicate" << duplicate_policy.to_s
      end

      if labels && labels.any?
        command << "labels"
        labels.each do |key, value|
          command << key << value.to_s
        end
      end

      @redis.run command
    end

    def get(key : String)
      @redis.run({"ts.get", key})
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

    def aggregation(
      aggregator : AggregationType,
      bucket_duration : Time::Span,
      buckettimestamp : BucketTimestamp? = nil,
      empty : Bool? = nil
    )
      Aggregation.new(aggregator, bucket_duration, buckettimestamp, empty)
    end

    record Aggregation,
      aggregator : AggregationType,
      bucket_duration : Time::Span,
      buckettimestamp : BucketTimestamp? = nil,
      empty : Bool? = nil

    def mrange(
      time_range : ::Range(Time, Time?),
      filter : String,
      filter_by_ts : Enumerable(Time)? = nil,
      filter_by_value : ::Range(Float64, Float64)? = nil,
      withlabels : Bool? = nil,
      selected_labels : Enumerable(String)? = nil,
      count : Int? = nil,
      aggregation : Aggregation? = nil,
      groupby : String? = nil,
      reduce : String? = nil
    )
      from = time_range.begin
      # Default to the maximum 32-bit Unix timestamp
      # TODO: Fix this before the year 2038
      to = time_range.end || Time::UNIX_EPOCH + Int32::MAX.seconds

      # TS.MRANGE fromTimestamp toTimestamp
      #   [FILTER_BY_TS Timestamp [Timestamp ...]]
      #   [FILTER_BY_VALUE min max]
      #   [WITHLABELS | SELECTED_LABELS label1 [label1 ...]]
      #   [COUNT count]
      #   [
      #     [ALIGN value]
      #     AGGREGATION AVG | FIRST | LAST | MIN | MAX | SUM | RANGE | COUNT | STD.P | STD.S | VAR.P | VAR.S | TWA
      #     bucketDuration
      #     [BUCKETTIMESTAMP bt]
      #     [EMPTY]
      #   ]
      #   FILTER l=v | l!=v | l= | l!= | l=(v1,v2,...) | l!=(v1,v2,...) [ l=v | l!=v | l= | l!= | l=(v1,v2,...) | l!=(v1,v2,...) ...]
      #   [GROUPBY label REDUCE reducer]
      command_size = 1 +                                      # TS.MRANGE
                     2 +                                      # from to
                     1 + (filter_by_ts.try(&.size) || 0) +    # FILTER_BY_TS
                     3 +                                      # FILTER_BY_VALUE
                     1 +                                      # WITHLABELS
                     1 + (selected_labels.try(&.size) || 0) + # SELECTED_LABELS
                     2 +                                      # COUNT
                     8 +                                      # ALIGN value AGGREGATION agg bucketDuration BUCKETTIMESTAMP bt EMPTY
                     2 +                                      # FILTER expr
                     4                                        # GROUPBY label REDUCE reducer
      command = Array(String).new(initial_capacity: command_size)
      command << "ts.mrange" << from.to_unix_ms.to_s << to.to_unix_ms.to_s
      if filter_by_ts
        command << "filter_by_ts"
        filter_by_ts.each do |ts|
          command << ts.to_unix_ms.to_s
        end
      end
      if filter_by_value
        command << "filter_by_value"
        command << filter_by_value.begin.to_s
        command << filter_by_value.end.to_s
      end
      command << "withlabels" if withlabels
      if selected_labels
        command << "selected_labels"
        selected_labels.each { |label| command << label }
      end
      if count
        command << "count" << count.to_s
      end
      if aggregation
        # [[ALIGN value] AGGREGATION aggregator bucketDuration [BUCKETTIMESTAMP bt] [EMPTY]]
        # TODO: Implement this
        # if alignment = aggregation.align
        #   command << "align" << alignment
        # end

        command << "aggregation" << aggregation.aggregator.to_s << aggregation.bucket_duration.total_milliseconds.to_i64.to_s
        if bucket_ts = aggregation.buckettimestamp
          command << "buckettimestamp" << bucket_ts.to_s
        end
        if aggregation.empty
          command << "empty"
        end
      end
      command << "filter" << filter
      if groupby && reduce
        command << "groupby" << groupby << "reduce" << reduce
      end

      @redis.run command
    end

    alias Labels = Hash(String, String)
    record Datapoint,
      timestamp : Time,
      value : Float64
    record Datapoints, labels : Labels, datapoints : Array(Datapoint) do
      include Enumerable(Datapoint)

      def each
        datapoints.each { |datapoint| yield datapoint }
      end

      def <<(datapoint : Datapoint)
        datapoints << datapoint
      end
    end

    struct MRangeResponse
      include Enumerable({String, Datapoints})

      alias DatapointMap = Hash(String, Datapoints)

      getter data : DatapointMap

      def initialize(response)
        response = response.as Array
        @data = DatapointMap.new(initial_capacity: response.size)
        response.each do |result|
          key, labels_list, raw_datapoints = result.as Array
          key = key.as String
          labels_list = labels_list.as Array
          raw_datapoints = raw_datapoints.as Array

          labels = Labels.new(initial_capacity: labels_list.size)
          labels_list.each do |label|
            k, v = label.as Array
            labels[k.as(String)] = v.as(String)
          end
          datapoints = @data[key] = Datapoints.new(
            labels: labels,
            datapoints: Array(Datapoint).new(initial_capacity: raw_datapoints.size),
          )

          raw_datapoints.each do |datapoint|
            datapoint = datapoint.as Array
            ts, value = datapoint
            datapoints << Datapoint.new(
              timestamp: Time.unix_ms(ts.as(Int)),
              value: value.as(String).to_f64,
            )
          end
        end
      end

      def each
        data.each { |i| yield i }
      end

      def [](key : String)
        @data[key]
      end

      def []?(key : String)
        @data[key]?
      end
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
    end

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

    enum BucketTimestamp
      HIGH
      MID
      LOW
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
