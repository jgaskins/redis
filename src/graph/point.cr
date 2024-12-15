module Redis::Graph
  struct Point
    include JSON::Serializable

    getter latitude : Float64
    getter longitude : Float64

    def self.matches_redis_graph_type?(type : ::Redis::Graph::ValueType) : Bool
      type.array?
    end

    def self.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
      raw_lat, raw_long = value.as(Array)
      lat = Float64.from_redis_graph_value(:double, raw_lat, cache)
      long = Float64.from_redis_graph_value(:double, raw_long, cache)
      new lat, long
    end

    def initialize(@latitude, @longitude)
    end
  end
end
