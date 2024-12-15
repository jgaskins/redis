require "./value"
require "./node"
require "./relationship"
require "../to_hash"

module Redis::Graph
  enum ValueType
    UNKNOWN =  0
    NULL    =  1
    STRING  =  2
    INTEGER =  3
    BOOLEAN =  4
    DOUBLE  =  5
    ARRAY   =  6
    EDGE    =  7
    NODE    =  8
    PATH    =  9
    MAP     = 10
    POINT   = 11

    def self.value_for(type, value, cache) : Value
      type = ValueType.new(type.as(Int64).to_i)
      case type
      in .string?
        value.as(String)
      in .integer?
        value.as(Int64)
      in .null?
        nil
      in .boolean?
        value == 1
      in .double?
        value.as(String).to_f
      in .edge?
        Relationship.from value.as(Array), cache
      in .node?
        Node.from value.as(Array), cache
      in .array?
        # (Node.from?(value.as(Array)) || Relationship.from?(value.as(Array)) || value.as(Array))
        value.as(Array).map do |item|
          t, v = item.as(Array)
          value_for(t, v, cache)
        end
      in .path?
        raise ArgumentError.new("Paths not supported yet")
      in .map?
        # Map.from_redis_graph_value(type, value, cache)
        hash = Map.new(initial_capacity: value.as(Array).size // 2)
        value.as(Array).each_slice(2, reuse: true) do |(key, value)|
          t, v = value.as(Array)
          t = Redis::Graph::ValueType.new(t.as(Int).to_i)
          parsed_value = Value.from_redis_graph_value(t, v, cache)
          hash[key.as(String)] = parsed_value
        end
        hash
      in .point?
        latitude, longitude = value.as(Array)
        Point.new(latitude.as(String).to_f, longitude.as(String).to_f)
      in .unknown?
        raise ArgumentError.new("Unknown value type #{type}. Value: #{value.inspect}")
      end.as(Value)
    end
  end
end
