require "./cache"
require "./value_type"
require "./value"

module Redis::Graph
  # Represents a node in the graph.
  #
  # ```
  # result = graph.read_query(<<-CYPHER)
  #   MATCH (u:User)
  #   RETURN u
  # CYPHER
  #
  # result.each do |(user)|
  #   user = user.as(Redis::Graph::Node)
  #   # ...
  # end
  # ```
  struct Node
    getter id : Int64
    getter labels : Array(String)
    getter properties : Hash(String, Value)

    def self.from_redis_graph_value(type : ValueType, value, cache : Cache)
      raise ArgumentError.new("Expected Node, got: #{value.inspect}") unless type.node?

      from value.as(Array), cache
    end

    # :nodoc:
    def self.from(array : Array, cache : Cache)
      id, label_ids, raw_properties = array

      id = id.as Int64
      labels = label_ids.as(Array).map do |label_id|
        cache.label(label_id.as(Int64))
      end
      properties = Map.new(initial_capacity: array[2].as(Array).size)
      raw_properties.as(Array).each do |property|
        property_id, type, value = property.as(Array)
        key = cache.property(property_id.as(Int64))
        properties[key] = ValueType.value_for(type, value, cache)
      end
      # array[2].as(Array)[1].as(Array).each do |prop|
      #   key, value = prop.as(Array)
      #   properties[key.as(String)] = value.as(Property)
      # end

      new(id, labels, properties)
    end

    # :nodoc:
    def initialize(@id, @labels, @properties)
    end
  end
end
