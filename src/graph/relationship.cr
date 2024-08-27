require "./value"
require "./cache"

module Redis::Graph
  # Represents a relationship in your graph
  #
  # ```
  # result = graph.read_query(<<-CYPHER)
  #   MATCH (:User)-[membership:MEMBER_OF]->(:Team)
  #   RETURN membership
  # CYPHER
  #
  # result.each do |(membership)|
  #   membership = membership.as(Redis::Graph::Relationship)
  #   # ...
  # end
  # ```
  struct Relationship
    # The identifier of the relationship.
    #
    # NOTE: If this relationship has an `id` property, this is *not* that.
    # WARNING: Do not try to query against this. RedisGraph provides no
    # guarantees that this relationship will be at the same offset it was at
    # the last time you queried it.
    getter id : Int64

    # The type of relationship, for example with `[:MEMBER_OF]`, the `type`
    # will be `"MEMBER_OF"`.
    getter type : String

    # The node that this relationship originates from, for example with
    # `(person)-[membership]->(team)`, it will be the node id of `person`.
    #
    # WARNING: This will not match an `id` *property* of the source node.
    getter src_node : Int64

    # The node that this relationship points to, for example with
    # `(person)-[membership]->(team)`, it will be the node id of `team`.
    #
    # WARNING: This will not match an `id` *property* of the destination node.
    getter dest_node : Int64

    # The hash of properties for this relationship.
    #
    # ```
    # result = graph.write_query <<-CYPHER, now: Time.utc.to_unix_ms
    #   CREATE (person)-[membership{since: $now}]->(team)
    #   RETURN membership
    # CYPHER
    # result.first.properties # => {"since" => 2022-05-15T05:48:23 UTC}
    # ```
    getter properties : Hash(String, Value)

    # :nodoc:
    def self.from_redis_graph_value(type : ValueType, value, cache : Cache)
      raise ArgumentError.new("Expected Node, got: #{value.inspect}") unless type.edge?

      from value.as(Array), cache
    end

    # :nodoc:
    def self.from(array : Array, cache : Cache)
      unless array.size == 5
        raise ArgumentError.new("Expected a Relationship, got: #{array}")
      end

      id, type_id, src_node, dest_node, raw_properties = array
      properties = Map.new(initial_capacity: raw_properties.as(Array).size)
      raw_properties.as(Array).each do |property|
        property_id, type, value = property.as(Array)
        key = cache.property(property_id.as(Int64))
        properties[key] = ValueType.value_for(type, value, cache)
      end

      new(
        id.as(Int64),
        cache.property(type_id.as(Int)),
        src_node.as(Int64),
        dest_node.as(Int64),
        properties
      )
    end

    def initialize(@id, @type, @src_node, @dest_node, @properties)
    end
  end
end
