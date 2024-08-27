module Redis::Graph
  annotation Field
  end

  # The `Redis::Graph::Serializable::*` mixins tell `Redis::Graph::Client` how
  # to deserialize nodes and relationships as your own Crystal object types,
  # similar to [`DB::Serializable`](http://crystal-lang.github.io/crystal-db/api/0.11.0/DB/Serializable.html).
  #
  # ```
  # require "redis/graph"
  #
  # struct Person
  #   include Redis::Graph::Serializable::Node
  #
  #   getter id : UUID
  #   getter name : String
  #   getter created_at : Time
  # end
  #
  # struct Team
  #   include Redis::Graph::Serializable::Node
  #
  #   getter name : String
  # end
  #
  # struct Membership
  #   include Redis::Graph::Serializable::Relationship
  #
  #   getter since : Time
  # end
  #
  # redis = Redis::Client.new
  # redis.del "my-graph"
  #
  # # Store the graph data in the Redis key "my-graph"
  # graph = redis.graph(key: "my-graph")
  #
  # id = UUID.random
  #
  # # Create some data in our graph
  # pp graph.write_query <<-CYPHER, id: id, name: "Jamie", now: Time.utc.to_unix_ms, team_name: "My Team"
  #   CREATE (:Person{id: $id, name: $name, created_at: $now})-[:MEMBER_OF{since: $now}]->(team:Team{name: $team_name})
  # CYPHER
  #
  # # The `return` argument specifies the return types of the results in your
  # # Cypher query's `RETURN` clause
  # pp graph.read_query(<<-CYPHER, {id: id}, return: {Person, Membership, Team})
  #   MATCH (person:Person{id: $id})-[membership:MEMBER_OF]->(team:Team)
  #   RETURN person, membership, team
  # CYPHER
  # ```
  module Serializable
    annotation Property
    end

    module Node
      record Metadata, id : Int64, labels : Array(String)

      def self.from_redis_graph_value(node_type : T.class, value_type : Redis::Graph::ValueType, value, cache) : T forall T
        id, label_ids, properties = value.as(Array)

        id = id.as Int64
        labels = label_ids.as(Array).map do |label_id|
          cache.label(label_id.as(Int64))
        end
        metadata = Metadata.new(id: id, labels: labels)

        T.new(metadata, properties.as(Array), cache)
      end

      macro included
        def self.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
          ::Redis::Graph::Serializable::Node.from_redis_graph_value(self, type, value, cache)
        end

        def self.matches_redis_graph_type?(type : ::Redis::Graph::ValueType) : Bool
          type.node?
        end
      end

      def initialize(metadata : ::Redis::Graph::Serializable::Node::Metadata, properties : Array, cache)
        {% begin %}
          {% for ivar in @type.instance_vars %}
            %found{ivar.name} = false
            %value{ivar.name} = uninitialized {{ivar.type}}
          {% end %}

          properties.as(Array).each do |property|
            property_id, type, value = property.as(Array)
            key = cache.property(property_id.as(Int64))
            case key
              {% for ivar in @type.instance_vars %}
                when "{{ivar.name}}"
                  %found{ivar.name} = true
                  %value{ivar.name} = 
                  {% if ann = ivar.annotation(::Redis::Graph::Field) && ann[:converter] %}
                    {{ann[:converter]}}.from_redis_graph_value(
                     {{ivar.type}}.from_redis_graph_value(::Redis::Graph::ValueType.new(type.as(Int).to_i), value, cache)
                    )
                  {% else %}
                    {{ivar.type}}.from_redis_graph_value(::Redis::Graph::ValueType.new(type.as(Int).to_i), value, cache)
                  {% end %}
              {% end %}
            else
              unknown_redis_graph_node_property key, value
            end
          end

          {% for ivar in @type.instance_vars %}
            if %found{ivar.name}
              @{{ivar}} = %value{ivar.name}
            {% unless ivar.type.nilable? %}
            else
              raise PropertyMissing.new("Node did not contain the property `{{ivar.name}}`")
            {% end %}
            end
          {% end %}
        {% end %}
      end

      def unknown_redis_graph_node_property(key, value)
      end

      class PropertyMissing < Error
      end
    end

    # module Node
    #   record Metadata, id : Int64, labels : Array(String)

    #   macro included
    #     @[Redis::Graph::Serializable::Property(ignore: true)]
    #     getter redis_graph_node : Metadata = Metadata.new(0i64, %w[])

    #     def self.can_transform_graph_result?(value : ::Redis::Graph::Node)
    #       true
    #     end

    #     def self.can_transform_graph_result?(value)
    #       false
    #     end

    #     def self.from_graph_result(array : Array)
    #       if array.is_a?(Array(Redis::Graph::List)) && (node = Redis::Graph::Node.from?(array))
    #         new node
    #       else
    #         raise Redis::Graph::Error.new("Cannot build a {{@type.id}} from Node representation: #{array.inspect}")
    #       end
    #     end

    #     def self.from_graph_result(result : Redis::Value | Redis::Graph::Relationship)
    #       raise ArgumentError.new("Cannot create a {{@type.id}} from #{result.inspect}")
    #     end

    #     def self.from_graph_result(result : Redis::Graph::Node)
    #       new result
    #     end

    #     def initialize(node : Redis::Graph::Node)
    #       {% verbatim do %}
    #         {% begin %}
    #           {% for ivar in @type.instance_vars %}
    #             {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}

    #             {% if !ann || !ann[:ignore] %}
    #               {% if ann && ann[:converter] %}
    #                 @{{ivar.name}} = {{ann[:converter]}}.from_graph_result(node.properties[{{ivar.name.stringify}}]?)
    #               {% else %}
    #                 @{{ivar.name}} = {{ivar.type}}.from_graph_result(node.properties[{{ivar.name.stringify}}]?)
    #               {% end %}
    #             {% end %}
    #           {% end %}
    #         {% end %}
    #         @node = ::Redis::Graph::Serializable::Node::Metadata.new(
    #           id: node.id,
    #           labels: node.labels,
    #         )
    #       {% end %}
    #     end

    #     def to_redis_graph_param(io : IO)
    #       {% verbatim do %}
    #         io << '{'
    #         {% for ivar, index in @type.instance_vars %}
    #           {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}

    #           {% if !ann || !ann[:ignore] %}
    #             # We don't need to serialize nil values
    #             if @{{ivar}}
    #               io << "{{ivar}}: "
    #               @{{ivar}}.to_redis_graph_param io

    #               {% if index < @type.instance_vars.size - 1 %}
    #                 io << ", "
    #               {% end %}
    #             end
    #           {% end %}
    #         {% end %}
    #         io << '}'
    #       {% end %}
    #     end
    #   end
    # end

    # module Relationship
    #   record Metadata, id : Int64, type : String, source_node : Int64, destination_node : Int64

    #   macro included
    #     @[Redis::Graph::Serializable::Property(ignore: true)]
    #     getter relationship : Metadata

    #     def self.from_graph_result(array : Array)
    #       if node = Redis::Graph::Relationship.from?(array)
    #         new node
    #       else
    #         raise Redis::Graph::Error.new("Cannot build a {{@type.id}} from Relationship representation: #{array.inspect}")
    #       end
    #     end

    #     def self.from_graph_result(result : Redis::Value | Redis::Graph::Node)
    #       raise ArgumentError.new("Cannot create a {{@type.id}} from #{result.inspect}")
    #     end

    #     def self.from_graph_result(result : Redis::Graph::Relationship)
    #       new result
    #     end

    #     def initialize(relationship : Redis::Graph::Relationship)
    #       {% verbatim do %}
    #         {% begin %}
    #           {% for ivar in @type.instance_vars %}
    #             {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}
    #             {% if !ann || !ann[:ignore] %}
    #               @{{ivar.name}} = {{ivar.type}}.from_graph_result(relationship.properties[{{ivar.name.stringify}}]?)
    #             {% end %}
    #           {% end %}
    #         {% end %}
    #         @relationship = ::Redis::Graph::Serializable::Relationship::Metadata.new(
    #           id: relationship.id,
    #           type: relationship.type,
    #           source_node: relationship.src_node,
    #           destination_node: relationship.dest_node,
    #         )
    #       {% end %}
    #     end
    #   end
    # end
  end
end

# :nodoc:
struct Tuple
  def self.from_graph_result(result : Array)
    {% begin %}
      {
        {% for type, index in @type.type_vars %}
          {{type.instance}}.from_graph_result(result[{{index}}].as(::Redis::Graph::Value)),
        {% end %}
      }
    {% end %}
  end

  def from_graph_result(result : Array)
    {% begin %}
      {
        {% for type, index in @type.type_vars %}
          {{type.instance}}.from_graph_result(result[{{index}}]).as(::Redis::Graph::Value),
        {% end %}
      }
    {% end %}
  end

  def self.can_transform_graph_result?(result : Array)
    true
  end
end

# :nodoc:
struct Time
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a {{@type.id}} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Value)
    false
  end

  def self.can_transform_graph_result?(result : Int64 | String)
    true
  end

  def self.from_graph_result(result : String)
    Time::Format::RFC_3339.parse result
  end

  def self.from_graph_result(result : Int64)
    Time.unix_ms(result)
  end

  def to_redis_graph_param(io : IO)
    to_unix_ms.to_redis_graph_param io
  end
end

# :nodoc:
struct UUID
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : String)
    true
  end

  def self.from_graph_result(result : String)
    new result
  end

  def to_redis_graph_param(io : IO)
    io << '"'
    to_s io
    io << '"'
  end
end

# :nodoc:
class String
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : String)
    true
  end

  def self.from_graph_result(result : String)
    result
  end

  def to_redis_graph_param(io : IO)
    inspect io
  end
end

# :nodoc:
struct Nil
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : Nil)
    true
  end

  def self.from_graph_result(result : Nil)
    nil
  end

  def to_redis_graph_param(io : IO)
    io << "null"
  end
end

# :nodoc:
struct Int
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : Int)
    true
  end

  def to_redis_graph_param(io : IO)
    io << self
  end
end

{% for bits in %w[8 16 32 64] %}
  # :nodoc:
  struct Int{{bits.id}}
    def self.from_graph_result(result : Int64)
      result.to_i{{bits.id}}
    end
  end
{% end %}

# :nodoc:
struct Float64
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.from_graph_result(result : String)
    result.to_f64
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : String)
    true
  end

  def to_redis_graph_param(io : IO)
    to_s io
  end
end

# :nodoc:
struct Bool
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.from_graph_result(result : Nil)
    false
  end

  # Booleans in Redis tend to be represented by integers where 0 indicates false
  def self.from_graph_result(value : Int64)
    value != 0
  end
end

# :nodoc:
class Array
  def self.from_graph_result(result : Redis::Graph::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.from_graph_result(result : Array)
    result.map do |item|
      T.from_graph_result(item)
    end
  end

  def self.can_transform_graph_result?(result : Redis::Graph::Value)
    false
  end

  def self.can_transform_graph_result?(result : Array)
    true
  end

  def to_redis_graph_param(io : IO)
    io << %("[)
    join io, ", "
    io << %(]")
  end
end

# :nodoc:
def Union.from_graph_result(result : Redis::Graph::Value)
  {% begin %}
  if false
    raise "lolwut"
  {% for type in T %}
    elsif {{type}}.can_transform_graph_result?(result)
      {{type}}.from_graph_result(result)
  {% end %}
  else
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect} (#{result.class})")
  end
  {% end %}
end
