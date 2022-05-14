module Redis::Graph
  module Serializable
    annotation Property
    end

    module Node
      record Metadata, id : Int64, labels : Array(String)

      macro included
        @[Redis::Graph::Serializable::Property(ignore: true)]
        getter node : Metadata = Metadata.new(0i64, %w[])

        def self.from_graph_result(array : Array)
          pp array: array
          if array.is_a?(Array(Redis::Graph::List)) && (node = Redis::Graph::Node.from?(array))
            new node
          else
            raise Redis::Graph::Error.new("Cannot build a {{@type.id}} from Node representation: #{array.inspect}")
          end
        end

        def self.from_graph_result(result : Redis::Value | Redis::Graph::Relationship)
          raise ArgumentError.new("Cannot create a {{@type.id}} from #{result.inspect}")
        end

        def self.from_graph_result(result : Redis::Graph::Node)
          new result
        end

        def initialize(node : Redis::Graph::Node)
          {% verbatim do %}
            {% begin %}
              {% for ivar in @type.instance_vars %}
                {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}

                {% if !ann || !ann[:ignore] %}
                  {% if ann && ann[:converter] %}
                    @{{ivar.name}} = {{ann[:converter]}}.from_graph_result(node.properties[{{ivar.name.stringify}}]?)
                  {% else %}
                    @{{ivar.name}} = {{ivar.type}}.from_graph_result(node.properties[{{ivar.name.stringify}}]?)
                  {% end %}
                {% end %}
              {% end %}
            {% end %}
            @node = ::Redis::Graph::Serializable::Node::Metadata.new(
              id: node.id,
              labels: node.labels,
            )
          {% end %}
        end

        def to_redis_graph_param(io : IO)
          {% verbatim do %}
            io << '{'
            {% for ivar, index in @type.instance_vars %}
              {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}

              {% if !ann || !ann[:ignore] %}
                # We don't need to serialize nil values
                if @{{ivar}}
                  io << "{{ivar}}: "
                  @{{ivar}}.to_redis_graph_param io

                  {% if index < @type.instance_vars.size - 1 %}
                    io << ", "
                  {% end %}
                end
              {% end %}
            {% end %}
            io << '}'
          {% end %}
        end
      end
    end

    module Relationship
      record Metadata, id : Int64, type : String, source_node : Int64, destination_node : Int64

      macro included
        @[Redis::Graph::Serializable::Property(ignore: true)]
        getter relationship : Metadata

        def self.from_graph_result(array : Array)
          if node = Redis::Graph::Relationship.from?(array)
            new node
          else
            raise Redis::Graph::Error.new("Cannot build a {{@type.id}} from Relationship representation: #{array.inspect}")
          end
        end

        def self.from_graph_result(result : Redis::Value | Redis::Graph::Node)
          raise ArgumentError.new("Cannot create a {{@type.id}} from #{result.inspect}")
        end

        def self.from_graph_result(result : Redis::Graph::Relationship)
          new result
        end

        def initialize(relationship : Redis::Graph::Relationship)
          {% verbatim do %}
            {% begin %}
              {% for ivar in @type.instance_vars %}
                {% ann = ivar.annotation(::Redis::Graph::Serializable::Property) %}
                {% if !ann || !ann[:ignore] %}
                  @{{ivar.name}} = {{ivar.type}}.from_graph_result(relationship.properties[{{ivar.name.stringify}}]?)
                {% end %}
              {% end %}
            {% end %}
            @relationship = ::Redis::Graph::Serializable::Relationship::Metadata.new(
              id: relationship.id,
              type: relationship.type,
              source_node: relationship.src_node,
              destination_node: relationship.dest_node,
            )
          {% end %}
        end
      end
    end
  end
end

struct Tuple
  def self.from_graph_result(result : Array)
    {% begin %}
      {
        {% for type in @type.type_vars %}
          {{type.instance}}.from_graph_result(result),
        {% end %}
      }
    {% end %}
  end

  def from_graph_result(result : Array)
    {% begin %}
      {
        {% for type, index in @type.type_vars %}
          {{type.instance}}.from_graph_result(result[{{index}}]),
        {% end %}
      }
    {% end %}
  end

  def self.can_transform_graph_result?(result : Array)
    true
  end
end

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

struct UUID
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Value)
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

class String
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Value)
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

struct Nil
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def self.can_transform_graph_result?(result : Redis::Value)
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

struct Int
  def self.from_graph_result(result : Redis::Value)
    raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
  end

  def to_redis_graph_param(io : IO)
    io << self
  end
end

{% for bits in %w[8 16 32 64] %}
  struct Int{{bits.id}}
    def self.from_graph_result(result : Int64)
      result.to_i{{bits.id}}
    end
  end
{% end %}

struct Bool
  def self.from_graph_result(result : Redis::Value)
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

class Array
  def self.from_graph_result(result : Redis::Value)
    # FIXME: For some reason overloading this method for various types isn't working for deserialization, so we're doing it Ruby style
    case result
    when Array
      result.map do |item|
        T.from_graph_result(item)
      end
    else
      raise ArgumentError.new("Cannot create a #{self} from #{result.inspect}")
    end
  end

  def self.can_transform_graph_result?(result : Redis::Value)
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

def Union.from_graph_result(result : Redis::Value)
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
