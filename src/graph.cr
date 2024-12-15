require "json"
require "uuid/json"

require "./redis"
require "./graph/value"
require "./graph/value_type"
require "./graph/serializable"
require "./graph/cache"
require "./graph/constraints"
require "./graph/indices"
require "./graph/node"
require "./graph/relationship"
require "./graph/point"

module Redis
  # [RedisGraph](https://redis.io/docs/stack/graph/) is a graph database built
  # on top of Redis that you query using the [Cypher](https://opencypher.org)
  # query language.
  #
  # If your Redis server is running RedisGraph (for example, using [Redis
  # Stack](https://redis.io/docs/stack/)), you can branch off of your existing
  # `Redis::Client` using the `Redis::Client#graph` method:
  #
  # ```
  # require "redis/graph"
  #
  # struct Person
  #   include Redis::Graph::Serializable::Node
  #
  #   getter id : Int64
  #   getter name : String
  # end
  #
  # redis = Redis::Client.new
  #
  # # Store the graph data in the Redis key "my-graph"
  # graph = redis.graph(key: "my-graph")
  #
  # # Create some data in our graph
  # graph.write_query <<-CYPHER, id: 123, name: "Jamie"
  #   CREATE (person:Person{id: $id, name: $name})
  # CYPHER
  #
  # # The `return` argument specifies the return types of the results in your
  # # Cypher query's `RETURN` clause
  # pp graph.read_query(<<-CYPHER, {id: 123}, return: {Person})
  #   MATCH (person:Person{id: $id})
  #   RETURN person
  # CYPHER
  # # => [{Person(
  # #       @id=123,
  # #       @name="Jamie",
  # #       @node=
  # #        Redis::Graph::Serializable::Node::Metadata(@id=0, @labels=["Person"]))}]
  # ```
  #
  # In addition to basic Redis property types, `Redis::Graph::Serializable` types
  # also support `Bool`, `UUID`, and `Time`.
  @[Experimental("The `Redis::Graph` API is experimental and may be subject to change.")]
  module Graph
    class Error < ::Redis::Error
    end

    struct Client(Runnable)
      getter cache : Cache

      def initialize(@redis : Runnable, @key : String, @cache = Cache.new(redis, key))
      end

      # Write data to this graph using the given Cypher query.
      #
      # ```
      # graph.write_query "MATCH (u:User{active: true}) SET u:ActiveUser, u.active = null"
      # ```
      def write_query(cypher : String)
        ResultSet(List).new(
          run(cypher),
          cache: cache,
        )
      end

      # Write data to this graph using the given Cypher query.
      #
      # ```
      # graph.write_query "MATCH (u:User{active: $active}) SET u:ActiveUser, u.active = null", active: active
      # ```
      def write_query(cypher : String, **params)
        ResultSet(List).new(
          run(cypher, params),
          cache: cache,
        )
      end

      # Write data to the graph using the given cypher query, passing in the
      # given params and returning the given types for the values in your
      # query's `RETURN` clause.
      #
      # ```
      # graph.write_query <<-CYPHER, {id: 123, now: Time.utc.to_unix_ms}, return: {Person}
      #   MATCH (person:Person{id: $id})
      #   SET person.confirmed_at = $now
      #   RETURN person
      # CYPHER
      # ```
      def write_query(cypher : String, return types : Tuple(*T)) forall T
        {% begin %}
          ResultSet({ {{T.type_vars.map(&.instance).join(", ").id}} }).new(
            run(cypher),
            cache: cache
          )
        {% end %}
      end

      def write_query(cypher : String, return types : T.class) forall T
        ResultSet(T).new(
          run(cypher),
          cache: cache
        )
      end

      # Write data to the graph using the given cypher query, passing in the
      # given params and returning the given types for the values in your
      # query's `RETURN` clause.
      #
      # ```
      # graph.write_query <<-CYPHER, {id: 123, now: Time.utc.to_unix_ms}, return: {Person}
      #   MATCH (person:Person{id: $id})
      #   SET person.confirmed_at = $now
      #   RETURN person
      # CYPHER
      # ```
      def write_query(cypher : String, params : NamedTuple | Hash, return types : Tuple(*T)) forall T
        {% begin %}
          ResultSet({ {{T.type_vars.map(&.instance).join(", ").id}} }).new(
            run(cypher, params),
            cache: cache
          )
        {% end %}
      end

      def write_query(cypher : String, params : NamedTuple | Hash, return type : T.class) forall T
        ResultSet(T).new(
          run(cypher, params),
          cache: cache,
        )
      end

      # Query the graph with the given Cypher query.
      #
      # ```
      # graph.read_query <<-CYPHER
      #   MATCH (person:Person)
      #   RETURN person
      # CYPHER
      # ```
      def read_query(cypher : String)
        ResultSet(List).new(
          run(cypher),
          cache: cache,
        )
      end

      # Query the graph with the given Cypher query, passing in the given
      # params.
      #
      # ```
      # graph.read_query <<-CYPHER, team_id: 123
      #   MATCH (team:Team{id: $team_id})
      #   MATCH (person)-[membership:MEMBER_OF]->(team)
      #   RETURN person, membership, team
      # CYPHER
      # ```
      def read_query(cypher : String, **params)
        read_query cypher, params: params, return: List
      end

      def read_query(cypher : String, return types : T) forall T
        read_query cypher, params: NamedTuple.new, return: types
      end

      # Query the graph with the given Cypher query, passing in the given
      # params, and returning the given types corresponding to the values in
      # your Cypher `RETURN` clause.
      #
      # ```
      # graph.read_query <<-CYPHER, {team_id: 123}, return: {Person}
      #   MATCH (team:Team{id: $team_id})
      #   MATCH (person)-[:MEMBER_OF]->(team)
      #   RETURN person
      # CYPHER
      # ```
      def read_query(cypher : String, params : NamedTuple | Hash, return types : Tuple(*T)) forall T
        {% begin %}
        ResultSet({ {{T.type_vars.map(&.instance).join(", ").id}} }).new(
          run(cypher, params),
          cache: cache,
        )
        {% end %}
      end

      def read_query(cypher : String, params : NamedTuple | Hash, return type : T.class) forall T
        ResultSet(T).new(
          run(cypher, params),
          cache: cache,
        )
      end

      def explain(cypher : String, params)
        explain build_query(cypher, params)
      end

      def explain(cypher : String)
        @redis.run({"GRAPH.EXPLAIN", @key, cypher, "--compact"}).as(Array)
      end

      def indices
        Indices.new self
      end

      def constraints
        Constraints.new self, @redis, @key, cache
      end

      def delete!
        @redis.run({"GRAPH.DELETE", @key})
      end

      # Execute a transaction within the given graph
      #
      # ```
      # graph.multi do |txn|
      #   txn.write_query <<-CYPHER, team_id: 123
      #     MATCH (
      #   CYPHER
      # end
      # ```
      @[Experimental("This method may be difficult to use, since it relies primarily on `Redis::Client#multi`, which is not graph-aware. It is currently intended primarily to roll back previous writes if others do not succeed when a single query is not feasible. This may be iterated on in the future.")]
      def multi
        @redis.multi do |txn|
          yield Client.new(txn.@connection, @key)
        end
      end

      def run(cypher : String)
        execute cypher
      end

      def run(cypher : String, params)
        execute build_query(cypher, params)
      end

      def execute(query : String)
        @redis.run({"GRAPH.QUERY", @key, query, "--compact"}).as(Array)
      rescue ex : Redis::Error
        if (message = ex.message) && (match = message.match(/unique constraint violation on (node|edge) of type (\w+)/))
          raise ConstraintViolation.new("property set on #{match[1]} with type #{match[2]} violates a constraint", cause: ex)
        else
          raise ex
        end
      end

      protected def build_query(cypher, params)
        String.build do |str|
          str << "CYPHER "
          params.each do |key, value|
            key.to_s str
            str << '='
            encode_param value, str
            str << ' '
          end
          str << ' ' << cypher.strip
        end
      end

      private def encode_param(array : Array, io : IO) : Nil
        io << '['
        array.each_with_index 1 do |value, index|
          encode_param value, io
          io << ',' if index < array.size
        end
        io << ']'
      end

      private def encode_param(hash : Hash, io : IO) : Nil
        io << '{'
        hash.each_with_index 1 do |(key, value), index|
          key.to_s io
          io << ':'
          encode_param value, io
          io << ',' if index < hash.size
        end
        io << '}'
      end

      private def encode_param(kv : NamedTuple, io : IO) : Nil
        io << '{'
        kv.each_with_index 1 do |key, value, index|
          key.to_s io
          io << ':'
          encode_param value, io
          io << ',' if index < kv.size
        end
        io << '}'
      end

      private def encode_param(point : Redis::Graph::Point, io : IO) : Nil
        io << "{latitude: " << point.latitude << ",longitude: " << point.longitude << "}"
      end

      private def encode_param(value, io : IO) : Nil
        value.to_json io
      end
    end

    struct ResultSet(T)
      include Enumerable(T)

      # The names of the fields in a query's `RETURN` clause
      getter fields : Array(String)
      # The values of the fields in a query's `RETURN` clause
      getter rows : Array(T)
      # Indicates whether the query was cached by RedisGraph
      getter? cached_execution : Bool
      # How long it took RedisGraph to execute the query on the server side.
      getter duration : Time::Span
      # How many labels were added in this query
      getter labels_added : Int64
      # How many labels were removed in this query
      getter labels_removed : Int64
      # How many nodes were created in this query
      getter nodes_created : Int64
      # How many nodes were deleted in this query
      getter nodes_deleted : Int64
      # How many relationships were created in this query
      getter relationships_created : Int64
      # How many relationships were deleted in this query
      getter relationships_deleted : Int64
      # How many properties were set in this query
      getter properties_set : Int64
      # How many properties were removed in this query
      getter properties_removed : Int64
      getter indices_created : Int64
      getter indices_deleted : Int64

      # :nodoc:
      def self.new(response : Array, cache : Cache)
        if response.size == 3
          columns, rows, metadata = response

          parsed_rows = rows.as(Array).map do |row|
            row = row.as(Array)
            {% if T == Array(Value) %}
              row.map do |item|
                type, value = item.as(Array)

                ValueType.value_for(type, value, cache).as(Value)
              end
            {% elsif T < Tuple %}
              {
                {% for type, index in T %}
                  begin
                    type, value = row[{{index}}].as(Array)
                    type = ValueType.new(type.as(Int).to_i)
                    {{type}}.from_redis_graph_value(type, value, cache)
                  end,
                {% end %}
              }
            {% else %}
              type, value = row[0].as(Array)
              type = ValueType.new(type.as(Int).to_i)
              T.from_redis_graph_value(type, value, cache).as(T)
            {% end %}
          end
        else
          metadata = response[0]
        end

        labels_added = 0i64
        labels_removed = 0i64
        nodes_created = 0i64
        nodes_deleted = 0i64
        relationships_created = 0i64
        relationships_deleted = 0i64
        properties_set = 0i64
        properties_removed = 0i64
        indices_created = 0i64
        indices_deleted = 0i64
        cached = false
        query_time = 0.seconds
        metadata.as(Array).each do |item|
          item = item.as(String)
          case item
          when .starts_with? "Labels added:"
            labels_added = item["Labels added:".size..].to_i64
          when .starts_with? "Labels removed:"
            labels_removed = item["Labels removed:".size..].to_i64
          when .starts_with? "Nodes created: "
            nodes_created = item["Nodes created: ".size..].to_i64
          when .starts_with? "Nodes deleted: "
            nodes_deleted = item["Nodes deleted: ".size..].to_i64
          when .starts_with? "Relationships created: "
            relationships_created = item["Relationships created: ".size..].to_i64
          when .starts_with? "Relationships deleted: "
            relationships_deleted = item["Relationships deleted: ".size..].to_i64
          when .starts_with? "Properties set: "
            properties_set = item["Properties set: ".size..].to_i64
          when .starts_with? "Properties removed: "
            properties_removed = item["Properties removed: ".size..].to_i64
          when .starts_with? "Indices created: "
            indices_created = item["Indices created: ".size..].to_i64
          when .starts_with? "Indices deleted: "
            indices_deleted = item["Indices deleted: ".size..].to_i64
          when .starts_with? "Cached execution: "
            cached = item.ends_with? "1"
          when /Query internal execution time: (\d+\.\d+) milliseconds/
            query_time = $1.to_f64.milliseconds
          # else
          #   puts "UNHANDLED METADATA: #{item}"
          end
        end

        new(
          fields: columns.try(&.as(Array).map(&.as(Array)[1].as(String))) || [] of String,
          rows: parsed_rows || [] of T,
          cached_execution: !!cached,
          duration: query_time,
          labels_added: labels_added,
          labels_removed: labels_removed,
          nodes_created: nodes_created,
          nodes_deleted: nodes_deleted,
          relationships_created: relationships_created,
          relationships_deleted: relationships_deleted,
          properties_set: properties_set,
          properties_removed: properties_removed,
          indices_created: indices_created,
          indices_deleted: indices_deleted,
        )
      end

      def initialize(
        *,
        @fields,
        @rows,
        @cached_execution,
        @duration,
        @labels_added,
        @labels_removed,
        @nodes_created,
        @nodes_deleted,
        @relationships_created,
        @relationships_deleted,
        @properties_set,
        @properties_removed,
        @indices_created,
        @indices_deleted
      )
      end

      # Iterate over each of the results, yielding a tuple containing instances
      # of the types in `T`.
      def each_row
        field_map = Hash(String, Int32).new(initial_capacity: @fields.size)
        @fields.each_with_index do |field, index|
          field_map[field] = index
        end
        @rows.each do |row|
          yield Row.new(
            data: row,
            field_map: field_map,
          )
        end
      end

      def each
        @rows.each do |row|
          yield row
        end
      end

      def size
        @rows.size
      end

      struct Row(T)
        @field_map : Hash(String, Int32)
        getter data : T

        def initialize(@data, @field_map)
        end

        def first
          self[0]
        end

        def [](key : Int)
          @data[key]
        end

        def [](key : String)
          @data[@field_map[key]]
        end
      end
    end

    class UnexpectedValue < Error
    end

    class ConstraintViolation < Error
    end
  end

  module Commands
    # Instantiate a `Redis::Graph::Client` backed by this `Redis::Client`.
    def graph(key : String)
      Graph::Client.new(self, key)
    end
  end

  class Connection
    # :nodoc:
    def encode(node : Graph::Node)
      encode [
        ["id", node.id],
        ["labels", node.labels],
        ["properties", node.properties.to_a],
      ]
    end
  end
end

{% for type in %w[String Int64] %}
def {{type.id}}.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache) : {{type.id}}
  case value
  when {{type.id}}
    value.as({{type.id}})
  when Array
    value[1].as {{type.id}}
  else
    raise ArgumentError.new("Could not parse {{type.id}} from #{value.inspect}")
  end
end
{% end %}

{% for size in %w[32 64] %}
def Float{{size.id}}.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  String.from_redis_graph_value(type, value, cache).to_f{{size.id}}
end
{% end %}

{% for size in %w[8 16 32] %}
def Int{{size.id}}.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  Int64.from_redis_graph_value(type, value, cache).to_i{{size.id}}
end
{% end %}

def Bool.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache) : Bool
  case value
  when Int
    value == 1
  when Array
    value[1] == 1
  else
    raise ArgumentError.new("Could not parse {{type.id}} from #{value.inspect}")
  end
end

def Time.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  Time.unix_ms Int64.from_redis_graph_value(type, value, cache)
end

def UUID.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  new String.from_redis_graph_value(type, value, cache)
end

def Nil.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
end

def Array.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  value.as(Array).map do |item|
    t, v = item.as Array
    t = Redis::Graph::ValueType.new(t.as(Int).to_i)
    T.from_redis_graph_value(t, v, cache).as(T)
  end
end

def Tuple.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  {% begin %}
    array = value.as(Array)
    {
      {% for type, index in T %}
        begin
          item = array[{{index}}]
          t, v = item.as Array
          t = Redis::Graph::ValueType.new(t.as(Int).to_i)
          T.from_redis_graph_value(t, v, cache).as(T)
        end,
      {% end %}
    }
  {% end %}
end

def Hash.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  hash = new(initial_capacity: value.as(Array).size // 2)
  value.as(Array).each_slice(2, reuse: true) do |(key, value)|
    t, v = value.as(Array)
    t = Redis::Graph::ValueType.new(t.as(Int).to_i)
    hash[key.as(K)] = V.from_redis_graph_value(t, v, cache)
  end
  hash
end

def Enum.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache)
  case type
  when .integer?
    new Int64.from_redis_graph_value(type, value, cache).to_i32
  when .string?
    parse String.from_redis_graph_value(type, value, cache)
  else
    raise ArgumentError.new("Could not parse #{self} from #{value.inspect} (#{type})")
  end
end

def Redis::Graph::Value.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache) : Redis::Graph::Value
  Union(*T).from_redis_graph_value type, value, cache
end

def Union.from_redis_graph_value(type : Redis::Graph::ValueType, value, cache) : self
  {% for type in T %}
    if {{type}}.matches_redis_graph_type? type
      return {{type}}.from_redis_graph_value(type, value, cache)
    end
  {% end %}

  raise Redis::Graph::UnexpectedValue.new("Expected #{value.inspect} (#{type}) to be a #{self}")
end

def Array.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.array?
end

def Tuple.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.array?
end

def Hash.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.map?
end

def Bool.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.boolean?
end

def String.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.string?
end

def Int.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.integer?
end

def Float.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.double?
end

def Nil.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.null?
end

def Redis::Graph::Node.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.node?
end

def Redis::Graph::Relationship.matches_redis_graph_type?(type : Redis::Graph::ValueType) : Bool
  type.edge?
end
