require "json"
require "uuid/json"

require "./redis"
require "./graph/serializable"

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
      def initialize(@redis : Runnable, @key : String)
      end

      # Write data to this graph using the given Cypher query.
      #
      # ```
      # graph.write_query "MATCH (u:User{active: true}) SET u:ActiveUser, u.active = null"
      # ```
      def write_query(cypher : String)
        Result.new(@redis.run({"GRAPH.QUERY", @key, cypher}).as(Array))
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
          result = Result.new(@redis.run({"GRAPH.QUERY", @key, build_query(cypher, params)}).as(Array))
          TypedResult({ {{T.type_vars.map(&.instance).join(", ").id}} }).new(result)
        {% end %}
      end

      def write_query(cypher : String, params : NamedTuple | Hash, return type : T.class) forall T
        {% begin %}
          result = Result.new(@redis.run({"GRAPH.QUERY", @key, build_query(cypher, params)}).as(Array))
          TypedResult({{T.instance}}).new(result)
        {% end %}
      end

      # Write data to the graph using the given cypher query, passing in the
      # given query parameters.
      def write_query(cypher : String, **params)
        Result.new(@redis.run({"GRAPH.QUERY", @key, build_query(cypher, params)}).as(Array))
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
        Result.new(@redis.run({"GRAPH.RO_QUERY", @key, cypher}).as(Array))
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
        Result.new(@redis.run({"GRAPH.RO_QUERY", @key, build_query(cypher, params)}).as(Array))
      end

      def read_query(cypher : String, return types : Tuple(*T)) forall T
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
          result = Result.new(@redis.run({"GRAPH.RO_QUERY", @key, build_query(cypher, params)}).as(Array))
          TypedResult({ {{T.type_vars.map(&.instance).join(", ").id}} }).new(result)
        {% end %}
      end

      def read_query(cypher : String, params : NamedTuple | Hash, return type : T.class) forall T
        {% begin %}
          result = Result.new(@redis.run({"GRAPH.RO_QUERY", @key, build_query(cypher, params)}).as(Array))
          TypedResult({{T.instance}}).new(result)
        {% end %}
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

      private def build_query(cypher, params)
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

      private def encode_param(value, io : IO) : Nil
        value.to_json io
      end
    end

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
      getter properties : Hash(String, Property)

      # :nodoc:
      def self.from?(array : Array(Redis::Value))
        return if array.size != 3
        return unless array[0].as?(Array).try(&.[0]?) == "id"
        return unless array[1].as?(Array).try(&.[0]?) == "labels"
        return unless array[2].as?(Array).try(&.[0]?) == "properties"

        id = array[0].as(Array)[1].as(Int64)
        labels = array[1].as(Array)[1].as(Array).map(&.as(String))
        properties = Map.new(initial_capacity: array[2].as(Array).size)
        array[2].as(Array)[1].as(Array).each do |prop|
          key, value = prop.as(Array)
          properties[key.as(String)] = value.as(Property)
        end

        new(id, labels, properties)
      end

      # :nodoc:
      def initialize(@id, @labels, @properties)
      end
    end

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
      getter properties : Hash(String, Property)

      # :nodoc:
      def self.from?(array : Array)
        return if array.size != 5
        return unless array[0].as?(Array).try(&.[0]?) == "id"
        return unless array[1].as?(Array).try(&.[0]?) == "type"
        return unless array[2].as?(Array).try(&.[0]?) == "src_node"
        return unless array[3].as?(Array).try(&.[0]?) == "dest_node"
        return unless array[4].as?(Array).try(&.[0]?) == "properties"

        id = array[0].as(Array)[1].as(Int64)
        type = array[1].as(Array)[1].as(String)
        src_node = array[2].as(Array)[1].as(Int64)
        dest_node = array[3].as(Array)[1].as(Int64)
        properties = Map.new(initial_capacity: array[4].as(Array).size)
        array[4].as(Array)[1].as(Array).each do |prop|
          key, value = prop.as(Array)
          properties[key.as(String)] = value.as(Property)
        end

        new(id, type, src_node, dest_node, properties)
      end

      # :nodoc:
      def initialize(@id, @type, @src_node, @dest_node, @properties)
      end
    end

    alias Property = ::Redis::Value
    alias ResultValue = Property | Node | Relationship
    alias List = Array(ResultValue | Array(ResultValue))
    alias Map = Hash(String, Property)
    alias Value = ResultValue | List | Map

    # Parses the results of a Cypher query
    struct Result
      include Enumerable(List)

      # The names of the fields in a query's `RETURN` clause
      getter fields : Array(String)

      # The values of the fields in a query's `RETURN` clause
      getter rows : Array(List)

      # Indicates whether the query was cached by RedisGraph
      getter? cached_execution : Bool

      # How long it took RedisGraph to execute the query on the server side.
      getter duration : Time::Span

      # How many labels were added in this query
      getter labels_added : Int64

      # How many nodes were created in this query
      getter nodes_created : Int64

      # How many relationships were created in this query
      getter relationships_created : Int64

      # How many properties were set in this query
      getter properties_set : Int64

      def self.new(raw : Array)
        case raw.size
        when 1
          fields = [] of String
          rows = [] of Redis::Value
          metadata = raw.first
        when 3
          fields, rows, metadata = raw
        else
          raise Error.new("Don't know how to process this result: #{raw.inspect}")
        end

        labels_added = 0i64
        nodes_created = 0i64
        relationships_created = 0i64
        properties_set = 0i64
        cached = false
        query_time = 0.seconds

        metadata.as(Array).each do |item|
          case item
          when /Labels added: (\d+)/
            labels_added = $1.to_i64
          when /Nodes created: (\d+)/
            nodes_created = $1.to_i64
          when /Relationships created: (\d+)/
            relationships_created = $1.to_i64
          when /Properties set: (\d+)/
            properties_set = $1.to_i64
          when /Query internal execution time: (\d+\.\d+) milliseconds/
            query_time = $1.to_f64.milliseconds
          when /Cached execution: (\d+)/
            cached = ($1 != "0")
          end
        end

        new(
          fields: fields.as(Array).map(&.as(String)),
          rows: rows.as(Array).map { |row|
            row = row.as(Array)
            list = List.new(initial_capacity: row.size)
            row.each do |item|
              case item
              in String, Int64, Nil
                list << item
              in Array
                list << (Node.from?(item) || Relationship.from?(item) || item)
              in Redis::Error
                raise item
              end.as(ResultValue | List)
            end
            list
          },
          cached_execution: !!cached,
          duration: query_time,
          labels_added: labels_added,
          nodes_created: nodes_created,
          relationships_created: relationships_created,
          properties_set: properties_set,
        )
      end

      def initialize(*, @fields, @rows, @cached_execution, @duration, @labels_added, @nodes_created, @relationships_created, @properties_set)
      end

      def each
        @rows.each { |row| yield row }
      end
    end

    struct TypedResult(T)
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
      # How many nodes were created in this query
      getter nodes_created : Int64
      # How many relationships were created in this query
      getter relationships_created : Int64
      # How many properties were set in this query
      getter properties_set : Int64

      # :nodoc:
      def self.new(result : Result)
        rows = result.map do |row|
          {% if T < Tuple %}
            T.from_graph_result(row.as(Array))
          {% else %}
            T.from_graph_result(row.as(Array).first)
          {% end %}
        end

        new(
          fields: result.fields,
          rows: rows,
          cached_execution: result.cached_execution?,
          duration: result.duration,
          labels_added: result.labels_added,
          nodes_created: result.nodes_created,
          relationships_created: result.relationships_created,
          properties_set: result.properties_set,
        )
      end

      # :nodoc:
      def initialize(*, @fields, @rows, @cached_execution, @duration, @labels_added, @nodes_created, @relationships_created, @properties_set)
      end

      # Iterate over each of the results, yielding a tuple containing instances
      # of the types in `T`.
      def each
        @rows.each { |row| yield row }
      end
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
