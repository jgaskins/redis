require "json"
require "uuid/json"

require "./redis"
require "./graph/serializable"

module Redis
  @[Experimental("Please don't use this in production yet. I'm not even sure what I'm doing here.")]
  module Graph
    class Error < ::Redis::Error
    end

    struct Client(Runnable)
      def initialize(@redis : Runnable, @key : String)
      end

      def write_query(cypher : String)
        @redis.run({"GRAPH.QUERY", @key, cypher}).as Array
      end

      def write_query(cypher : String, params : NamedTuple | Hash, return types : Tuple(*T)) forall T
        result = Result.new(@redis.run({"GRAPH.QUERY", @key, build_query(cypher, params)}).as(Array))
        result.map do |row|
          types.from_graph_result(row.as(Array))
        end
      end

      def write_query(cypher : String, **params : Property)
        @redis.run({"GRAPH.QUERY", @key, build_query(cypher, params)}).as Array
      end

      def read_query(cypher : String)
        @redis.run({"GRAPH.RO_QUERY", @key, cypher}).as Array
      end

      def read_query(cypher : String, params : NamedTuple | Hash, return types : Tuple(*T)) forall T
        result = Result.new(@redis.run({"GRAPH.RO_QUERY", @key, build_query(cypher, params)}).as(Array))
        result.map do |row|
          types.from_graph_result(row.as(Array))
        end
      end

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

      private def encode_param(value, io : IO) : Nil
        value.to_json io
      end
    end

    struct Node
      getter id : Int64
      getter labels : Array(String)
      getter properties : Hash(String, Property)

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

      def initialize(@id, @labels, @properties)
      end
    end

    struct Relationship
      getter id : Int64
      getter type : String
      getter src_node : Int64
      getter dest_node : Int64
      getter properties : Hash(String, Property)

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

      def initialize(@id, @type, @src_node, @dest_node, @properties)
      end
    end

    alias Property = ::Redis::Value
    alias ResultValue = Property | Node | Relationship
    alias List = Array(ResultValue | Array(ResultValue))
    alias Map = Hash(String, Property)

    struct Result
      include Enumerable(List)

      getter columns : Array(String)
      getter rows : Array(List)
      getter? cached_execution : Bool
      getter duration : Time::Span
      getter labels_added : Int64
      getter nodes_created : Int64
      getter properties_set : Int64

      def self.new(raw : Array)
        columns, rows, metadata = raw
        labels_added = 0i64
        nodes_created = 0i64
        properties_set = 0i64
        cached = false
        query_time = 0.seconds

        metadata.as(Array).each do |item|
          case item
          when /Labels added: (\d+)/
            labels_added = $1.to_i64
          when /Nodes created: (\d+)/
            nodes_created = $1.to_i64
          when /Properties set: (\d+)/
            properties_set = $1.to_i64
          when /Query internal execution time: (\d+\.\d+) milliseconds/
            query_time = $1.to_f64.milliseconds
          when /Cached execution: (\d+)/
            cached = $1 != "0"
          end
        end

        new(
          columns: columns.as(Array).map(&.as(String)),
          rows: rows.as(Array).map { |row|
            row = row.as(Array)
            list = List.new(initial_capacity: row.size)
            row.each do |item|
              case item
              in String, Int64, Nil
                list << item
              in Array
                list << (Node.from?(item) || Relationship.from?(item) || item)
              end.as(ResultValue | List)
            end
            list
          },
          cached_execution: !!(cached =~ /1/),
          duration: query_time,
          labels_added: labels_added,
          nodes_created: nodes_created,
          properties_set: properties_set,
        )
      end

      def initialize(*, @columns, @rows, @cached_execution, @duration, @labels_added, @nodes_created, @properties_set)
      end

      def each
        @rows.each { |row| yield row }
      end
    end
  end

  class Client
    def graph(key : String)
      Graph::Client.new(self, key)
    end
  end

  class Connection
    def encode(node : Graph::Node)
      encode [
        ["id", node.id],
        ["labels", node.labels],
        ["properties", node.properties.to_a],
      ]
    end
  end
end
