require "./redis"

module Redis
  @[Experimental("Please don't use this in production yet. I'm not even sure what I'm doing here.")]
  module Graph
    class Error < ::Redis::Error
    end

    struct Client
      def initialize(@redis : ::Redis::Client, @key : String)
      end

      def write_query(cypher : String)
        @redis.run({"GRAPH.QUERY", @key, cypher}).as Array
      end

      def write_query(cypher : String, **params : Property)
        cypher = String.build do |str|
          str << "CYPHER "
          params.each do |key, value|
            str << key << '='
            encode_param value, str
          end
          str << ' ' << cypher.strip
        end

        @redis.run({"GRAPH.QUERY", @key, cypher}).as Array
      end

      def read_query(cypher : String)
        @redis.run({"GRAPH.RO_QUERY", @key, cypher}).as Array
      end

      private def encode_param(value : String | Int | Float, io : IO) : Nil
        value.inspect io
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

    alias Property = ::Redis::Value
    alias ResultValue = Property | Node
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
                list << Node.from?(item) || item
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
