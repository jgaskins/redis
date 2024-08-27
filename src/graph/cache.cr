require "../commands"

module Redis::Graph
  class Cache
    getter redis : Commands
    getter key : String
    @label_mutex = Mutex.new
    @relationship_mutex = Mutex.new
    @property_mutex = Mutex.new

    def initialize(@redis, @key)
      @labels = [] of String
      @relationship_types = [] of String
      @properties = [] of String
    end

    def label(label_id : Int64) : String
      @label_mutex.synchronize do
        @labels.fetch label_id do
          fetch_new("labels", "label", @labels.size) do |row|
            @labels << row.as(Array)[0].as(Array)[1].as(String)
          end

          @labels[label_id]
        end
      end
    end

    def labels
      @label_mutex.synchronize do
        if @labels.empty?
          fetch_new("labels", "label", @labels.size) do |row|
            @labels << row.as(Array)[0].as(Array)[1].as(String)
          end
        end

        @labels
      end
    end

    def relationship_types
      @relationship_mutex.synchronize do
        if @relationship_types.empty?
          fetch_new("relationshipTypes", "relationshipType", @relationship_types.size) do |row|
            @relationship_types << row.as(Array)[0].as(Array)[1].as(String)
          end
        end

        @relationship_types
      end
    end

    def properties
      @property_mutex.synchronize do
        refresh_properties if @properties.empty?

        @properties
      end
    end

    def property(property_id : Int64) : String
      @property_mutex.synchronize do
        @properties.fetch property_id do
          refresh_properties

          @properties[property_id]
        end
      end
    end

    def refresh_properties : Nil
      fetch_new("propertyKeys", "propertyKey", @properties.size) do |row|
        @properties << row.as(Array)[0].as(Array)[1].as(String)
      end
    end

    def clear
      initialize @redis, @key
    end

    private def fetch_new(function, column, current_size)
      cypher = <<-CYPHER
        CALL db.#{function}() YIELD #{column}
        RETURN #{column}
        SKIP $current_size
        CYPHER
      query = @redis.graph(@key).build_query(cypher, {current_size: current_size})
      response = @redis.run({"GRAPH.QUERY", @key, query, "--compact"}).as(Array)

      response[1].as(Array).each { |row| yield row }
    end
  end
end
