record Redis::Graph::Indices(T), graph : Graph::Client(T) do
  def create(label : String, property : String)
    create label, {property}
  end

  def create(label : String, properties : Enumerable(String))
    cypher = String.build do |str|
      str << "CREATE INDEX FOR (n:" << label << ") ON ("
      properties.each_with_index 1 do |property, index|
        str << "n." << property
        if index < properties.size
          str << ", "
        end
      end

      str << ')'
    end

    begin
      graph.write_query cypher
    rescue ex : Redis::Error
      if (msg = ex.message) && (match = msg.match(/Attribute '(.*)' is already indexed/))
        raise IndexAlreadyExists.new("Index on #{label.inspect} already indexes #{match[1].inspect}")
      else
        raise ex
      end
    end
  end

  class IndexAlreadyExists < Error
  end
end
