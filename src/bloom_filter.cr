module Redis
  struct BloomFilter
    private getter redis : Commands

    def initialize(@redis)
    end

    def reserve(
      key : String,
      error_rate : String | Float64,
      capacity : String | Int64,
      *,
      nonscaling : Bool = false,
      expansion : String | Int64 | Nil = nil,
    )
      command = {"bf.reserve", key, error_rate.to_s, capacity.to_s}
      command += {"nonscaling"} if nonscaling
      command += {"expansion", expansion} if expansion

      run command
    end

    def insert(
      key : String,
      items : Array(String),
      *,
      capacity : String | Int64 = nil,
      error error_rate : String | Float64 | Nil = nil,
      nonscaling : Bool = false,
      expansion : String | Int64 | Nil = nil,
      nocreate : Bool = false,
    )
      options_size = {
        (capacity ? 2 : 0),
        (error_rate ? 2 : 0),
        (nonscaling ? 1 : 0),
        (expansion ? 2 : 0),
        (nocreate ? 1 : 0),
      }.sum

      command = Array(String).new(initial_capacity: 3 + items.size + options_size)
      command << "bf.insert" << key
      command << "capacity" << capacity.to_s if capacity
      command << "error" << error_rate.to_s if error_rate
      command << "expansion" << expansion if expansion
      command << "nocreate" if nocreate
      command << "nonscaling" if nonscaling
      command << "items"
      command.concat items
      run command
    end

    def add(key : String, item : String)
      run({"bf.add", key, item})
    end

    def madd(key : String, items : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + items.size)
      command << "bf.madd" << key
      command.concat items
      run command
    end

    def exists(key : String, item : String)
      run({"bf.exists", key, item})
    end

    def card(key : String)
      run({"bf.card", key})
    end

    def info(key : String)
      redis.run({"bf.info", key})
    end

    private def run(command)
      @redis.run command
    end
  end

  module Commands
    def bf
      BloomFilter.new(self)
    end
  end
end
