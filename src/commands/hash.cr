module Redis::Commands::Hash
  def hget(key : String, field : String)
    run({"hget", key, field})
  end

  def hgetall(key : String)
    run({"hgetall", key})
  end

  def hmget(key : String, *fields : String)
    run({"hmget", key} + fields)
  end

  def hset(key : String, field : String, value : String)
    run({"hset", key, field, value})
  end

  def hmset(key : String, data : Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + data.size)

    command << "hmset" << key
    data.each do |key, value|
      command << key << value
    end

    run command
  end
end
