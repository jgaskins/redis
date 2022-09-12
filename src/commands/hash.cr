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

  def hset(key : String, **fields : String)
    hash = ::Hash(String, String).new(initial_capacity: fields.size)
    fields.each do |key, value|
      hash[key.to_s] = value
    end

    hset key, hash
  end

  def hset(key : String, *fields : String)
    run({"hset", key} + fields)
  end

  def hset(key : String, data : ::Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + data.size)

    command << "hset" << key
    data.each do |key, value|
      command << key << value
    end

    run command
  end

  @[Deprecated("The Redis HMSET command is deprecated. Use HSET instead. This method will be removed in v1.0.0 of this shard. See https://redis.io/commands/hmset/")]
  def hmset(key : String, data : ::Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + data.size)

    command << "hmset" << key
    data.each do |key, value|
      command << key << value
    end

    run command
  end
end
