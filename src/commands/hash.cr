module Redis::Commands::Hash
  def hdel(key : String, *fields : String)
    run({"hdel"} + fields)
  end

  def hdel(key : String, fields : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + fields.size)
    command << "hdel" << key
    fields.each { |key| command << key }

    run command
  end

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

  def hsetnx(key : String, **fields : String)
    command = Array(String).new(initial_capacity: 2 + 2 * fields.size)
    command << "hsetnx" << key
    fields.each do |key, value|
      command << key.to_s << value
    end

    run command
  end

  def hsetnx(key : String, fields : Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + 2 * fields.size)
    command << "hsetnx" << key
    fields.each { |key, value| command << key << value }

    run command
  end

  def hsetnx(key : String, fields : Enumerable(String))
    if fields.size.even?
      raise ArgumentError.new("fields must have an even number of elements")
    end

    command = Array(String).new(initial_capacity: 2 + fields.size)
    command << "hsetnx" << key
    fields.each { |value| command << key }

    run command
  end

  def hincrby(key : String, field : String, increment : Int | String)
    run({"hincrby", key, field, increment.to_s})
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
