module Redis::Commands::Set
  def sadd(key : String, *values : String)
    run({"sadd", key} + values)
  end

  def sadd(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "sadd" << key
    command.concat values

    run command
  end

  def sismember(key : String, value : String)
    run({"sismember", key, value})
  end

  def smembers(key : String)
    run({"smembers", key})
  end

  def srem(key : String, *values : String)
    run({"srem", key} + values)
  end

  def srem(key : String, members : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + members.size)
    command << "srem" << key
    command.concat members

    run command
  end

  def sdiff(first : String, *others : String)
    run({"sdiff", first} + others)
  end

  def sdiffstore(destination : String, *others : String)
    run({"sdiffstore", destination} + others)
  end

  def sdiffstore(destination : String, others : Enumerable(String))
    command = Array(String).new(2 + others.size)
    command << "sdiffstore" << destination
    command.concat others

    run command
  end

  def sinter(first : String, *others : String)
    run({"sinter", first} + others)
  end

  def sinter(keys : Enumerable(String))
    command = Array(String).new(initial_capacity: 1 + keys.size)
    command << "sinter"
    command.concat keys

    run command
  end

  def sinterstore(destination : String, *others : String)
    run({"sinterstore", destination} + others)
  end

  def sinterstore(destination : String, others : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + keys.size)
    command << "sinterstore" << destination
    command.concat others

    run command
  end

  def sunion(*keys : String)
    run({"sunion"} + keys)
  end

  def sunion(keys : Enumerable(String))
    command = Array(String).new(1 + keys.size)
    command << "sunion"
    command.concat keys

    run command
  end

  def sunionstore(destination : String, *others : String)
    run({"sunionstore", destination} + others)
  end

  def sunionstore(destination : String, others : Enumerable(String))
    command = Array(String).new(2 + keys.size)
    command << "sunionstore" << destination
    command.concat others

    run command
  end

  def scard(key : String)
    run({"scard", key})
  end

  def smove(source : String, destination : String, member : String)
    run({"smove", source, destination, member})
  end

  def sscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"sscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
