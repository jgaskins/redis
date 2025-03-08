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
    members.each do |member|
      command << member
    end

    run command
  end

  def sdiff(first : String, second : String)
    run({"sdiff", first, second})
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

  def scard(key : String)
    run({"scard", key})
  end

  def sscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"sscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
