module Redis::Commands::SortedSet
  def zcard(key : String)
    run({"zcard", key})
  end

  def zrevrange(key : String, starting : String | Int64, ending : String | Int64, with_scores : Bool = false)
    command = {"zrevrange", key, starting.to_s, ending.to_s}
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  def zrange(key : String, starting : String | Int64, ending : String | Int64, with_scores : Bool = false)
    command = {"zrange", key, starting.to_s, ending.to_s}
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  def zrangebyscore(key : String, low : String | Int64, high : String | Int64, limit : Enumerable(String)? = nil)
    command = {"zrangebyscore", key, low.to_s, high.to_s}

    if limit
      command += {"limit", limit[0], limit[1]}
    end

    run command
  end

  def zremrangebyscore(key : String, low : String | Int64, high : String | Int64)
    run({"zremrangebyscore", key, low.to_s, high.to_s})
  end

  def zremrangebyrank(key : String, low : Int64, high : Int64)
    run({"zremrangebyrank", key, low.to_s, high.to_s})
  end

  def zadd(key : String, score : String | Int64, value : String)
    run({"zadd", key, score.to_s, value})
  end

  def zadd(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "zadd" << key
    values.each { |value| command << value }

    run command
  end

  def zrem(key : String, value : String)
    run({"zrem", key, value})
  end

  def zrem(key : String, *values : String)
    run({"zrem", key} + values)
  end

  def zrem(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "zrem" << key
    values.each { |value| command << value.as(String) }

    run command
  end

  def zcount(key : String, min : String, max : String)
    run({"zcount", key, min, max})
  end

  def zscore(key : String, value : String)
    run({"zscore", key, value})
  end

  def zscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"zscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
