module Redis::Commands::SortedSet
  def zcard(key : String)
    run({"zcard", key})
  end

  def zrevrange(key : String, starting : String | Int, ending : String | Int, with_scores : Bool = false)
    command = {"zrevrange", key, starting.to_s, ending.to_s}
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  def zrange(key : String, starting : String | Int, ending : String | Int, with_scores : Bool = false)
    command = {"zrange", key, starting.to_s, ending.to_s}
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  def zrangebyscore(key : String, low : String | Float, high : String | Float, limit : Enumerable(String)? = nil)
    command = {"zrangebyscore", key, low.to_s, high.to_s}

    if limit
      command += {"limit", limit[0], limit[1]}
    end

    run command
  end

  def zremrangebyscore(key : String, low : String | Float, high : String | Float)
    run({"zremrangebyscore", key, low.to_s, high.to_s})
  end

  def zremrangebyrank(key : String, low : Int, high : Int)
    run({"zremrangebyrank", key, low.to_s, high.to_s})
  end

  def zadd(key : String, score : String | Float, value : String)
    run({"zadd", key, score.to_s, value})
  end

  def zadd(key : String, values : Enumerable)
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "zadd" << key
    values.each { |value| command << value.as(String) }

    run command
  end

  def zrem(key : String, value : String)
    run({"zrem", key, value})
  end
end
