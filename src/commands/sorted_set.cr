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

  def zrange(
    key : String,
    start : String | Int64,
    stop : String | Int64,
    by order : ZRangeOrder? = nil,
    rev : Bool? = nil,
    limit : {String | Int, String | Int}? = nil,
    with_scores : Bool = false,
  )
    command = {"zrange", key, start.to_s, stop.to_s}
    case order
    in nil
    in .lex?
      command += {"bylex"}
    in .score?
      command += {"byscore"}
    end
    command += {"rev"} if rev
    command += {"limit"} + limit.map(&.to_s) if limit
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  enum ZRangeOrder
    LEX
    SCORE
  end

  def zrangebyscore(
    key : String,
    min : String | Int64,
    max : String | Int64,
    with_scores = false,
    limit : Enumerable(String)? = nil,
  )
    command = {"zrangebyscore", key, min.to_s, max.to_s}

    if with_scores
      command += {"withscores"}
    end
    if limit
      command += {"limit", limit[0], limit[1]}
    end

    run command
  end

  def zremrangebyscore(key : String, start : String | Int64, stop : String | Int64)
    run({"zremrangebyscore", key, start.to_s, stop.to_s})
  end

  def zremrangebyrank(key : String, start : Int64, stop : Int64)
    run({"zremrangebyrank", key, start.to_s, stop.to_s})
  end

  def zadd(key : String, score : String | Int64, value : String)
    run({"zadd", key, score.to_s, value})
  end

  def zadd(key, *values : String)
    run({"zadd", key} + values)
  end

  def zadd(
    key : String,
    values : Enumerable(String),
    *,
    nx = false,
    xx = false,
    gt = false,
    lt = false,
    ch = false,
    incr = nil,
  )
    if values.size.odd?
      raise ArgumentError.new("There must be an even number of value arguments to represent score/value pairs")
    end

    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "zadd" << key
    command << "nx" if nx
    command << "xx" if xx
    command << "lt" if lt
    command << "gt" if gt
    command << "ch" if ch
    command << "incr" if incr
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
