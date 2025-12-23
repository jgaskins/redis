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

  def zremrangebylex(key : String, range : Range)
    min = range.begin
    max = range.end

    if min.nil?
      min = "-"
    else
      min = "[#{min}"
    end
    if max.nil?
      max = "+"
    elsif range.excludes_end?
      max = "(#{max}"
    else
      max = "[#{max}"
    end

    zremrangebylex key, min, max
  end

  def zremrangebylex(key : String, min : String, max : String)
    unless min.starts_with?('[') || min.starts_with?('(') || min == "-" || min == "+"
      raise ArgumentError.new(%{zremrangebylex requires that `min` start with either '[' or '(' or be the special "-" or "+" values})
    end
    unless max.starts_with?('[') || max.starts_with?('(') || max == "-" || max == "+"
      raise ArgumentError.new(%{zremrangebylex requires that `max` start with either '[' or '(' or be the special "-" or "+" values})
    end

    run({"zremrangebylex", key, min, max})
  end

  def zremrangebyscore(key : String, range : Range)
    start = range.begin
    stop = range.end

    start = "-inf" if start.nil?
    stop = "+inf" if stop.nil?
    if range.excludes_end?
      stop = "(#{stop}"
    end

    start = start.to_f64 if start.is_a? Int
    stop = stop.to_f64 if stop.is_a? Int

    zremrangebyscore key, start, stop
  end

  def zremrangebyscore(key : String, min : String | Float64, max : String | Float64)
    run({"zremrangebyscore", key, min.to_s, max.to_s})
  end

  def zremrangebyrank(key : String, range : Range)
    start = range.begin
    stop = range.end

    start = "0" if start.nil?
    stop = "-1" if stop.nil?
    if range.excludes_end?
      stop = stop.to_i64 - 1
    end

    start = start.to_i64 if start.is_a? Int
    stop = stop.to_i64 if stop.is_a? Int

    zremrangebyrank(key, start, stop)
  end

  def zremrangebyrank(key : String, start : Int64 | String, stop : Int64 | String)
    run({"zremrangebyrank", key, start.to_s, stop.to_s})
  end

  def zadd(
    key : String,
    score : String | Int64 | Float64,
    value : String,
    *,
    nx = false,
    xx = false,
    gt = false,
    lt = false,
    ch = false,
    incr = false,
  )
    command = {"zadd", key}
    command += {"nx"} if nx
    command += {"xx"} if xx
    command += {"lt"} if lt
    command += {"gt"} if gt
    command += {"ch"} if ch
    command += {"incr"} if incr
    command += {score.to_s, value}
    run(command)
  end

  def zadd(
    key : String,
    *values : String,
    nx = false,
    xx = false,
    gt = false,
    lt = false,
    ch = false,
  )
    command = {"zadd", key}
    command += {"nx"} if nx
    command += {"xx"} if xx
    command += {"lt"} if lt
    command += {"gt"} if gt
    command += {"ch"} if ch
    command += values

    run command
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
    incr = false,
  )
    if values.size.odd?
      raise ArgumentError.new("There must be an even number of value arguments to represent score/value pairs")
    end

    options_size = {nx, xx, gt, lt, ch, incr}.count(&.itself)
    command = Array(String).new(initial_capacity: 2 + options_size + values.size)
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
