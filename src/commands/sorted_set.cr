module Redis::Commands::SortedSet
  def zcard(key : String)
    run({"zcard", key})
  end

  def zrank(key : String, member : String, *, withscore : Bool = false)
    command = {"zrank", key, member}
    command += {"withscore"} if withscore

    run command
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

  def zrangestore(
    destination : String,
    source : String,
    min : String,
    max : String,
    by order : ZRangeOrder? = nil,
    rev : Bool? = nil,
    limit : {String | Int, String | Int}? = nil,
  )
    command = {"zrangestore", destination, source, min, max}
    case order
    in nil
    in .lex?
      command += {"bylex"}
    in .score?
      command += {"byscore"}
    end
    command += {"rev"} if rev
    command += {"limit"} + limit.map(&.to_s) if limit

    run command
  end

  enum ZRangeOrder
    LEX
    SCORE
  end

  def zrangebyscore(
    key : String,
    min : String | Float64,
    max : String | Float64,
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
    command.concat values

    run command
  end

  def zcount(key : String, min : String, max : String)
    run({"zcount", key, min, max})
  end

  def zrandmember(
    key : String,
    *,
    count : String | Int | Nil = nil,
    withscores : Bool = false,
  )
    command = {"zrandmember", key}
    command += {count.to_s} if count
    command += {"withscores"} if withscores

    run command
  end

  def zincrby(key : String, increment : String | Int, member : String)
    run({"zincrby", key, increment.to_s, member})
  end

  def zscore(key : String, value : String)
    run({"zscore", key, value})
  end

  def zdiff(*keys : String, withscores : Bool = false)
    command = {"zdiff", keys.size.to_s} + keys
    command += {"withscores"} if withscores

    run command
  end

  def zdiff(keys : Enumerable(String), withscores : Bool = false)
    command = Array(String).new(3 + keys.size)
    command << "zdiff" << keys.size.to_s
    command.concat keys
    command << "withscores" if withscores

    run command
  end

  def zdiffstore(destination : String, *keys : String)
    run({"zdiffstore", destination, keys.size.to_s} + keys)
  end

  def zdiffstore(destination : String, keys : Enumerable(String))
    command = Array(String).new(3 + keys.size)
    command << "zdiffstore" << destination << keys.size.to_s
    command.concat keys

    run command
  end

  def zinter(*keys : String)
    command = {"zinter", keys.size.to_s} + keys
    run command
  end

  def zinter(
    keys : Enumerable(String),
    *,
    weights : Enumerable(String)? = nil,
    aggregate : ZINTERAggregate? = nil,
    withscores : Bool = false,
  )
    command = Array(String).new(6 + keys.size + (weights.try(&.size) || 0))
    command << "zinter" << keys.size.to_s
    command.concat keys
    if weights
      command << "weights"
      command.concat weights
    end
    command << "aggregate" << aggregate.to_s if aggregate
    command << "withscores" if withscores

    run command
  end

  def zinterstore(destination : String, *keys : String)
    run({"zinterstore", destination, keys.size.to_s} + keys)
  end

  def zinterstore(
    destination : String,
    keys : Enumerable(String),
    *,
    weights : Enumerable(String)? = nil,
    aggregate : ZINTERAggregate? = nil,
  )
    command = Array(String).new(6 + keys.size + (weights.try(&.size) || 0))
    command << "zinterstore" << destination << keys.size.to_s
    command.concat keys
    if weights
      command << "weights"
      command.concat weights
    end
    command << "aggregate" << aggregate.to_s if aggregate

    run command
  end

  def zintercard(*keys : String)
    run({"zintercard", keys.size.to_s} + keys)
  end

  def zintercard(keys : Enumerable(String), limit : String | Int | Nil = nil)
    command = Array(String).new(4 + keys.size)
    command << "zintercard" << keys.size.to_s
    command.concat keys
    command << "limit" << limit.to_s if limit

    run command
  end

  enum ZINTERAggregate
    SUM
    MIN
    MAX
    COUNT
  end

  def zlexcount(key : String, min : String, max : String)
    run({"zlexcount", key, min, max})
  end

  def zmscore(key : String, *members : String)
    run({"zmscore", key} + members)
  end

  def zmscore(key : String, members : Enumerable(String))
    command = Array(String).new(2 + members.size)
    command << "zmscore" << key
    command.concat members

    run command
  end

  def zpopmax(key : String, *, count : String | Int | Nil = nil)
    command = {"zpopmax", key}
    command += {count.to_s} if count

    run command
  end

  def zpopmin(key : String, *, count : String | Int | Nil = nil)
    command = {"zpopmin", key}
    command += {count.to_s} if count

    run command
  end

  def bzmpop(timeout : Time::Span | String | Int, keys : Enumerable(String), side : ZMPOPSide, count : String | Int | Nil = nil)
    if timeout.is_a? Time::Span
      timeout = timeout.total_seconds.to_i64
    end
    command = Array(String).new(5 + keys.size)
    command << "bzmpop" << timeout.to_s << keys.size.to_s
    command.concat keys
    command << side.to_s
    command << "count" << count.to_s if count

    run command
  end

  def zmpop(keys : Enumerable(String), side : ZMPOPSide, count : String | Int | Nil = nil)
    command = Array(String).new(5 + keys.size)
    command << "zmpop" << keys.size.to_s
    command.concat keys
    command << side.to_s
    command << "count" << count.to_s if count

    run command
  end

  enum ZMPOPSide
    MIN
    MAX
  end

  def bzpopmax(keys : Enumerable(String), timeout : Time::Span | String | Int)
    if timeout.is_a? Time::Span
      timeout = timeout.total_seconds.to_i64
    end
    command = Array(String).new(2 + keys.size)
    command << "bzpopmax"
    command.concat keys
    command << timeout.to_s

    run command
  end

  def bzpopmin(keys : Enumerable(String), timeout : Time::Span | String | Int)
    if timeout.is_a? Time::Span
      timeout = timeout.total_seconds.to_i64
    end
    command = Array(String).new(2 + keys.size)
    command << "bzpopmin"
    command.concat keys
    command << timeout.to_s

    run command
  end

  def zscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"zscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
