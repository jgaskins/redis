module Redis::Commands::SortedSet
  # Return the number of members in the sorted set stored at `key`, or `0` if
  # `key` does not exist.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zcard "scores" # => 2
  # ```
  def zcard(key : String)
    run({"zcard", key})
  end

  # Return the 0-based rank of `member` in the sorted set stored at `key`, with
  # members ordered from lowest to highest score. Returns `nil` if `member` is
  # not in the sorted set or `key` does not exist.
  #
  # If `withscore` is `true`, returns a tuple of the rank and the score of the
  # member instead (requires Redis 7.2+).
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zrank "scores", "two"                  # => 1
  # redis.zrank "scores", "two", withscore: true # => [1, "2"]
  # redis.zrank "scores", "three"                # => nil
  # ```
  def zrank(key : String, member : String, *, withscore : Bool = false)
    command = {"zrank", key, member}
    command += {"withscore"} if withscore

    run command
  end

  # Return the members of the sorted set stored at `key` with ranks between
  # `starting` and `ending` (inclusive), ordered from highest to lowest score.
  # Negative ranks count from the end, so `-1` is the lowest-scored member.
  #
  # If `with_scores` is `true`, each member is followed by its score in the
  # returned `Array`.
  #
  # NOTE: The `ZREVRANGE` command is deprecated as of Redis 6.2. Prefer `zrange`
  # with `rev: true`.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zrevrange "scores", 0, -1                   # => ["three", "two", "one"]
  # redis.zrevrange "scores", 0, 1, with_scores: true # => ["three", "3", "two", "2"]
  # ```
  def zrevrange(key : String, starting : String | Int64, ending : String | Int64, with_scores : Bool = false)
    command = {"zrevrange", key, starting.to_s, ending.to_s}
    if with_scores
      command += {"withscores"}
    end

    run command
  end

  # Return the members of the sorted set stored at `key` between `start` and
  # `stop` (inclusive). How `start` and `stop` are interpreted depends on `by`:
  #
  # - By default, they are 0-based ranks, where negative ranks count from the
  #   end (`-1` is the highest-scored member)
  # - With `by: :score`, they are scores. Prefix a score with `(` to make it
  #   exclusive, or use `"-inf"` and `"+inf"` for unbounded ranges.
  # - With `by: :lex`, they are lexicographical bounds, which must be prefixed
  #   with `[` (inclusive) or `(` (exclusive), or be `"-"` or `"+"` for
  #   unbounded ranges. This is only meaningful when all members share the same
  #   score.
  #
  # Additional options:
  #
  # - `rev` returns members in descending order. When combined with `by`, you
  #   must also swap `start` and `stop` so that `start` is the higher bound.
  # - `limit` is an `{offset, count}` tuple to paginate the results; only valid
  #   with `by`
  # - `with_scores` causes each member to be followed by its score in the
  #   returned `Array`
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zrange "scores", 0, -1                    # => ["one", "two", "three"]
  # redis.zrange "scores", "(1", "+inf", by: :score # => ["two", "three"]
  # redis.zrange "scores", "+inf", "-inf", by: :score, rev: true, limit: {0, 1}, with_scores: true
  # # => ["three", "3"]
  #
  # redis.zadd "letters", "0", "a", "0", "b", "0", "c"
  # redis.zrange "letters", "-", "+", by: :lex, limit: {0, 2} # => ["a", "b"]
  # ```
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

  # Store the members of the sorted set at `source` between `min` and `max` into
  # the sorted set at `destination`, returning the number of members in the
  # resulting sorted set. If `destination` already exists, it is overwritten.
  #
  # The `min`, `max`, `by`, `rev`, and `limit` arguments behave the same as
  # they do for `zrange`. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zrangestore "top-scores", "scores", "1", "-1" # => 2
  # redis.zrange "top-scores", 0, -1                    # => ["two", "three"]
  # redis.zrangestore "low-scores", "scores", "1", "2",
  #   by: :score,
  #   limit: {0, 1} # => 1
  # ```
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

  # How `zrange` and `zrangestore` interpret their range bounds
  enum ZRangeOrder
    # Interpret bounds lexicographically (`BYLEX`)
    LEX

    # Interpret bounds as scores (`BYSCORE`)
    SCORE
  end

  # Return the members of the sorted set stored at `key` with scores between
  # `min` and `max` (inclusive), ordered from lowest to highest score. Prefix a
  # score with `(` to make it exclusive, or use `"-inf"` and `"+inf"` for
  # unbounded ranges.
  #
  # If `with_scores` is `true`, each member is followed by its score in the
  # returned `Array`. `limit` is an `{offset, count}` pair to paginate the
  # results.
  #
  # NOTE: The `ZRANGEBYSCORE` command is deprecated as of Redis 6.2. Prefer
  # `zrange` with `by: :score`.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zrangebyscore "scores", "(1", "+inf"                # => ["two", "three"]
  # redis.zrangebyscore "scores", 1.0, 2.0, with_scores: true # => ["one", "1", "two", "2"]
  # ```
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

  # Remove all members of the sorted set stored at `key` that fall within the
  # lexicographical `range`, returning the number of members removed. The
  # beginning of the range is always inclusive, and the end is exclusive if the
  # range excludes its end (`...`). Beginless and endless ranges are unbounded
  # on that side.
  #
  # This is only meaningful when all members share the same score.
  #
  # ```
  # redis.zadd "letters", "0", "a", "0", "b", "0", "c", "0", "d"
  # redis.zremrangebylex "letters", "a"..."c" # => 2
  # redis.zremrangebylex "letters", .."c"     # => 1
  # ```
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

  # Remove all members of the sorted set stored at `key` between the
  # lexicographical bounds `min` and `max`, returning the number of members
  # removed. Both bounds must be prefixed with `[` (inclusive) or `(`
  # (exclusive), or be `"-"` or `"+"` for unbounded ranges.
  #
  # This is only meaningful when all members share the same score.
  #
  # Raises `ArgumentError` if either bound is not formatted correctly.
  #
  # ```
  # redis.zadd "letters", "0", "a", "0", "b", "0", "c", "0", "d"
  # redis.zremrangebylex "letters", "[a", "(c" # => 2
  # redis.zremrangebylex "letters", "-", "+"   # => 2
  # ```
  def zremrangebylex(key : String, min : String, max : String)
    unless min.starts_with?('[') || min.starts_with?('(') || min == "-" || min == "+"
      raise ArgumentError.new(%{zremrangebylex requires that `min` start with either '[' or '(' or be the special "-" or "+" values})
    end
    unless max.starts_with?('[') || max.starts_with?('(') || max == "-" || max == "+"
      raise ArgumentError.new(%{zremrangebylex requires that `max` start with either '[' or '(' or be the special "-" or "+" values})
    end

    run({"zremrangebylex", key, min, max})
  end

  # Remove all members of the sorted set stored at `key` with scores within
  # `range`, returning the number of members removed. The end of the range is
  # exclusive if the range excludes its end (`...`). Beginless and endless
  # ranges are unbounded on that side.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three", "4", "four"
  # redis.zremrangebyscore "scores", 1...3 # => 2
  # redis.zremrangebyscore "scores", ..3.5 # => 1
  # ```
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

  # Remove all members of the sorted set stored at `key` with scores between
  # `min` and `max` (inclusive), returning the number of members removed.
  # Prefix a score with `(` to make it exclusive, or use `"-inf"` and `"+inf"`
  # for unbounded ranges.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zremrangebyscore "scores", min: 1.0, max: "(3" # => 2
  # ```
  def zremrangebyscore(key : String, min : String | Float64, max : String | Float64)
    run({"zremrangebyscore", key, min.to_s, max.to_s})
  end

  # Remove all members of the sorted set stored at `key` whose 0-based rank
  # (ordered from lowest to highest score) falls within `range`, returning the
  # number of members removed. Negative ranks count from the end, so `-1` is the
  # highest-scored member.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three", "4", "four"
  # redis.zremrangebyrank "scores", 0...2 # => 2 (removes "one" and "two")
  # redis.zremrangebyrank "scores", -1..  # => 1 (removes "four")
  # ```
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

  # Remove all members of the sorted set stored at `key` with 0-based ranks
  # between `start` and `stop` (inclusive), ordered from lowest to highest
  # score, returning the number of members removed. Negative ranks count from
  # the end, so `-1` is the highest-scored member.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zremrangebyrank "scores", 0, 1 # => 2
  # ```
  def zremrangebyrank(key : String, start : Int64 | String, stop : Int64 | String)
    run({"zremrangebyrank", key, start.to_s, stop.to_s})
  end

  # Add `value` with the given `score` to the sorted set stored at `key`,
  # returning the number of members added. If `value` is already a member, its
  # score is updated and it is not counted.
  #
  # Setting the following options to `true` will have the corresponding effect
  # on the command:
  #
  # - `nx` will only add new members, never updating existing ones; mutually exclusive with `xx`, `gt`, and `lt`
  # - `xx` will only update existing members, never adding new ones; mutually exclusive with `nx`
  # - `gt` will only update existing members if the new score is *greater* than the current score
  # - `lt` will only update existing members if the new score is *less* than the current score
  # - `ch` alters the return value to represent the number of members *changed* (added or updated) rather than only those *added*
  # - `incr` increments the member's score by `score` instead of setting it, like `zincrby`, and returns the new score (or `nil` if the operation was aborted by another option)
  #
  # ```
  # redis.zadd "scores", "1", "one"                     # => 1
  # redis.zadd "scores", "2", "one"                     # => 0
  # redis.zadd "scores", "3", "one", ch: true           # => 1
  # redis.zadd "scores", 1.5, "one", incr: true         # => "4.5"
  # redis.zadd "scores", "1", "one", gt: true, ch: true # => 0
  # ```
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

  # Add one or more members to the sorted set stored at `key`, returning the
  # number of members added. `values` must be alternating score/member pairs.
  # Members that already exist have their scores updated and are not counted.
  #
  # The `nx`, `xx`, `gt`, `lt`, and `ch` options behave the same as in the
  # single-member overload of this method.
  #
  # ```
  # redis.zadd "scores",
  #   "1", "one",
  #   "2", "two" # => 2
  # ```
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

  # Add one or more members to the sorted set stored at `key`, returning the
  # number of members added. `values` must contain alternating score/member
  # pairs. Members that already exist have their scores updated and are not
  # counted. This overload is useful when the number of members is not known at
  # compile time.
  #
  # The `nx`, `xx`, `gt`, `lt`, `ch`, and `incr` options behave the same as in
  # the single-member overload of this method. Note that `incr` only supports a
  # single score/member pair.
  #
  # Raises `ArgumentError` if `values` has an odd number of elements.
  #
  # ```
  # redis.zadd "scores", %w[
  #   1 one
  #   2 two
  #   3 three
  # ] # => 3
  # ```
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

  # Remove `value` from the sorted set stored at `key`, returning `1` if it was
  # removed or `0` if it was not a member.
  #
  # ```
  # redis.zadd "scores", "1", "one"
  # redis.zrem "scores", "one" # => 1
  # redis.zrem "scores", "one" # => 0
  # ```
  def zrem(key : String, value : String)
    run({"zrem", key, value})
  end

  # Remove one or more `values` from the sorted set stored at `key`, returning
  # the number of members removed. Values that are not members are ignored.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zrem "scores", "one", "two", "three" # => 2
  # ```
  def zrem(key : String, *values : String)
    run({"zrem", key} + values)
  end

  # Remove one or more `values` from the sorted set stored at `key`, returning
  # the number of members removed. Values that are not members are ignored.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zrem "scores", %w[one two three] # => 2
  # ```
  def zrem(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "zrem" << key
    command.concat values

    run command
  end

  # Return the number of members of the sorted set stored at `key` with scores
  # between `min` and `max` (inclusive). Prefix a score with `(` to make it
  # exclusive, or use `"-inf"` and `"+inf"` for unbounded ranges.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zcount "scores", "-inf", "+inf" # => 3
  # redis.zcount "scores", "(1", "3"      # => 2
  # ```
  def zcount(key : String, min : String, max : String)
    run({"zcount", key, min, max})
  end

  # Return one or more random members from the sorted set stored at `key`.
  # Requires Redis 6.2+.
  #
  # - Without `count`, returns a single member, or `nil` if `key` does not exist
  # - With a positive `count`, returns an `Array` of up to `count` *distinct* members
  # - With a negative `count`, returns an `Array` of exactly `count.abs` members, which may contain duplicates
  #
  # If `withscores` is `true`, each member is followed by its score in the
  # returned `Array`. This requires that `count` also be passed.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zrandmember "scores"                             # => "two"
  # redis.zrandmember "scores", count: 2                   # => ["three", "one"]
  # redis.zrandmember "scores", count: -4                  # => ["one", "one", "three", "two"]
  # redis.zrandmember "scores", count: 1, withscores: true # => ["two", "2"]
  # ```
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

  # Increment the score of `member` in the sorted set stored at `key` by
  # `increment`, returning the new score. If `member` is not in the sorted set,
  # it is added with `increment` as its score. A negative `increment` decrements
  # the score.
  #
  # ```
  # redis.zadd "scores", "1", "one"
  # redis.zincrby "scores", 2, "one"  # => "3"
  # redis.zincrby "scores", -1, "one" # => "2"
  # ```
  def zincrby(key : String, increment : String | Int, member : String)
    run({"zincrby", key, increment.to_s, member})
  end

  # Return the score of `value` in the sorted set stored at `key`, or `nil` if
  # `value` is not a member or `key` does not exist.
  #
  # ```
  # redis.zadd "scores", "1", "one"
  # redis.zscore "scores", "one" # => "1"
  # redis.zscore "scores", "two" # => nil
  # ```
  def zscore(key : String, value : String)
    run({"zscore", key, value})
  end

  # Return the members of the first sorted set in `keys` that are not in any of
  # the remaining sorted sets, ordered by score. If `withscores` is `true`, each
  # member is followed by its score in the returned `Array`. Requires Redis
  # 6.2+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zdiff "a", "b"                   # => ["three"]
  # redis.zdiff "a", "b", withscores: true # => ["three", "3"]
  # ```
  def zdiff(*keys : String, withscores : Bool = false)
    command = {"zdiff", keys.size.to_s} + keys
    command += {"withscores"} if withscores

    run command
  end

  # Return the members of the first sorted set in `keys` that are not in any of
  # the remaining sorted sets, ordered by score. If `withscores` is `true`, each
  # member is followed by its score in the returned `Array`. Requires Redis
  # 6.2+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zdiff %w[a b] # => ["three"]
  # ```
  def zdiff(keys : Enumerable(String), withscores : Bool = false)
    command = Array(String).new(3 + keys.size)
    command << "zdiff" << keys.size.to_s
    command.concat keys
    command << "withscores" if withscores

    run command
  end

  # Store the members of the first sorted set in `keys` that are not in any of
  # the remaining sorted sets at `destination`, returning the number of members
  # in the resulting sorted set. If `destination` already exists, it is
  # overwritten. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zdiffstore "a-minus-b", "a", "b" # => 1
  # ```
  def zdiffstore(destination : String, *keys : String)
    run({"zdiffstore", destination, keys.size.to_s} + keys)
  end

  # Store the members of the first sorted set in `keys` that are not in any of
  # the remaining sorted sets at `destination`, returning the number of members
  # in the resulting sorted set. If `destination` already exists, it is
  # overwritten. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zdiffstore "a-minus-b", %w[a b] # => 1
  # ```
  def zdiffstore(destination : String, keys : Enumerable(String))
    command = Array(String).new(3 + keys.size)
    command << "zdiffstore" << destination << keys.size.to_s
    command.concat keys

    run command
  end

  # Return the members that are in all of the sorted sets stored at `keys`,
  # ordered by score. Each member's score in the result is the sum of its scores
  # across all of the sorted sets. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two"
  # redis.zadd "b", "1", "one", "3", "three"
  # redis.zinter "a", "b" # => ["one"]
  # ```
  def zinter(*keys : String)
    command = {"zinter", keys.size.to_s} + keys
    run command
  end

  # Return the members that are in all of the sorted sets stored at `keys`,
  # ordered by score. Requires Redis 6.2+.
  #
  # - `weights` is a multiplication factor for each sorted set's scores, in the same order as `keys`, applied before aggregation
  # - `aggregate` specifies how to combine a member's scores across the sorted sets; defaults to `SUM`
  # - `withscores` causes each member to be followed by its aggregated score in the returned `Array`
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zinter %w[a b], weights: %w[1 3], withscores: true
  # # => ["one", "4", "two", "8"]
  # redis.zinter %w[a b], aggregate: :max, withscores: true
  # # => ["one", "1", "two", "2"]
  # ```
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

  # Store the members that are in all of the sorted sets stored at `keys` at
  # `destination`, returning the number of members in the resulting sorted set.
  # Each member's score is the sum of its scores across all of the sorted sets.
  # If `destination` already exists, it is overwritten.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two"
  # redis.zadd "b", "1", "one", "3", "three"
  # redis.zinterstore "a-and-b", "a", "b" # => 1
  # ```
  def zinterstore(destination : String, *keys : String)
    run({"zinterstore", destination, keys.size.to_s} + keys)
  end

  # Store the members that are in all of the sorted sets stored at `keys` at
  # `destination`, returning the number of members in the resulting sorted set.
  # If `destination` already exists, it is overwritten.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two"
  # redis.zadd "b", "1", "one", "3", "three"
  # redis.zinterstore "a-and-b", %w[a b] # => 1
  # ```
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

  # Return the number of members in the intersection of the sorted sets stored
  # at `keys`, without returning the members themselves. Requires Redis 7.0+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two"
  # redis.zintercard "a", "b" # => 2
  # ```
  def zintercard(*keys : String)
    run({"zintercard", keys.size.to_s} + keys)
  end

  # Return the number of members in the intersection of the sorted sets stored
  # at `keys`, without returning the members themselves. If `limit` is passed,
  # Redis stops counting once the cardinality reaches `limit`, which can be much
  # faster for large sets. Requires Redis 7.0+.
  #
  # ```
  # redis.zadd "a", "1", "one", "2", "two", "3", "three"
  # redis.zadd "b", "1", "one", "2", "two", "3", "three"
  # redis.zintercard %w[a b]           # => 3
  # redis.zintercard %w[a b], limit: 2 # => 2
  # ```
  def zintercard(keys : Enumerable(String), limit : String | Int | Nil = nil)
    command = Array(String).new(4 + keys.size)
    command << "zintercard" << keys.size.to_s
    command.concat keys
    command << "limit" << limit.to_s if limit

    run command
  end

  # How `zinter` and `zinterstore` combine a member's scores across sorted sets
  enum ZINTERAggregate
    # Add the member's scores together (the default)
    SUM

    # Use the lowest of the member's scores
    MIN

    # Use the highest of the member's scores
    MAX

    # Use the number of sorted sets containing the member (requires Redis 8.8+)
    COUNT
  end

  # Return the number of members of the sorted set stored at `key` between the
  # lexicographical bounds `min` and `max`. Both bounds must be prefixed with
  # `[` (inclusive) or `(` (exclusive), or be `"-"` or `"+"` for unbounded
  # ranges.
  #
  # This is only meaningful when all members share the same score.
  #
  # ```
  # redis.zadd "letters", "0", "a", "0", "b", "0", "c", "0", "d"
  # redis.zlexcount "letters", "[b", "[d" # => 3
  # redis.zlexcount "letters", "(b", "(d" # => 1
  # ```
  def zlexcount(key : String, min : String, max : String)
    run({"zlexcount", key, min, max})
  end

  # Return the scores of each of `members` in the sorted set stored at `key`,
  # in the same order as `members`. Members that are not in the sorted set have
  # a `nil` score. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zmscore "scores", "one", "two", "three" # => ["1", "2", nil]
  # ```
  def zmscore(key : String, *members : String)
    run({"zmscore", key} + members)
  end

  # Return the scores of each of `members` in the sorted set stored at `key`,
  # in the same order as `members`. Members that are not in the sorted set have
  # a `nil` score. Requires Redis 6.2+.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.zmscore "scores", %w[one two three] # => ["1", "2", nil]
  # ```
  def zmscore(key : String, members : Enumerable(String))
    command = Array(String).new(2 + members.size)
    command << "zmscore" << key
    command.concat members

    run command
  end

  # Remove and return the member with the highest score from the sorted set
  # stored at `key`, as an `Array` containing the member followed by its score.
  # If `count` is passed, removes and returns up to `count` members, from
  # highest to lowest score, each followed by its score. Returns an empty
  # `Array` if `key` does not exist.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zpopmax "scores"           # => ["three", "3"]
  # redis.zpopmax "scores", count: 2 # => ["two", "2", "one", "1"]
  # ```
  def zpopmax(key : String, *, count : String | Int | Nil = nil)
    command = {"zpopmax", key}
    command += {count.to_s} if count

    run command
  end

  # Remove and return the member with the lowest score from the sorted set
  # stored at `key`, as an `Array` containing the member followed by its score.
  # If `count` is passed, removes and returns up to `count` members, from
  # lowest to highest score, each followed by its score. Returns an empty
  # `Array` if `key` does not exist.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zpopmin "scores"           # => ["one", "1"]
  # redis.zpopmin "scores", count: 2 # => ["two", "2", "three", "3"]
  # ```
  def zpopmin(key : String, *, count : String | Int | Nil = nil)
    command = {"zpopmin", key}
    command += {count.to_s} if count

    run command
  end

  # Blocking version of `zmpop`. Remove and return members from the first
  # non-empty sorted set in `keys`, waiting up to `timeout` for one of them to
  # receive members if they are all empty. A `timeout` of `0` blocks
  # indefinitely. Returns `nil` if the timeout expires. Requires Redis 7.0+.
  #
  # NOTE: A `Time::Span` timeout is truncated to whole seconds, so a span of
  # less than one second becomes `0` and blocks indefinitely.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.bzmpop 1.second, %w[scores], :min           # => ["scores", [["one", "1"]]]
  # redis.bzmpop 1.second, %w[scores], :max, count: 2 # => ["scores", [["three", "3"], ["two", "2"]]]
  # redis.bzmpop 1.second, %w[empty], :min            # => nil (after 1 second)
  # ```
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

  # Remove and return members from the first non-empty sorted set in `keys`.
  # `side` determines whether the lowest- or highest-scored members are popped,
  # and `count` is the maximum number of members to pop (defaults to `1`).
  # Returns an `Array` containing the key that was popped from followed by an
  # `Array` of member/score pairs, or `nil` if all of the sorted sets are empty.
  # Requires Redis 7.0+.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two", "3", "three"
  # redis.zmpop %w[empty scores], :min           # => ["scores", [["one", "1"]]]
  # redis.zmpop %w[empty scores], :max, count: 2 # => ["scores", [["three", "3"], ["two", "2"]]]
  # redis.zmpop %w[empty], :min                  # => nil
  # ```
  def zmpop(keys : Enumerable(String), side : ZMPOPSide, count : String | Int | Nil = nil)
    command = Array(String).new(5 + keys.size)
    command << "zmpop" << keys.size.to_s
    command.concat keys
    command << side.to_s
    command << "count" << count.to_s if count

    run command
  end

  # Which end of a sorted set `zmpop` and `bzmpop` pop members from
  enum ZMPOPSide
    # Pop the lowest-scored members
    MIN

    # Pop the highest-scored members
    MAX
  end

  # Blocking version of `zpopmax`. Remove and return the highest-scored member
  # from the first non-empty sorted set in `keys`, waiting up to `timeout` for
  # one of them to receive members if they are all empty. A `timeout` of `0`
  # blocks indefinitely. Returns an `Array` containing the key, the member, and
  # its score, or `nil` if the timeout expires.
  #
  # NOTE: A `Time::Span` timeout is truncated to whole seconds, so a span of
  # less than one second becomes `0` and blocks indefinitely.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.bzpopmax %w[empty scores], 1.second # => ["scores", "two", "2"]
  # ```
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

  # Blocking version of `zpopmin`. Remove and return the lowest-scored member
  # from the first non-empty sorted set in `keys`, waiting up to `timeout` for
  # one of them to receive members if they are all empty. A `timeout` of `0`
  # blocks indefinitely. Returns an `Array` containing the key, the member, and
  # its score, or `nil` if the timeout expires.
  #
  # NOTE: A `Time::Span` timeout is truncated to whole seconds, so a span of
  # less than one second becomes `0` and blocks indefinitely.
  #
  # ```
  # redis.zadd "scores", "1", "one", "2", "two"
  # redis.bzpopmin %w[empty scores], 1.second # => ["scores", "one", "1"]
  # ```
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

  # Incrementally iterate over the members of the sorted set stored at `key`,
  # starting at `cursor` (use `"0"` to begin). Returns an `Array` containing the
  # next cursor followed by an `Array` of alternating members and scores. When
  # the returned cursor is `"0"`, iteration is complete.
  #
  # - `match` only returns members matching the given glob-style pattern
  # - `count` is a hint for how many members Redis should examine per call
  #
  # NOTE: Most of the time, you will want to use `Redis::Client#zscan_each`
  # instead, which handles the cursor for you.
  #
  # ```
  # cursor, members = redis.zscan("scores", "0").as(Array)
  # ```
  def zscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"zscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
