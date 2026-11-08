module Redis::Commands::Hash
  # Delete one or more `fields` from the given `key`, returning the number of
  # deleted fields.
  #
  # ```
  # redis.hdel "my-hash",
  #   "pending",
  #   "nonexistent-field"
  # # => 1
  # ```
  def hdel(key : String, *fields : String)
    run({"hdel", key} + fields)
  end

  # Delete one or more `fields` from the given `key`, returning the number of
  # deleted fields.
  #
  # ```
  # fields = %w[pending nonexistent-field]
  # redis.hdel "my-hash", fields
  # # => 1
  # ```
  def hdel(key : String, fields : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + fields.size)
    command << "hdel" << key
    fields.each { |key| command << key }

    run command
  end

  # Return the value of `field` for `key`, if both exist.
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com"
  # redis.hget "person:jamie", "email"    # => "jamie@example.com"
  # redis.hget "person:jamie", "password" # => nil
  # ```
  def hget(key : String, field : String)
    run({"hget", key, field})
  end

  # Return the entire hash stored at `key` as an `Array`
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com", name: "Jamie"
  # redis.hgetall "person:jamie"
  # # => ["email", "jamie@example.com", "name", "Jamie"]
  # ```
  def hgetall(key : String)
    run({"hgetall", key})
  end

  # Return the values for `fields` in `key` as an `Array`
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com", name: "Jamie"
  # redis.hmget "person:jamie", "email", "name"             # => ["jamie@example.com", "Jamie"]
  # redis.hmget "person:jamie", "nonexistent", "fake-field" # => [nil, nil]
  # ```
  def hmget(key : String, *fields : String)
    run({"hmget", key} + fields)
  end

  # Return the values for `fields` in `key` as an `Array`
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com", name: "Jamie"
  # redis.hmget "person:jamie", %w[email name]             # => ["jamie@example.com", "Jamie"]
  # redis.hmget "person:jamie", %w[nonexistent fake-field] # => [nil, nil]
  # ```
  def hmget(key : String, fields : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + fields.size)
    command << "hmget" << key
    fields.each { |field| command << field }

    run command
  end

  # Set the values for `fields` in the hash stored in `key`, returning the
  # number of fields created (not updated)
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com", name: "Jamie" # => 2
  # redis.hset "person:jamie", email: "jamie@example.dev", admin: "true" # => 1
  # redis.hset "person:jamie", admin: "false"                            # => 0
  # ```
  def hset(key : String, **fields : String)
    hash = ::Hash(String, String).new(initial_capacity: fields.size)
    fields.each do |key, value|
      hash[key.to_s] = value
    end

    hset key, hash
  end

  # Set the values for `fields` in the hash stored in `key`, returning the
  # number of fields created (not updated).
  #
  # NOTE: You _MUST_ pass an even number of arguments to `fields`
  #
  # ```
  # redis.hset "person:jamie", "email", "jamie@example.com", "name", "Jamie" # => 2
  # redis.hset "person:jamie", "email", "jamie@example.dev", "admin", "true" # => 1
  # redis.hset "person:jamie", "admin", "false"                              # => 0
  # ```
  def hset(key : String, *fields : String)
    run({"hset", key} + fields)
  end

  # Set the values for `fields` in the hash stored in `key`, returning the
  # number of fields created (not updated)
  #
  # NOTE: `fields` _MUST_ contain an even number of elements
  #
  # ```
  # redis.hset "person:jamie", %w[email jamie@example.com name Jamie] # => 2
  # redis.hset "person:jamie", %w[email jamie@example.dev admin true] # => 1
  # redis.hset "person:jamie", %w[admin false]                        # => 0
  # ```
  def hset(key : String, fields : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + fields.size)

    command << "hset" << key
    fields.each { |field| command << field }

    run command
  end

  # Set the values for `fields` in the hash stored in `key`, returning the
  # number of fields created (not updated)
  #
  # ```
  # redis.hset "person:jamie", {"email" => "jamie@example.com", "name" => "Jamie"} # => 2
  # redis.hset "person:jamie", {"email" => "jamie@example.dev", "admin" => "true"} # => 1
  # redis.hset "person:jamie", {"admin" => "false"}                                # => 0
  # ```
  def hset(key : String, fields : ::Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + 2 * fields.size)

    command << "hset" << key
    fields.each { |key, value| command << key << value }

    run command
  end

  # Set the values for `fields` in the hash stored in `key`, optionally setting
  # or preserving their expiration. Returns `1` if the fields were set or `0` if
  # they were not. The fields are set all-or-nothing, so if `fnx` or `fxx`
  # excludes any one of them, none of them are set.
  #
  # - `fnx`: only set the fields if none of them exist
  # - `fxx`: only set the fields if all of them exist
  # - `ex`: expire the fields after this duration, with 1-second precision
  # - `px`: expire the fields after this duration, with 1ms precision
  # - `exat`: expire the fields at this time, with 1-second precision
  # - `pxat`: expire the fields at this time, with 1ms precision
  # - `keepttl`: keep the expiration the fields already have
  #
  # NOTE: Raises `ArgumentError` if you pass more than one of `ex`, `px`, `exat`,
  # `pxat`, and `keepttl`, or if you pass both `fnx` and `fxx`.
  #
  # ```
  # redis.hsetex "session:1234", {token: "abc123"}, ex: 1.hour                        # => 1
  # redis.hsetex "session:1234", {"token" => "def456", "csrf" => "ghi789"}, fnx: true # => 0
  # redis.hsetex "session:1234", {token: "def456"}, keepttl: true                     # => 1
  # ```
  def hsetex(
    key : String,
    fields : ::Hash(String, String) | NamedTuple,
    *,
    fnx : Bool = false,
    fxx : Bool = false,
    ex : Time::Span | String | Int | Nil = nil,
    px : Time::Span | String | Int | Nil = nil,
    exat : Time | String | Int | Nil = nil,
    pxat : Time | String | Int | Nil = nil,
    keepttl : Bool = false,
  )
    if {ex, px, exat, pxat, keepttl}.count(&.itself) > 1
      raise ArgumentError.new("ex, px, exat, pxat, and keepttl are mutually exclusive")
    end
    if fnx && fxx
      raise ArgumentError.new("fnx and fxx are mutually exclusive")
    end

    if ex.is_a? Time::Span
      ex = ex.total_seconds.round.to_i64
    elsif px.is_a? Time::Span
      px = px.total_milliseconds.round.to_i64
    elsif exat.is_a? Time
      exat = exat.to_unix
    elsif pxat.is_a? Time
      pxat = pxat.to_unix_ms
    end

    command = Array(String).new(initial_capacity: 2 + 2 * fields.size)
    command << "hsetex" << key
    command << "fnx" if fnx
    command << "fxx" if fxx
    command << "ex" << ex.to_s if ex
    command << "px" << px.to_s if px
    command << "exat" << exat.to_s if exat
    command << "pxat" << pxat.to_s if pxat
    command << "keepttl" if keepttl
    command << "fields" << fields.size.to_s
    fields.each { |key, value| command << key.to_s << value }

    run command
  end

  # Set `field` in the hash stored in `key` to `value` if and only if it does not exist. Returns `1` if the field was set, `0` if it was not.
  #
  # ```
  # id = 1234
  #
  # redis.hsetnx "job:#{id}", "locked_at", Time.utc.to_rfc3339 # => 1
  # # Returned 1, lock succeeds
  #
  # redis.hsetnx "job:#{id}", "locked_at", Time.utc.to_rfc3339 # => 0
  # # Returned 0, lock did not succeed, so the job is already being processed
  # ```
  def hsetnx(key : String, field : String, value : String)
    run({"hsetnx", key, field, value})
  end

  # Increment the numeric value for `field` in the hash stored in `key` by
  # `increment`, returning the new value.
  #
  # ```
  # id = 1234
  # redis.hincrby "posts:#{id}", "likes", 1 # => 1
  # redis.hincrby "posts:#{id}", "likes", 1 # => 2
  # ```
  def hincrby(key : String, field : String, increment : Int | String)
    run({"hincrby", key, field, increment.to_s})
  end

  # Execute the `HSCAN` command to return a subset of keys in the hash stored at
  # `key`, allowing you to iterate through the keys in the hash without blocking
  # the Redis server for too long at a time.
  #
  # NOTE: You probably want to use `Redis::Client#hscan_each` instead of using
  # this method directly.
  #
  # ```
  # cursor = ""
  # until cursor == "0"
  #   response = redis.hscan(key, cursor)
  #   cursor, fields = response.as(Array)
  #   cursor = cursor.as(String)
  #   fields.as(Array).each do |field|
  #     # Do something with that hash field...
  #   end
  # end
  # ```
  def hscan(key : String, cursor : String, *, match pattern : String? = nil, count : String | Int | Nil = nil)
    command = {"hscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count.to_s} if count

    run command
  end

  # Return the length of the value of `field` in the hash stored in `key`, or `0`
  # if either the field or the key does not exist.
  #
  # ```
  # redis.hset "person:jamie", name: "Jamie"
  # redis.hstrlen "person:jamie", "name"        # => 5
  # redis.hstrlen "person:jamie", "nonexistent" # => 0
  # ```
  def hstrlen(key : String, field : String)
    run({"hstrlen", key, field})
  end

  # Expire `fields` in the hash stored in `key` after `ttl`, with 1-second
  # precision. Returns an `Array` containing one result for each of the `fields`,
  # in the same order:
  #
  # - `1`: the expiration was set
  # - `0`: the expiration was not set because the `nx`, `xx`, `gt`, or `lt`
  #   condition was not met
  # - `2`: the field was deleted because `ttl` was `0`
  # - `-2`: the field or the key does not exist
  #
  # You can restrict which fields receive the new expiration:
  #
  # - `nx`: only fields that have no expiration
  # - `xx`: only fields that already have an expiration
  # - `gt`: only fields whose new expiration is later than their current one
  # - `lt`: only fields whose new expiration is earlier than their current one
  #
  # NOTE: A `Time::Span` is truncated to whole seconds, so a `ttl` shorter than 1
  # second deletes the fields immediately. Use `#hpexpire` for subsecond TTLs.
  #
  # ```
  # redis.hset "session:1234", token: "abc123", csrf: "def456"
  # redis.hexpire "session:1234", 1.hour, fields: %w[token nonexistent]
  # # => [1, -2]
  # redis.hexpire "session:1234", 1.hour, nx: true, fields: %w[token csrf]
  # # => [0, 1]
  # ```
  def hexpire(
    key : String,
    ttl : Time::Span | String | Int,
    *,
    nx : Bool = false,
    xx : Bool = false,
    gt : Bool = false,
    lt : Bool = false,
    fields : Enumerable(String),
  )
    if ttl.is_a? Time::Span
      ttl = ttl.total_seconds.to_i64
    end
    command = Array(String).new(6 + fields.size)

    command << "hexpire" << key << ttl.to_s
    command << "nx" if nx
    command << "xx" if xx
    command << "gt" if gt
    command << "lt" if lt
    command << "fields" << fields.size.to_s
    command.concat fields

    run command
  end

  # Expire `fields` in the hash stored in `key` after `ttl`, with 1ms precision.
  # See `#hexpire` for the options and the values in the returned `Array`.
  #
  # ```
  # redis.hset "session:1234", token: "abc123"
  # redis.hpexpire "session:1234", 1500.milliseconds, fields: %w[token]
  # # => [1]
  # ```
  def hpexpire(
    key : String,
    ttl : Time::Span | String | Int,
    *,
    nx : Bool = false,
    xx : Bool = false,
    gt : Bool = false,
    lt : Bool = false,
    fields : Enumerable(String),
  )
    if ttl.is_a? Time::Span
      ttl = ttl.total_milliseconds.to_i64
    end
    command = Array(String).new(6 + fields.size)

    command << "hpexpire" << key << ttl.to_s
    command << "nx" if nx
    command << "xx" if xx
    command << "gt" if gt
    command << "lt" if lt
    command << "fields" << fields.size.to_s
    command.concat fields

    run command
  end

  # Expire `fields` in the hash stored in `key` at the given `expiration` time,
  # with 1-second precision. An `Int` or `String` `expiration` is a Unix timestamp
  # in seconds. If `expiration` is in the past, the fields are deleted and their
  # result is `2`. See `#hexpire` for the options and the values in the returned
  # `Array`.
  #
  # ```
  # redis.hset "session:1234", token: "abc123"
  # redis.hexpireat "session:1234", 1.hour.from_now, fields: %w[token]
  # # => [1]
  # ```
  def hexpireat(
    key : String,
    expiration : Time | String | Int,
    *,
    nx : Bool = false,
    xx : Bool = false,
    gt : Bool = false,
    lt : Bool = false,
    fields : Enumerable(String),
  )
    if expiration.is_a? Time
      expiration = expiration.to_unix.to_i64
    end
    command = Array(String).new(6 + fields.size)

    command << "hexpireat" << key << expiration.to_s
    command << "nx" if nx
    command << "xx" if xx
    command << "gt" if gt
    command << "lt" if lt
    command << "fields" << fields.size.to_s
    command.concat fields

    run command
  end

  # Expire `fields` in the hash stored in `key` at the given `expiration` time,
  # with 1ms precision. An `Int` or `String` `expiration` is a Unix timestamp in
  # milliseconds. If `expiration` is in the past, the fields are deleted and their
  # result is `2`. See `#hexpire` for the options and the values in the returned
  # `Array`.
  #
  # ```
  # redis.hset "session:1234", token: "abc123"
  # redis.hpexpireat "session:1234", 1.hour.from_now, fields: %w[token]
  # # => [1]
  # ```
  def hpexpireat(
    key : String,
    expiration : Time | String | Int,
    *,
    nx : Bool = false,
    xx : Bool = false,
    gt : Bool = false,
    lt : Bool = false,
    fields : Enumerable(String),
  )
    if expiration.is_a? Time
      expiration = expiration.to_unix_ms.to_i64
    end
    command = Array(String).new(6 + fields.size)

    command << "hpexpireat" << key << expiration.to_s
    command << "nx" if nx
    command << "xx" if xx
    command << "gt" if gt
    command << "lt" if lt
    command << "fields" << fields.size.to_s
    command.concat fields

    run command
  end

  # Remove the expiration from `fields` in the hash stored in `key`. Returns an
  # `Array` containing one result for each of the `fields`, in the same order:
  #
  # - `1`: the expiration was removed
  # - `-1`: the field exists but has no expiration
  # - `-2`: the field or the key does not exist
  #
  # ```
  # redis.hset "session:1234", token: "abc123", csrf: "def456"
  # redis.hexpire "session:1234", 1.hour, fields: %w[token]
  # redis.hpersist "session:1234", "token", "csrf", "nonexistent"
  # # => [1, -1, -2]
  # ```
  def hpersist(key : String, *fields : String)
    run({"hpersist", key, "fields", fields.size.to_s} + fields)
  end

  # Remove the expiration from `fields` in the hash stored in `key`. Returns an
  # `Array` containing one result for each of the `fields`, in the same order:
  #
  # - `1`: the expiration was removed
  # - `-1`: the field exists but has no expiration
  # - `-2`: the field or the key does not exist
  #
  # ```
  # redis.hset "session:1234", token: "abc123", csrf: "def456"
  # redis.hexpire "session:1234", 1.hour, fields: %w[token]
  # redis.hpersist "session:1234", %w[token csrf nonexistent]
  # # => [1, -1, -2]
  # ```
  def hpersist(key : String, fields : Enumerable(String))
    command = Array(String).new(4 + fields.size)
    command << "hpersist" << key << "fields" << fields.size.to_s
    command.concat fields

    run command
  end

  # Return an `Array` containing the time to live in seconds of each of the
  # `fields` in the hash stored in `key`, in the same order. A value of `-1` means
  # the field exists but has no expiration, and `-2` means the field or the key
  # does not exist.
  #
  # ```
  # redis.hset "session:1234", token: "abc123", csrf: "def456"
  # redis.hexpire "session:1234", 1.hour, fields: %w[token]
  # redis.httl "session:1234", "token", "csrf", "nonexistent"
  # # => [3600, -1, -2]
  # ```
  def httl(key : String, *fields : String)
    run({"httl", key, "fields", fields.size.to_s} + fields)
  end

  # Return an `Array` containing the time to live in milliseconds of each of the
  # `fields` in the hash stored in `key`, in the same order. A value of `-1` means
  # the field exists but has no expiration, and `-2` means the field or the key
  # does not exist.
  #
  # ```
  # redis.hset "session:1234", token: "abc123", csrf: "def456"
  # redis.hexpire "session:1234", 1.hour, fields: %w[token]
  # redis.hpttl "session:1234", "token", "csrf", "nonexistent"
  # # => [3600000, -1, -2]
  # ```
  def hpttl(key : String, *fields : String)
    run({"hpttl", key, "fields", fields.size.to_s} + fields)
  end

  # Set the values for the fields in `data` in the hash stored in `key`,
  # returning `"OK"`.
  #
  # ```
  # redis.hmset "person:jamie", {"email" => "jamie@example.com", "name" => "Jamie"}
  # # => "OK"
  # ```
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
