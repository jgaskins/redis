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
  # redis.hget "person:jamie", "email" # => "jamie@example.com"
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
  # redis.hmget "person:jamie", "email", "name" # => ["jamie@example.com", "Jamie"]
  # redis.hmget "person:jamie", "nonexistent", "fake-field" # => [nil, nil]
  # ```
  def hmget(key : String, *fields : String)
    run({"hmget", key} + fields)
  end

  # Return the values for `fields` in `key` as an `Array`
  #
  # ```
  # redis.hset "person:jamie", email: "jamie@example.com", name: "Jamie"
  # redis.hmget "person:jamie", %w[email name] # => ["jamie@example.com", "Jamie"]
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
  # redis.hset "person:jamie", admin: "false" # => 0
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
  # redis.hset "person:jamie", "admin", "false" # => 0
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
  # redis.hset "person:jamie", %w[admin false] # => 0
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
  # redis.hset "person:jamie", {"email" =>  "jamie@example.com", "name" =>  "Jamie"} # => 2
  # redis.hset "person:jamie", {"email" => "jamie@example.dev", "admin" =>  "true"} # => 1
  # redis.hset "person:jamie", {"admin" => "false"} # => 0
  # ```
  def hset(key : String, fields : ::Hash(String, String))
    command = Array(String).new(initial_capacity: 2 + 2 * fields.size)

    command << "hset" << key
    fields.each { |key, value| command << key << value }

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
