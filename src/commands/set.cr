module Redis::Commands::Set
  # Add one or more `values` to the set stored at `key`, returning the number of
  # values that were added. Values already in the set are not counted.
  #
  # ```
  # redis.sadd "colors", "red", "green"  # => 2
  # redis.sadd "colors", "green", "blue" # => 1
  # ```
  def sadd(key : String, *values : String)
    run({"sadd", key} + values)
  end

  # Add one or more `values` to the set stored at `key`, returning the number of
  # values that were added. Values already in the set are not counted.
  #
  # ```
  # redis.sadd "colors", %w[red green]  # => 2
  # redis.sadd "colors", %w[green blue] # => 1
  # ```
  def sadd(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "sadd" << key
    command.concat values

    run command
  end

  # Return `1` if `value` is a member of the set stored at `key`, or `0` if it
  # is not or `key` does not exist.
  #
  # ```
  # redis.sadd "colors", "red"
  # redis.sismember "colors", "red"  # => 1
  # redis.sismember "colors", "blue" # => 0
  # ```
  def sismember(key : String, value : String)
    run({"sismember", key, value})
  end

  # Return all of the members of the set stored at `key` as an `Array`, in no
  # particular order. If `key` does not exist, the `Array` is empty.
  #
  # NOTE: This returns the entire set at once. For large sets, consider using
  # `Redis::Client#sscan_each` to avoid blocking the Redis server.
  #
  # ```
  # redis.sadd "colors", "red", "green"
  # redis.smembers "colors" # => ["red", "green"]
  # ```
  def smembers(key : String)
    run({"smembers", key})
  end

  # Remove one or more `values` from the set stored at `key`, returning the
  # number of values that were removed.
  #
  # ```
  # redis.sadd "colors", "red", "green"
  # redis.srem "colors", "red", "blue" # => 1
  # ```
  def srem(key : String, *values : String)
    run({"srem", key} + values)
  end

  # Remove one or more `members` from the set stored at `key`, returning the
  # number of members that were removed.
  #
  # ```
  # redis.sadd "colors", "red", "green"
  # redis.srem "colors", %w[red blue] # => 1
  # ```
  def srem(key : String, members : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + members.size)
    command << "srem" << key
    command.concat members

    run command
  end

  # Return the members of the set stored at `first` that are not in any of the
  # sets stored at `others`, as an `Array` in no particular order.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sdiff "primary", "warm" # => ["green", "blue"]
  # ```
  def sdiff(first : String, *others : String)
    run({"sdiff", first} + others)
  end

  # Store the members of the first set in `others` that are not in any of the
  # remaining sets in `others` at `destination`, returning the number of members
  # in the resulting set. If `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sdiffstore "cool", "primary", "warm" # => 2
  # ```
  def sdiffstore(destination : String, *others : String)
    run({"sdiffstore", destination} + others)
  end

  # Store the members of the first set in `others` that are not in any of the
  # remaining sets in `others` at `destination`, returning the number of members
  # in the resulting set. If `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sdiffstore "cool", %w[primary warm] # => 2
  # ```
  def sdiffstore(destination : String, others : Enumerable(String))
    command = Array(String).new(2 + others.size)
    command << "sdiffstore" << destination
    command.concat others

    run command
  end

  # Return the members that are in all of the sets stored at `first` and
  # `others`, as an `Array` in no particular order.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sinter "primary", "warm" # => ["red"]
  # ```
  def sinter(first : String, *others : String)
    run({"sinter", first} + others)
  end

  # Return the members that are in all of the sets stored at `keys`, as an
  # `Array` in no particular order.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sinter %w[primary warm] # => ["red"]
  # ```
  def sinter(keys : Enumerable(String))
    command = Array(String).new(initial_capacity: 1 + keys.size)
    command << "sinter"
    command.concat keys

    run command
  end

  # Store the members that are in all of the sets stored at `others` in
  # `destination`, returning the number of members in the resulting set. If
  # `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sinterstore "both", "primary", "warm" # => 1
  # ```
  def sinterstore(destination : String, *others : String)
    run({"sinterstore", destination} + others)
  end

  # Store the members that are in all of the sets stored at `others` in
  # `destination`, returning the number of members in the resulting set. If
  # `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sinterstore "both", %w[primary warm] # => 1
  # ```
  def sinterstore(destination : String, others : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + others.size)
    command << "sinterstore" << destination
    command.concat others

    run command
  end

  # Return the members that are in any of the sets stored at `keys` as an
  # `Array`, in no particular order.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sunion "primary", "warm" # => ["red", "green", "blue", "orange"]
  # ```
  def sunion(*keys : String)
    run({"sunion"} + keys)
  end

  # Return the members that are in any of the sets stored at `keys` as an
  # `Array`, in no particular order.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sunion %w[primary warm] # => ["red", "green", "blue", "orange"]
  # ```
  def sunion(keys : Enumerable(String))
    command = Array(String).new(1 + keys.size)
    command << "sunion"
    command.concat keys

    run command
  end

  # Store the members that are in any of the sets stored at `others` in
  # `destination`, returning the number of members in the resulting set. If
  # `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sunionstore "all", "primary", "warm" # => 4
  # ```
  def sunionstore(destination : String, *others : String)
    run({"sunionstore", destination} + others)
  end

  # Store the members that are in any of the sets stored at `others` in
  # `destination`, returning the number of members in the resulting set. If
  # `destination` already exists, it is overwritten.
  #
  # ```
  # redis.sadd "primary", "red", "green", "blue"
  # redis.sadd "warm", "red", "orange"
  # redis.sunionstore "all", %w[primary warm] # => 4
  # ```
  def sunionstore(destination : String, others : Enumerable(String))
    command = Array(String).new(2 + others.size)
    command << "sunionstore" << destination
    command.concat others

    run command
  end

  # Return the number of members in the set stored at `key`, or `0` if `key` does
  # not exist.
  #
  # ```
  # redis.sadd "colors", "red", "green"
  # redis.scard "colors" # => 2
  # ```
  def scard(key : String)
    run({"scard", key})
  end

  # Atomically move `member` from the set stored at `source` to the set stored at
  # `destination`. Returns `1` if `member` was moved, or `0` if it was not in
  # `source`.
  #
  # ```
  # redis.sadd "todo", "laundry"
  # redis.smove "todo", "done", "laundry" # => 1
  # redis.smove "todo", "done", "laundry" # => 0
  # ```
  def smove(source : String, destination : String, member : String)
    run({"smove", source, destination, member})
  end

  # Execute the `SSCAN` command to return a subset of members in the set stored
  # at `key`, allowing you to iterate through the members of the set without
  # blocking the Redis server for too long at a time. Pass `match` to return only
  # members matching a glob-style pattern and `count` to hint at how many members
  # to return per call.
  #
  # NOTE: You probably want to use `Redis::Client#sscan_each` instead of using
  # this method directly.
  #
  # ```
  # cursor = "0"
  # loop do
  #   response = redis.sscan(key, cursor)
  #   cursor, members = response.as(Array)
  #   cursor = cursor.as(String)
  #   members.as(Array).each do |member|
  #     # Do something with that set member...
  #   end
  #   break if cursor == "0"
  # end
  # ```
  def sscan(key : String, cursor : String, match pattern : String? = nil, count : String? = nil)
    command = {"sscan", key, cursor}
    command += {"match", pattern} if pattern
    command += {"count", count} if count

    run command
  end
end
