module Redis::Commands::Stream
  # Append an entry with the specified data to the stream with the given `key`
  # and gives it the specified `id`. If the id is `"*"`, Redis will assign it
  # an id of the form `"#{Time.utc.to_unix_ms}-#{autoincrementing_index}"`.
  # If `maxlen` is provided, Redis will trim the stream to the specified
  # length. If `maxlen` is of the form `~ 1000`, Redis will trim it to
  # *approximately* that length, removing entries when it can do so
  # efficiently. This method returns the `id` that Redis stores.
  #
  # ```
  # redis.xadd "my-stream", "*", name: "foo", id: UUID.random.to_s
  # ```
  def xadd(key : String, id : String, maxlen = nil, **data)
    command = Array(String).new(initial_capacity: data.size * 2 + 6)
    command << "xadd" << key
    if maxlen
      command << "maxlen"
      case maxlen
      when Tuple
        maxlen.each { |entry| command << entry }
      when String
        command << maxlen
      end
    end
    command << id
    data.each do |key, value|
      command << key.to_s << value
    end

    run command
  end

  # Append an entry with the specified data to the stream with the given `key`
  # and gives it the specified `id`. If the id is `"*"`, Redis will assign it
  # an id of the form `"#{Time.utc.to_unix_ms}-#{autoincrementing_index}"`.
  # If `maxlen` is provided, Redis will trim the stream to the specified
  # length. If `maxlen` is of the form `~ 1000`, Redis will trim it to
  # *approximately* that length, removing entries when it can do so
  # efficiently. This method returns the `id` that Redis stores.
  #
  # ```
  # redis.xadd "my-stream", "*", {"name" => "foo", "id" => UUID.random.to_s}
  # ```
  def xadd(key : String, id : String, data : ::Hash(String, String))
    xadd key, id, maxlen: nil, data: data
  end

  # Append an entry with the specified data to the stream with the given `key`
  # and gives it the specified `id`. If the id is `"*"`, Redis will assign it
  # an id of the form `"#{Time.utc.to_unix_ms}-#{autoincrementing_index}"`.
  # If `maxlen` is provided, Redis will trim the stream to the specified
  # length. If `maxlen` is of the form `~ 1000`, Redis will trim it to
  # *approximately* that length, removing entries when it can do so
  # efficiently. This method returns the `id` that Redis stores.
  #
  # ```
  # redis.xadd "my-stream", "*", {"name" => "foo", "id" => UUID.random.to_s}
  # ```
  def xadd(key : String, id : String, maxlen, data : ::Hash(String, String))
    command = Array(String).new(initial_capacity: data.size * 2 + 3)
    command << "xadd" << key
    command << "maxlen" << maxlen if maxlen
    command << id
    data.each do |key, value|
      command << key << value
    end

    run command
  end

  def xdel(key : String, *ids : String)
    xdel key, ids
  end

  def xdel(key : String, ids : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + ids.size)
    command << "xdel" << key
    ids.each { |id| command << id }

    run command
  end

  # Return the number of entries in the given stream
  def xlen(key : String)
    run({"xlen", key})
  end

  # Return the entries in the given stream between the `start` and `end` ids.
  # If `count` is provided, Redis will return only that number of entries.
  def xrange(key : String, start min, end max, count = nil)
    command = {"xrange", key, min, max}
    if count
      command += {"count", count}
    end

    run command
  end

  def xgroup_create(key : String, groupname : String, *, id : String = "$", mkstream = false)
    xgroup XGroup::CREATE, key, groupname, id: id, mkstream: mkstream
  end

  # XGROUP CREATECONSUMER key groupname consumername
  def xgroup_create_consumer(key : String, groupname : String, consumer_name : String)
    xgroup XGroup::CREATECONSUMER, key, groupname, consumer_name: consumer_name
  end

  # Run a Redis XGROUP subcommand for a given stream. See the [XGROUP command in the Redis documentation](https://redis.io/commands/xgroup) for more information.
  #
  # ```
  # redis.xgroup :create, "my-stream", "my-group", mkstream: true
  # ```
  def xgroup(command : XGroup, key : String, groupname : String, *, id : String? = nil, mkstream = false, consumer_name : String? = nil)
    cmd = Array(String).new(initial_capacity: 7)
    cmd << "xgroup" << command.to_s << key << groupname
    cmd << id if id
    cmd << "mkstream" if mkstream
    cmd << consumer_name if consumer_name

    run cmd
  end

  enum XGroup
    CREATE
    DESTROY
    CREATECONSUMER
    DELCONSUMER
  end

  # Run a Redis XGROUP subcommand for a given stream. See the [XGROUP command in the Redis documentation](https://redis.io/commands/xgroup) for more information.
  #
  # ```
  # redis.xgroup "DESTROY", "my-stream", "my-group"
  # ```
  def xgroup(command : String, key : String, groupname : String)
    run({"xgroup", command, key, groupname})
  end

  # Run a Redis XGROUP subcommand for a given stream. See the [XGROUP command in the Redis documentation](https://redis.io/commands/xgroup) for more information.
  #
  # ```
  # redis.xgroup "CREATE", "my-stream", "my-group", "0"
  # ```
  def xgroup(command : String, key : String, groupname : String, *args : String)
    run({"xgroup", command, key, groupname} + args)
  end

  # Execute an `XREADGROUP` command on the Redis server.
  #
  # This is returned in its raw form from Redis, but you can pass it to a
  # `Redis::Streaming::XReadGroupResponse` to make it easier to work with.
  def xreadgroup(
    group : String,
    consumer : String,
    count : String | Int | Nil = nil,
    block : Time::Span | String | Int | Nil = nil,
    no_ack = false,
    streams : ::Hash(String, String) = {} of String => String
  )
    command = Array(String).new(initial_capacity: 9 + streams.size * 2)
    command << "xreadgroup" << "group" << group << consumer
    command << "count" << count.to_s if count
    case block
    in Time::Span
      command << "block" << block.total_milliseconds.to_i.to_s
    in String
      command << "block" << block
    in Int
      command << "block" << block.to_s
    in Nil
      # No blocking, so we don't add it to the command
    end
    command << "noack" if no_ack
    command << "streams"
    streams.each_key { |key| command << key }
    streams.each_value { |value| command << value }

    run command
  end

  # Execute an `XREADGROUP` command on the Redis server. If `block` is not nil, the server will block for up to that much time (if you pass a number, it will be interpreted as milliseconds) until any new messages enter the stream.
  #
  # This is returned in its raw form from Redis, but you can pass it to a
  # `Redis::Streaming::XReadGroupResponse` to make it easier to work with.
  #
  # ```
  # # Long-poll for up to 10 messages from the stream with key `my_stream`,
  # # blocking for up to 2 seconds if there are no messages waiting.
  # response = redis.xreadgroup "group", "consumer",
  #   streams: {my_stream: ">"},
  #   count: 10,
  #   block: 2.seconds
  # response = Redis::Streaming::XReadGroupResponse.new(response)
  # ```
  def xreadgroup(
    group : String,
    consumer : String,
    count : String | Int | Nil = nil,
    block : Time::Span | String | Int | Nil = nil,
    no_ack = false,
    streams : NamedTuple = NamedTuple.new
  )
    command = Array(String).new(initial_capacity: 9 + streams.size * 2)
    command << "xreadgroup" << "group" << group << consumer
    command << "count" << count.to_s if count
    case block
    in Time::Span
      command << "block" << block.total_milliseconds.to_i.to_s
    in String
      command << "block" << block
    in Int
      command << "block" << block.to_s
    in Nil
      # No blocking, so we don't add it to the command
    end
    command << "noack" if no_ack
    command << "streams"
    streams.each_key { |key| command << key.to_s }
    streams.each_value { |value| command << value }

    run command
  end

  # XPENDING key group [[IDLE min-idle-time] start end count [consumer]]
  def xpending(key : String, group : String)
    run({"xpending", key, group})
  end

  def xpending(
    key : String,
    group : String,
    start : String,
    end finish : String,
    count : String | Int,
    idle : String | Time::Span | Nil = nil
  )
    command = {"xpending", key, group}
    case idle
    when String
      command += {"idle", idle}
    when Time::Span
      command += {"idle", idle.total_milliseconds.to_i.to_s}
    end
    command += {start, finish, count.to_s}

    run command
  end

  def xack(key : String, group : String, id : String)
    run({"xack", key, group, id})
  end

  def xack(key : String, group : String, ids : Enumerable(String))
    command = Array(String).new(initial_capacity: ids.size + 3)
    command << "xack" << key << group
    ids.each { |id| command << id }

    run command
  end

  def xautoclaim(
    key : String,
    group : String,
    consumer : String,
    min_idle_time : Time::Span,
    start : String,
    count : Int | String | Nil = nil
  )
    min_idle_time = min_idle_time.total_milliseconds.to_i.to_s
    command = {"xautoclaim", key, group, consumer, min_idle_time, start}
    command += {"count", count.to_s} if count

    run command
  end
end
