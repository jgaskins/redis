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
  # redis.xadd "my-stream", "*", {name: "foo", id: UUID.random.to_s}
  # ```
  @[Deprecated(%{Using keyword arguments for stream event fields is deprecated and will be removed in a future release. It causes conflicts with optional `maxlen` and `minid` when passed as string values. Use the `fields: {foo: "bar"}` overload instead.})]
  def xadd(key : String, id : String, **fields : String)
    xadd key, id, fields
  end

  @[Deprecated(%{Using keyword arguments for stream event fields is deprecated and will be removed in a future release. It causes conflicts with optional `maxlen` and `minid` when passed as string values. Use the `fields: {foo: "bar"}` overload instead.})]
  def xadd(key : String, id : String, *, maxlen, **fields : String)
    xadd key, id, maxlen: maxlen, fields: fields
  end

  @[Deprecated(%{Using keyword arguments for stream event fields is deprecated and will be removed in a future release. It causes conflicts with optional `maxlen` and `minid` when passed as string values. Use the `fields: {foo: "bar"}` overload instead.})]
  def xadd(key : String, id : String, *, minid, **fields : String)
    xadd key, id, minid: minid, fields: fields
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
  def xadd(key : String, id : String, fields : NamedTuple | ::Hash(String, String), *, idmpauto : String? = nil, idmp : {String, String}? = nil)
    xadd key, id,
      maxlen: nil,
      minid: nil,
      fields: fields,
      idmpauto: idmpauto,
      idmp: idmp
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
  # redis.xadd "my-stream", "*", maxlen: {"~", "1000"}, fields: {"name" => "foo", "id" => UUID.random.to_s}
  # ```
  def xadd(key : String, id : String, *, maxlen, fields : NamedTuple | ::Hash(String, String), idmpauto : String? = nil, idmp : {String, String}? = nil)
    xadd key, id,
      maxlen: maxlen,
      minid: nil,
      fields: fields,
      idmpauto: idmpauto,
      idmp: idmp
  end

  def xadd(key : String, id : String, *, minid, fields : NamedTuple | ::Hash(String, String), idmpauto : String? = nil, idmp : {String, String}? = nil)
    xadd key, id,
      maxlen: nil,
      minid: minid,
      fields: fields,
      idmpauto: idmpauto,
      idmp: idmp
  end

  private def xadd(
    key : String,
    id : String,
    *,
    maxlen : {String, String}?,
    minid : {String, String}?,
    limit : String | Int | Nil = nil,
    fields : NamedTuple | ::Hash(String, String),
    nomkstream : Bool = false,
    idmpauto : String? = nil,
    idmp : {String, String}? = nil,
  )
    command_size = 3 +                         # XADD key id
                   (maxlen || minid ? 3 : 0) + # MAXLEN ~ 1000
                   (limit ? 2 : 0) +           # LIMIT 100
                   (nomkstream ? 1 : 0) +      # NOMKSTREAM
                   (idmpauto ? 2 : 0) +        # IDMPAUTO producer-id
                   (idmp ? 3 : 0) +            # IDMP producer-id idempotency-key
                   fields.size * 2             # field id ...

    command = Array(String).new(initial_capacity: 12 + fields.size * 2)
    command << "xadd" << key
    if nomkstream
      command << "nomkstream"
    end
    if idmpauto
      command << "idmpauto" << idmpauto
    end
    if idmp
      command << "idmp"
      command.concat idmp
    end
    if maxlen
      command << "maxlen"
      case maxlen
      in String
        command << maxlen
      in Tuple(String, String)
        command << maxlen[0] << maxlen[1]
      in Nil
      end
      if limit
        command << "limit" << limit.to_s
      end
    elsif minid
      command << "minid"
      case minid
      in String
        command << minid
      in Tuple(String, String)
        command << minid[0] << minid[1]
      in Nil
      end
      if limit
        command << "limit" << limit.to_s
      end
    end
    command << id
    fields.each do |field, value|
      command << field.to_s << value
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

  def xread(
    *,
    count : Int | String | Nil = nil,
    block : Time::Span | Int | String | Nil = nil,
    streams : NamedTuple,
  )
    if block.is_a? Time::Span
      block = block.total_milliseconds.to_i64
    end
    command = {"xread"}
    command += {"count", count.to_s} if count
    command += {"block", block.to_s} if block
    command += {"streams"} + streams.keys.map(&.to_s) + streams.values

    run command
  end

  def xread(
    *,
    count : Int | String | Nil = nil,
    block : Time::Span | Int | String | Nil = nil,
    streams : ::Hash(String, String),
  )
    if block.is_a? Time::Span
      block = block.total_milliseconds.to_i64
    end
    command_size = streams.size * 2 + # key [key...] id [id...]
                   (count ? 2 : 0) +  # [COUNT count]
                   (block ? 2 : 0) +  # [BLOCK milliseconds]
                   2 +                # XREAD ... STREAMS
                   0

    command = Array(String).new(initial_capacity: command_size)
    command << "xread"
    command << "count" << count.to_s if count
    command << "block" << block.to_s if block
    command << "streams"
    streams.each_key { |key| command << key }
    streams.each_value { |value| command << value }

    run command
  end

  # Return the entries in the given stream between the `start` and `end` ids.
  # If `count` is provided, Redis will return only that number of entries.
  def xrange(key : String, start min : String, end max : String, count : String | Int32 | Nil = nil)
    command = {"xrange", key, min, max}
    command += {"count", count.to_s} if count

    run command
  end

  # Return the entries in the given stream between the `start` and `end` ids.
  # If `count` is provided, Redis will return only that number of entries.
  def xrevrange(key : String, end max : String, start min : String, count : String | Int32 | Nil = nil)
    command = {"xrevrange", key, max, min}
    command += {"count", count.to_s} if count

    run command
  end

  def xtrim(key : String, *, maxlen : {String, String}, limit : String | Int32 | Nil = nil, delete_mode : DeleteMode? = nil)
    command = {"xtrim", key, "maxlen"} + maxlen
    command += {"limit", limit.to_s} if limit
    command += {delete_mode.to_s} if delete_mode

    run command
  end

  def xtrim(key : String, *, minid : {String, String}, limit : String | Int32 | Nil = nil, delete_mode : DeleteMode? = nil)
    command = {"xtrim", key, "minid"} + minid
    command += {"limit", limit.to_s} if limit
    command += {delete_mode.to_s} if delete_mode

    run command
  end

  # Create the consumer group `groupname` in the stream contained in `key`.
  def xgroup_create(key : String, groupname : String, *, id : String = "$", mkstream = false)
    xgroup :create, key, groupname, id: id, mkstream: mkstream
  end

  # Create a consumer `consumer_name` in the consumer group `groupname` in the
  # stream contained in `key`.
  #
  # ```
  # consumer_id = UUID.v7.to_s
  # redis.xgroup_create "orders", "fulfillment", mkstream: true
  # redis.xgroup_create_consumer "orders", "fulfillment", consumer_id
  # ```
  def xgroup_create_consumer(key : String, groupname : String, consumer_name : String)
    xgroup :createconsumer, key, groupname, consumer_name: consumer_name
  end

  # Delete the consumer group `group` in the stream contained in `key`.
  def xgroup_destroy(key : String, group : String)
    xgroup :destroy, key, group
  end

  # Delete the given consumer from the given group in the stream stored in `key`.
  def xgroup_del_consumer(key : String, group : String, consumer : String)
    xgroup :delconsumer, key, group, consumer_name: consumer
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

  # Return the details about the stream stored in `key`.
  #
  # ```
  # stream = Redis::Streaming::XInfoStreamResponse.new(
  #   redis.xinfo_stream("orders")
  # )
  # # => Redis::Streaming::XInfoStreamResponse(
  # #     @entries_added=1,
  # #     @first_entry=
  # #      Redis::Streaming::Message(
  # #       @delivery_count=0,
  # #       @id="1780361273088-0",
  # #       @last_delivered_at=1970-01-01 00:00:00Z,
  # #       @values={"id" => "0"}),
  # #     @groups=1,
  # #     @idmp_duration=100,
  # #     @idmp_maxsize=100,
  # #     @iids_added=0,
  # #     @iids_duplicates=0,
  # #     @iids_tracked=0,
  # #     @last_entry=
  # #      Redis::Streaming::Message(
  # #       @delivery_count=0,
  # #       @id="1780361273088-0",
  # #       @last_delivered_at=1970-01-01 00:00:00Z,
  # #       @values={"id" => "0"}),
  # #     @last_generated_id="1780361273088-0",
  # #     @length=1,
  # #     @max_deleted_entry_id="0-0",
  # #     @pids_tracked=0,
  # #     @radix_tree_keys=1,
  # #     @radix_tree_nodes=2,
  # #     @recorded_first_entry_id="1780361273088-0")
  # ```
  def xinfo_stream(key : String)
    run({"xinfo", "stream", key})
  end

  def xinfo_stream_full(key : String, *, count : Int | String | Nil = nil)
    command = {"xinfo", "stream", key, "full"}
    command += {"count", count.to_s} if count

    run command
  end

  def xinfo_groups(key : String)
    run({"xinfo", "groups", key})
  end

  def xinfo_consumers(key : String, group : String)
    run({"xinfo", "consumers", key, group})
  end

  # Execute an `XREADGROUP` command on the Redis server.
  #
  # This is returned in its raw form from Redis, but you can pass it to a
  # `Redis::Streaming::XReadGroupResponse` to make it easier to work with.
  def xreadgroup(
    group : String,
    consumer : String,
    count : String | Int32 | Nil = nil,
    block : Time::Span | String | Int32 | Nil = nil,
    claim : Time::Span | String | Int32 | Nil = nil,
    no_ack = false,
    streams : ::Hash(String, String) = {} of String => String,
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
    case claim
    in Time::Span
      command << "claim" << claim.total_milliseconds.to_i.to_s
    in String
      command << "claim" << claim
    in Int
      command << "claim" << claim.to_s
    in Nil
      # No claiming, so we don't add it to the command
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
    count : String | Int32 | Nil = nil,
    block : Time::Span | String | Int32 | Nil = nil,
    claim : Time::Span | String | Int32 | Nil = nil,
    no_ack = false,
    streams : NamedTuple = NamedTuple.new,
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
    case claim
    in Time::Span
      command << "claim" << claim.total_milliseconds.to_i.to_s
    in String
      command << "claim" << claim
    in Int
      command << "claim" << claim.to_s
    in Nil
      # No claiming, so we don't add it to the command
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
    count : String | Int32,
    idle : String | Time::Span | Nil = nil,
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
    return 0i64 if ids.none?

    command = Array(String).new(initial_capacity: ids.size + 3)
    command << "xack" << key << group
    ids.each { |id| command << id }

    run command
  end

  def xackdel(key : String, group : String, ids : Enumerable(String))
    xackdel key, group,
      delete_mode: nil,
      ids: ids
  end

  def xackdel(key : String, group : String, delete_mode : DeleteMode?, ids : Enumerable(String))
    return [] of Redis::Value if ids.none?

    command = Array(String).new(initial_capacity: ids.size + 3)
    command << "xackdel" << key << group
    if delete_mode
      command << delete_mode.to_s
    end
    command << "ids" << ids.size.to_s
    command.concat ids

    run command
  end

  enum DeleteMode
    KEEPREF
    DELREF
    ACKED
  end

  def xautoclaim(
    key : String,
    group : String,
    consumer : String,
    min_idle_time : Time::Span,
    start : String,
    count : Int32 | String | Nil = nil,
  )
    min_idle_time = min_idle_time.total_milliseconds.to_i.to_s
    command = {"xautoclaim", key, group, consumer, min_idle_time, start}
    command += {"count", count.to_s} if count

    run command
  end

  def xnack(key : String, group : String, mode : NackMode, ids : Enumerable(String))
    return 0i64 if ids.none?

    command = Array(String).new(initial_capacity: ids.size + 4)
    command << "xnack" << key << group << mode.to_s
    command << "ids" << ids.size.to_s
    command.concat ids

    run command
  end

  enum NackMode
    SILENT
    FAIL
    FATAL
  end
end
