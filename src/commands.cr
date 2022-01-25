require "./value"

module Redis
  # All Redis commands are defined in this module. Any paradigm that needs to
  # use these commands simply overrides `run`, which takes a single `command`
  # object, which must be an `Enumerable`.
  #
  # TODO: Add more Redis commands from https://redis.io/commands
  module Commands
    # Execute the given command and return the result from the server. Commands
    # must be an `Enumerable` and its `size` method must be re-entrant.
    #
    # ```
    # run({"set", "foo", "bar"})
    # ```
    abstract def run(command)

    # Get the keys whose names follow the specified glob pattern. If a pattern
    # is not specified, it will return all keys by default. Be careful when
    # using this command on Redis servers with a lot of traffic and millions
    # of keys.
    #
    # ```
    # redis.keys       # => ["foo", "bar", "baz"]
    # redis.keys("f*") # => ["foo"]
    # redis.keys("b*") # => ["bar", "baz"]
    # ```
    def keys(pattern = "*")
      run({"keys", pattern})
    end

    # Set a given key to a given value, optionally specifying time-to-live (TTL).
    #
    # - `ex`: TTL in seconds (mnemonic: "ex" = "expiration")
    # - `px`: TTL in milliseconds
    # - `nx`: Only set this key if it does not exist (mnemonic: "nx" = it does "not exist")
    # - `xx`: only set this key if it does exist (mnemonic: "xx" = it "exists exists" — look, I don't make the rules)
    # - `keepttl`: If there is a TTL already set on the key, retain that TTL instead of overwriting it
    #
    # ```
    # redis.set "foo", "bar", ex: 1
    # redis.get("foo") # => "bar"
    # sleep 1.second
    # redis.get("foo") # => nil
    # ```
    def set(key : String, value : String, ex : (String | Int)? = nil, px : String | Int | Nil = nil, nx = false, xx = false, keepttl = false)
      command = {"set", key, value}
      command += {"ex", ex.to_s} if ex
      command += {"px", px.to_s} if px
      command += {"nx"} if nx
      command += {"xx"} if xx
      command += {"keepttl"} if keepttl

      run command
    end

    def set(key, value, ex : Time, nx = false, xx = false, keepttl = false)
      set key, value, ex: ex - Time.utc, nx: nx, xx: xx, keepttl: keepttl
    end

    def set(key, value, ex : Time::Span, nx = false, xx = false, keepttl = false)
      set key, value, px: ex.total_milliseconds.to_i64, nx: nx, xx: xx, keepttl: keepttl
    end

    # Get the value for the specified key
    #
    # ```
    # redis.set "foo", "bar"
    # redis.get("foo") # => "bar"
    # ```
    def get(key : String)
      run({"get", key})
    end

    # Atomically increment and return the integer value for the specified key,
    # creating it if it does not exist
    #
    # ```
    # redis.del "counter"
    # redis.incr "counter" # => 1
    # ```
    def incr(key : String)
      run({"incr", key})
    end

    # Atomically decrement and return the integer value for the specified key,
    # creating it if it does not exist
    #
    # ```
    # redis.del "counter"
    # redis.decr "counter" # => -1
    # ```
    def decr(key : String)
      run({"decr", key})
    end

    # Atomically increment and return the integer value for the specified key by
    # the specified amount, creating it if it does not exist
    #
    # ```
    # redis.del "counter"
    # redis.incrby "counter", 2 # => 2
    # ```
    def incrby(key : String, amount : Int | String)
      run({"incrby", key, amount.to_s})
    end

    # Atomically decrement and return the integer value for the specified key by
    # the specified amount, creating it if it does not exist
    #
    # ```
    # redis.del "counter"
    # redis.decrby "counter", 2 # => -2
    # ```
    def decrby(key : String, amount : Int | String)
      run({"decrby", key, amount.to_s})
    end

    # Delete all specified keys and return the number of keys deleted.
    #
    # ```
    # redis.set "foo", "12"
    # redis.del "foo", "bar" # => 1
    # redis.del "foo", "bar" # => 0
    # ```
    def del(*keys : String)
      run({"del"} + keys)
    end

    def unlink(*keys : String)
      run({"unlink"} + keys)
    end

    # Return the number of specified keys that exist
    #
    # ```
    # redis.exists("foo", "bar") # => 0
    # redis.set "foo", "exists now"
    # redis.exists("foo", "bar") # => 1
    # redis.set "bar", "also exists now"
    # redis.exists("foo", "bar") # => 2
    # ```
    def exists(*keys : String)
      run({"exists"} + keys)
    end

    # Insert an item at the beginning of a list, returning the number of items
    # in the list after the insert.
    #
    # ```
    # redis.del "my-list"                 # Delete so we know it's empty
    # redis.lpush "my-list", "foo", "bar" # => 2
    # redis.lpush "my-list", "foo", "bar" # => 4
    # ```
    def lpush(key, *values : String)
      run({"lpush", key} + values)
    end

    # Remove an item from the beginning of a list, returning the item or `nil`
    # if the list was empty.
    #
    # ```
    # redis.del "my-list" # Delete so we know it's empty
    # redis.lpush "my-list", "foo"
    # redis.lpop "my-list" # => "foo"
    # redis.lpop "my-list" # => nil
    # ```
    def lpop(key : String, count : String? = nil)
      command = {"lpop", key}
      command += {count} if count

      run(command)
    end

    def lrange(key : String, start : String, finish : String)
      run({"lrange", key, start, finish})
    end

    # Atomically remove an item from the end of a list and insert it at the
    # beginning of another. Returns that list item. If the first list is empty,
    # nothing happens and this method returns `nil`.
    #
    # ```
    # redis.del "foo"
    # redis.lpush "foo", "hello", "world"
    # redis.rpoplpush "foo", "bar" # => "hello"
    # redis.rpoplpush "foo", "bar" # => "world"
    # redis.rpoplpush "foo", "bar" # => nil
    # ```
    def rpoplpush(source : String, destination : String)
      run({"rpoplpush", source, destination})
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method returns `nil`
    #
    # ```
    # redis.lpush "foo", "hello"
    # redis.rpop "foo" # => "hello"
    # redis.rpop "foo" # => nil
    # ```
    def rpop(key : String)
      run({"rpop", key})
    end

    # Insert an item at the end of a list, returning the number of items
    # in the list after the insert.
    #
    # ```
    # redis.del "my-list"                 # Delete so we know it's empty
    # redis.rpush "my-list", "foo", "bar" # => 2
    # redis.rpush "my-list", "foo", "bar" # => 4
    # ```
    def rpush(key, *values : String)
      run({"rpush", key} + values)
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified amount
    # of time for an element to be added to it by another connection. If the
    # element *is* added by another connection within that amount of time, this
    # method will return it immediately. If it *is not*, then this method returns
    # `nil`.
    #
    # ```
    # redis.rpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.rpush "foo", "second"
    # end
    # redis.blpop "foo", 1.second # => "first"
    # redis.blpop "foo", 1.second # => "second" (after 100 milliseconds)
    # redis.blpop "foo", 1.second # => nil (after 1 second)
    # ```
    def blpop(*keys : String, timeout : Time::Span)
      blpop(*keys, timeout: timeout.total_seconds.to_i.to_s)
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified number
    # of seconds for an element to be added to it by another connection. If the
    # element *is* added by another connection within that number of seconds,
    # this method will return it immediately. If it *is not*, then this method
    # returns `nil`.
    #
    # ```
    # redis.lpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.lpush "foo", "second"
    # end
    # redis.blpop "foo", 1 # => "first"
    # redis.blpop "foo", 1 # => "second" (after 100 milliseconds)
    # redis.blpop "foo", 1 # => nil (after 1 second)
    # ```
    def blpop(*keys : String, timeout : Int | Float)
      timeout = timeout.to_i if timeout == timeout.to_i
      blpop(*keys, timeout: timeout.to_s)
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified number
    # of seconds for an element to be added to it by another connection. If the
    # element *is* added by another connection within that number of seconds,
    # this method will return it immediately. If it *is not*, then this method
    # returns `nil`.
    #
    # ```
    # redis.lpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.lpush "foo", "second"
    # end
    # redis.blpop "foo", "1" # => "first"
    # redis.blpop "foo", "1" # => "second" (after 100 milliseconds)
    # redis.blpop "foo", "1" # => nil (after 1 second)
    # ```
    def blpop(*keys : String, timeout : String)
      run({"blpop"} + keys + {timeout})
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified amount
    # of time for an element to be added to it by another connection. If the
    # element *is* added by another connection within that amount of time, this
    # method will return it immediately. If it *is not*, then this method returns
    # `nil`.
    #
    # ```
    # redis.lpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.lpush "foo", "second"
    # end
    # redis.brpop "foo", 1.second # => "first"
    # redis.brpop "foo", 1.second # => "second" (after 100 milliseconds)
    # redis.brpop "foo", 1.second # => nil (after 1 second)
    # ```
    def brpop(*keys : String, timeout : Time::Span)
      brpop(*keys, timeout: timeout.total_seconds)
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified number
    # of seconds for an element to be added to it by another connection. If the
    # element *is* added by another connection within that number of seconds,
    # this method will return it immediately. If it *is not*, then this method
    # returns `nil`.
    #
    # ```
    # redis.lpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.lpush "foo", "second"
    # end
    # redis.brpop "foo", 1 # => "first"
    # redis.brpop "foo", 1 # => "second" (after 100 milliseconds)
    # redis.brpop "foo", 1 # => nil (after 1 second)
    # ```
    def brpop(*keys : String, timeout : Number)
      timeout = timeout.to_i if timeout == timeout.to_i
      brpop(*keys, timeout: timeout.to_s)
    end

    # Remove and return an element from the end of the given list. If the list
    # is empty or the key does not exist, this method waits the specified number
    # of seconds for an element to be added to it by another connection. If the
    # element *is* added by another connection within that number of seconds,
    # this method will return it immediately. If it *is not*, then this method
    # returns `nil`.
    #
    # ```
    # redis.lpush "foo", "first"
    # spawn do
    #   sleep 100.milliseconds
    #   redis.lpush "foo", "second"
    # end
    # redis.brpop "foo", "1" # => "first"
    # redis.brpop "foo", "1" # => "second" (after 100 milliseconds)
    # redis.brpop "foo", "1" # => nil (after 1 second)
    # ```
    def brpop(*keys : String, timeout : String)
      run({"brpop"} + keys + {timeout})
    end

    def sadd(key : String, *values : String)
      run({"sadd", key} + values)
    end

    def sismember(key : String, value : String)
      run({"sismember", key, value})
    end

    def smembers(key : String)
      run({"smembers", key})
    end

    def srem(key : String, *values : String)
      run({"srem", key} + values)
    end

    def sdiff(first : String, second : String)
      run({"sdiff", first, second})
    end

    def sinter(first : String, *others : String)
      run({"sinter", first} + others)
    end

    def scard(key : String)
      run({"scard", key})
    end

    def scan(cursor : String = "0", match : String? = nil, count : String | Int | Nil = nil, type : String? = nil)
      # SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
      command = {"scan", cursor}
      command += {"match", match} if match
      command += {"count", count.to_s} if count
      command += {"type", type} if type

      run command
    end

    def publish(channel : String, message : String)
      run({"publish", channel, message})
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
    # redis.xadd "my-stream", "*", name: "foo", id: UUID.random.to_s
    # ```
    def xadd(key : String, id : String, maxlen = nil, **data)
      command = Array(Value).new(initial_capacity: data.size * 2 + 6)
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
    def xadd(key : String, id : String, data : Hash(String, String))
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
    def xadd(key : String, id : String, maxlen, data : Hash(String, String))
      command = Array(Value).new(initial_capacity: data.size * 2 + 3)
      command << "xadd" << key
      command << "maxlen" << maxlen if maxlen
      command << id
      data.each do |key, value|
        command << key << value
      end

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
      streams : Hash(String, String) = {} of String => String
    )
      command = Array(Value).new(initial_capacity: 9 + streams.size * 2)
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
      command = Array(Value).new(initial_capacity: 9 + streams.size * 2)
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

    def hget(key : String, field : String)
      run({"hget", key, field})
    end

    def hgetall(key : String)
      run({"hgetall", key})
    end

    def hmget(key : String, *fields : String)
      run({"hmget", key} + fields)
    end

    def hset(key : String, field : String, value : String)
      run({"hset", key, field, value})
    end

    def hmset(key : String, data : Hash(String, String))
      command = Array(String).new(initial_capacity: 2 + data.size)

      command << "hmset" << key
      data.each do |key, value|
        command << key << value
      end

      run command
    end

    def llen(key : String)
      run({"llen", key})
    end

    def lrange(key : String, starting : String, ending : String)
      run({"lrange", key, starting, ending})
    end

    def srem(key : String, members : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + members.size)
      command << "srem" << key
      members.each do |member|
        command << member
      end

      run command
    end

    def zcard(key : String)
      run({"zcard", key})
    end

    def expire(key : String, ttl : Int)
      run({"expire", key, ttl.to_s})
    end

    def pexpire(key : String, ttl : Int)
      run({"pexpire", key, ttl.to_s})
    end

    def expireat(key : String, at : Time)
      run({"expireat", key, at.to_unix.to_s})
    end

    def pexpireAt(key : String, at : Time)
      run({"pexpireat", key, at.to_unix_ms.to_s})
    end

    def ttl(key : String)
      run({"ttl", key})
    end

    def pttl(key : String)
      run({"pttl", key})
    end

    def type(key : String)
      run({"type", key})
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

    def lrem(key : String, count : Int, value : String)
      run({"lrem", key, count.to_s, value})
    end

    def lpush(key : String, values : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + values.size)
      command << "lpush" << key
      values.each { |value| command << value }

      run command
    end

    def brpop(keys : Enumerable(String), timeout : Int)
      command = Array(String).new(initial_capacity: 2 + keys.size)
      command << "brpop"
      keys.each do |key|
        command << key
      end

      command << timeout.to_s

      run command
    end

    def mget(keys : Enumerable(String))
      command = Array(String).new(initial_capacity: 1 + keys.size)
      command << "mget"
      keys.each { |key| command << key }

      run command
    end

    def mset(data : Hash(String, String))
      command = Array(String).new(initial_capacity: 1 + data.size)
      command << "mset"
      data.each do |key, value|
        command << key << value
      end

      run command
    end

    def flushdb
      run({"flushdb"})
    end

    def info
      run({"info"})
        .as(String)
        .lines
        .reject { |line| line =~ /^(#|$)/ }
        .map(&.split(':', 2))
        .to_h
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
end
