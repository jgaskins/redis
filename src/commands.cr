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
    # redis.keys # => ["foo", "bar", "baz"]
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
    def set(key : String, value : String, ex = nil, px = nil, nx = false, xx = false, keepttl = false) : Nil
      command = {"set", key, value}
      command += {"ex", ex.to_s} if ex
      command += {"px", px.to_s} if px
      command += {"nx"} if nx
      command += {"xx"} if xx
      command += {"keepttl"} if keepttl

      run command
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
    # redis.del "my-list" # Delete so we know it's empty
    # redis.lpush "my-list", "foo", "bar" # => 2
    # redis.lpush "my-list", "foo", "bar" # => 4
    # ```
    def lpush(key, *values)
      run({"lpush", key} + values)
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
    def brpop(*keys : String, timeout : Int | Float)
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
      command = Array(Value).new(initial_capacity: data.size * 2 + 5)
      command << "xadd" << key
      command << "maxlen" << maxlen if maxlen
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
    # TODO: Make the return value of this command easier to work with. Nested
    # heterogeneous arrays aren't easy to work with.
    def xreadgroup(
      group : String,
      consumer : String,
      count : String | Int | Nil = nil,
      block : Time::Span | String | Int | Nil = nil,
      no_ack = false,
      streams : NamedTuple = NamedTuple.new,
    )
      command = Array(Value).new(initial_capacity: 9 + streams.size * 2)
      command << "xreadgroup" << "group" << group << consumer
      command << "count" << count if count
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
      streams.each_key do |key|
        # Symbol#to_s does not allocate a string on the heap, so the only
        # allocation in this method is the array.
        command << key.to_s
      end
      streams.each_value do |value|
        command << value
      end

      run command
    end
  end
end
