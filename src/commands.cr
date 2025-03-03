require "./value"
require "./commands/hash"
require "./commands/list"
require "./commands/set"
require "./commands/sorted_set"
require "./commands/stream"
require "./commands/geo"

module Redis
  # All Redis commands are defined in this module. Any paradigm that needs to
  # use these commands simply overrides `run`, which takes a single `command`
  # object, which must be an `Enumerable`.
  #
  # TODO: Add more Redis commands from https://redis.io/commands
  module Commands
    include Commands::Hash
    include Commands::List
    include Commands::Set
    include Commands::SortedSet
    include Commands::Stream
    include Commands::Geo

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

    # Delete all specified keys and return the number of keys deleted.
    #
    # ```
    # redis.set "foo", "12"
    # redis.del ["foo", "bar"] # => 1
    # redis.del ["foo", "bar"] # => 0
    # ```
    def del(keys : Enumerable(String))
      command = Array(String).new(initial_capacity: 1 + keys.size)
      command << "del"
      command.concat keys

      run command
    end

    def unlink(*keys : String)
      run({"unlink"} + keys)
    end

    def unlink(keys : Enumerable(String))
      command = Array(String).new(initial_capacity: 1 + keys.size)
      command << "unlink"
      command.concat keys

      run command
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

    # Return the number of specified keys that exist
    #
    # ```
    # redis.exists(%w[foo bar]) # => 0
    # redis.set "foo", "exists now"
    # redis.exists(%w[foo bar]) # => 1
    # redis.set "bar", "also exists now"
    # redis.exists(%w[foo bar]) # => 2
    # ```
    def exists(keys : Enumerable(String))
      command = Array(String).new(initial_capacity: 1 + keys.size)
      command << "exists"
      command.concat keys

      run command
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

    # Preload a Lua script, returning the SHA of the script to pass to `evalsha`.
    # ```
    # sha = redis.script_load(<<-LUA)
    #   return "Hello " + ARGV[1]
    # LUA
    #
    # redis.evalsha(sha, args: ["world"]) # => "Hello world"
    # ```
    def script_load(script : String)
      run({"script", "load", script})
    end

    # Flush the Lua scripts cache, see [the official docs](https://redis.io/commands/script-flush/).
    #
    # ```
    # redis.script_flush :async # Flush scripts asynchronously
    # redis.script_flush :sync  # Flush scripts immediately
    # ```
    def script_flush(mode : ScriptFlushMode)
      run({"script", "flush", mode.to_s})
    end

    enum ScriptFlushMode
      ASYNC
      SYNC
    end

    # Return an array where each entry is `1` if the corresponding entry in the
    # list of `shas` exists or `0` if it does not.
    def script_exists(*shas : String)
      script_exists shas
    end

    # :ditto:
    def script_exists(shas : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + keys.size)
      command << "script" << "exists"
      shas.each { |sha| command << sha }

      run command
    end

    # Kill the currently executing `eval` script, assuming no write operation
    # was yet performed by the script.
    def script_kill
      run({"script", "kill"})
    end

    # Shorthand for defining all of the EVAL* commands since they're all pretty
    # much identical.
    private macro define_eval(command, arg_name)
      # Evaluate the given Lua script, either referenced by SHA with `evalsha`
      # or directly with `eval`.
      #
      # NOTE: Use `eval` only for very trivial scripts and `evalsha` for larger
      # or frequently executed scripts to amortize parse/compile time as well as
      # send fewer bytes along the wire.
      #
      # NOTE: Use `eval_ro` and `evalsha_ro` in a clustered environment to
      # evaluate the scripts on read-only replicas.
      #
      # ```
      # script = <<-LUA
      #   return "this script was " + ARGV[1]
      # LUA
      #
      # sha = redis.script_load(script)
      # redis.eval(script, args: ["evaluated on the fly"]
      # redis.evalsha(sha, args: ["precompiled"])
      # ```
      def {{command.id}}({{arg_name.id}} : String, keys : Enumerable(String) = EmptyEnumerable.new, args : Enumerable(String) = EmptyEnumerable.new)
        command = Array(String).new(initial_capacity: 3 + keys.size + args.size)
        command << "{{command.id}}" << {{arg_name.id}} << keys.size.to_s
        keys.each { |key| command << key }
        args.each { |arg| command << arg }

        run command
      end
    end

    define_eval evalsha, sha
    define_eval evalsha_ro, script
    define_eval eval, script
    define_eval eval_ro, script

    # This type exists to avoid allocation of an array on the heap.
    struct EmptyEnumerable
      include Enumerable(String)

      def each(&block : String ->)
      end

      def size
        0
      end
    end

    def expire(key : String, ttl : Time::Span)
      expire key, ttl.total_seconds.to_i64
    end

    def expire(key : String, ttl : Int)
      run({"expire", key, ttl.to_s})
    end

    def pexpire(key : String, ttl : Time::Span)
      pexpire key, ttl.total_milliseconds.to_i64
    end

    def pexpire(key : String, ttl : Int)
      run({"pexpire", key, ttl.to_s})
    end

    def expireat(key : String, at : Time)
      run({"expireat", key, at.to_unix.to_s})
    end

    def pexpireat(key : String, at : Time)
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

    # Delete all the keys of the currently selected DB
    def flushdb
      run({"flushdb"})
    end

    # Delete all the keys of all the existing databases,
    # not just the currently selected one.
    def flushall
      run({"flushall"})
    end

    def info
      run({"info"})
        .as(String)
        .lines
        .reject { |line| line =~ /^(#|$)/ }
        .map(&.split(':', 2))
        .to_h
    end

    def wait(numreplicas replica_count : Int | String, timeout : Time::Span)
      wait replica_count, timeout.total_seconds.ceil.to_i
    end

    def wait(numreplicas replica_count : Int | String, timeout : Int | String)
      run({"wait", replica_count.to_s, timeout.to_s})
    end
  end
end
