require "../commands"

# Immediate objects are ones that execute commands on the server and return
# their results immediately. This allows the caller to know that it can
# go to work on the results directly. The counterpart to this is the `Deferred`
# type.
#
# ```
# client = Redis::Client.new
# p! typeof(client.get("foo")) # => (String | Nil)
# client.pipeline do |pipeline|
#   p! typeof(pipeline.get("foo")) # => Redis::Future
# end
# client.multi do |txn|
#   p! typeof(txn.get("foo")) # => Nil
# end
# ```
#
# Objects that include this mixin must return a `Redis::Value` from their `run`
# method. This mixin overrides various Redis command methods to downcast the
# return types to only the types that the Redis server is known to return.
module Redis::Commands::Immediate
  # :nodoc:
  macro override_return_types(methods)
    {% for method, return_type in methods %}
      {% for methods in [Commands, Commands::Hash, Commands::List, Commands::Set, Commands::SortedSet, Commands::Stream, Commands::Geo].map(&.methods.select { |m| m.name == method }).reject(&.nil?) %}
        {% for m in methods %}
          # :nodoc:
          def {{method.id}}(
            {% for arg, index in m.args %}
              {% if m.splat_index == index %}*{% end %}{{arg}},
            {% end %}
          ) : {{return_type}}
            super{{".as(#{return_type})".id unless return_type.stringify == "Nil"}}
          end
        {% end %}
      {% end %}
    {% end %}
  end

  macro set_return_types!
    # When new commands are added to the Commands mixin, add an entry here to
    # make sure the return type is set when run directly on the connection.
    override_return_types({
      keys:        Array,
      dbsize:      Int64,
      del:         Int64,
      unlink:      Int64,
      ttl:         Int64,
      pttl:        Int64,
      script_load: String,

      # String commands
      append:      Int64,
      decr:        Int64,
      decrby:      Int64,
      get:         String?,
      getdel:      String?,
      getex:       String?,
      getrange:    String,
      getset:      String?,
      incr:        Int64,
      incrby:      Int64,
      incrbyfloat: String,
      mget:        Array,
      mset:        String,
      msetnx:      Int64,
      psetex:      String,
      set:         String?,
      setex:       String,
      setnx:       Int64,
      setrange:    Int64,
      strlen:      Int64,
      substr:      String,

      # List commands
      blmove:     String?,
      blmpop:     Array(Value)?,
      blpop:      Array(Value)?,
      brpop:      Array(Value)?,
      brpoplpush: String?,
      lindex:     String?,
      linsert:    Int64?,
      llen:       Int64,
      lmove:      String?,
      lmpop:      Array(Value)?,
      lpop:       String | Array(Value) | Nil,
      lpos:       Int64 | Array(Value) | Nil,
      lpush:      Int64,
      lpushx:     Int64,
      lrange:     Array,
      lrem:       Int64,
      lset:       String,
      ltrim:      String,
      rpop:       String?,
      rpoplpush:  String?,
      rpush:      Int64,
      rpushx:     Int64,

      # Hash commands
      hdel:         Int64,
      hexists:      Int64,
      hget:         String?,
      hgetall:      Array,
      hincrby:      Int64,
      hincrbyfloat: String,
      hkeys:        Array,
      hlen:         Int64,
      hmget:        Array,
      hmset:        String,
      hrandfield:   String | Array(Value),
      hscan:        Array,
      hset:         Int64,
      hsetnx:       Int64,
      hstrlen:      Int64,
      hvals:        Array,

      # Sets
      sadd:        Int64,
      scard:       Int64,
      sdiff:       Array,
      sdiffstore:  Int64,
      sinter:      Array,
      sintercard:  Int64,
      sinterstore: Int64,
      sismember:   Int64,
      smembers:    Array,
      smismember:  Array,
      smove:       Int64,
      spop:        String | Array(Value) | Nil,
      srandmember: String | Array(Value) | Nil,
      srem:        Int64,
      sscan:       Array,
      sunion:      Array,
      sunionstore: Int64,

      # Sorted Sets
      bzmpop:           Array?,
      bzpopmax:         Array,
      bzpopmin:         Array,
      zadd:             Int64,
      zcard:            Int64,
      zcount:           Int64,
      zdiff:            Array,
      zdiffstore:       In64,
      zincrby:          String,
      zinter:           Array,
      zintercard:       Int64,
      zinterstore:      Int64,
      zlexcount:        Int64,
      zmpop:            Array?,
      zmscore:          Array,
      zpopmax:          Array,
      zpopmin:          Array,
      zrandmember:      Array | String | Nil,
      zrange:           Array,
      zrangebylex:      Array,
      zrangebyscore:    Array,
      zrangestore:      Int64,
      zrank:            Int64?,
      zrem:             Int64,
      zremrangebylex:   Int64,
      zremrangebyscore: Int64,
      zremrangebyrank:  Int64,
      zrevrange:        Array,
      zrevrangebylex:   Array,
      zrevrangebyscore: Array,
      zrevrank:         Int64?,
      zscan:            Array,
      zscore:           String?,
      zunion:           Array,
      zunionstore:      Int64,

      # Streams
      xack:       Int64,
      xadd:       String?,
      xautoclaim: Array,
      xclaim:     Array,
      xdel:       Int64,
      xgroup:     String | Int64,
      xinfo:      Array,
      xlen:       Int64,
      xpending:   Array,
      xrange:     Array,
      xreadgroup: Array(Value)?,
      xrevrange:  Array,
      xtrim:      Int64,

      geopos:    Array,
      geodist:   String,
      geosearch: Array,
    })
  end

  set_return_types!
end
