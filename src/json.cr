require "json"

require "./redis"

module Redis
  # `Redis::JSON` wraps a `Redis::Client` or `Redis::Cluster` to execute
  # commands against keys storing JSON data using the
  # [RedisJSON](https://oss.redis.com/redisjson/) plugin for Redis.
  #
  # ```
  # require "redis"
  # require "redis/json"
  # require "uuid/json"
  #
  # struct Person
  #   include JSON::Serializable
  #
  #   getter id : UUID
  #   getter name : String
  #   getter email : String
  #
  #   def initialize(@id, @name, @email)
  #   end
  # end
  #
  # redis = Redis::Client.new
  #
  # id = UUID.random
  #
  # redis.json.set "person:#{id}", ".", Person.new(id, "Jamie", "jamie@example.com")
  # redis.json.get("person:#{id}", ".", as: Person)
  # # => Person(@email="jamie@example.com", @id=UUID(081af617-6b50-48d3-b53f-dbc17a1072f5), @name="Jamie")
  # redis.json.get("person:#{id}", ".name", as: String)
  # # => "Jamie"
  # ```
  struct JSON(Runnable)
    def initialize(@redis : Runnable)
    end

    # Set the given key to the given JSON `value`. The `value` will have
    # `to_json` called on it automatically. The `nx` and `xx` arguments are
    # identical to `Redis::Commands#set`.
    #
    # Store a user at the root JSONPath (`.`) of the specified key:
    #
    # ```
    # struct User
    #   include JSON::Serializable
    #
    #   getter id : UUID
    #   getter name : String
    #   getter email : String
    # end
    #
    # redis.json.set "user:#{user.id}", ".", user
    # ```
    #
    # Update (with `xx` so we ensure the key exists) a user's email by setting
    # the new email value at the `.email` JSONPath:
    #
    # ```
    # redis.json.set "user:#{id}", ".email", new_email, xx: true
    # ```
    def set(key : String, path : String, value, *, nx = false, xx = false) : Nil
      command = {"json.set", key, path, value.to_json}
      command += {"nx"} if nx
      command += {"xx"} if xx

      @redis.run command
    end

    # Get the raw JSON string at the specified `key`
    def get(key : String)
      @redis.run({"json.get", key})
    end

    # Get the raw JSON string representation at the given `key` nested at the
    # given JSONPath.
    #
    # ```
    # email = redis.json.get("user:#{id}", ".email").as(String)
    # # => "me@example.com"
    # ```
    def get(key : String, path : String)
      @redis.run({"json.get", key, path})
    end

    # Get the values stored at the specifed JSONPaths, deserializing them into
    # the specified type as values in a `Hash`.
    #
    # ```
    # if result = redis.json.get("posts:#{id}", %w[title tags], as: String | Array(String))
    #   result         # => {"title" => "JSON with Redis", tags: ["redis", "json", "crystal"]}
    #   typeof(result) # => Hash(String, String | Array(String))
    # end
    # ```
    #
    # NOTE: This method _cannot_ be used in a pipeline or `Redis::Connection#multi`
    # block because it operates on the value returned from the Redis server,
    # which isn't a thing when using pipelines or `multi`.
    def get(key : String, paths : Array(String), as type : T.class = ::JSON::Any) forall T
      if result = @redis.run(["json.get", key] + paths)
        Hash(String, T).from_json result.as(String)
      end
    end

    # Get the value stored at the given JSONPath inside the given `key`,
    # deserializing the value into the type `T`. You can use this to deserialize
    # your custom `JSON::Serializable` objects.
    #
    # ```
    # struct Order
    #   include JSON::Serializable
    #   getter id : UUID
    #   getter customer_id : UUID
    #   getter line_items : Array(LineItem)
    # end
    #
    # struct LineItem
    #   include JSON::Serializable
    #   getter product_id : UUID
    #   getter quantity : Int32
    #   getter unit_price_cents : Int32
    # end
    #
    # # Get the Order at the root
    # redis.json.get("order:#{id}", ".", as: Order)
    # # Get the LineItems at the `.line_items` JSONPath
    # # Note the `$` at the beginning to make it an array
    # redis.json.get("order:#{id}", "$.line_items", as: Array(LineItem))
    # # Get only the `product_id` properties of all the `line_items`
    # redis.json.get("order:#{id}", "$.line_items..product_id", as: Array(UUID))
    # ```
    #
    # NOTE: This method _cannot_ be used in a pipeline or `Redis::Connection#multi`
    # block because it operates on the value returned from the Redis server
    # inside the method, which isn't possible when using pipelines or `multi`.
    def get(key : String, path : String, as type : T.class) : T? forall T
      if result = @redis.run({"json.get", key, path})
        T.from_json result.as(String)
      end
    end

    # Get the values for all `keys` at the specified JSONPaths.
    #
    # NOTE: This method _cannot_ be used in a pipeline or `Redis::Connection#multi`
    # block because it operates on the value returned from the Redis server
    # inside the method, which isn't possible when using pipelines or `multi`.
    # NOTE: If you are using a cluster, the `keys` _must_ all be on the same
    # shard, or you may only get some of them back with this method. When using
    # `mget` in a cluster, you'll most likely want to use subhashed keys (with
    # `{}` around the same part of the name) to ensure all keys are on the same
    # shard.
    def mget(keys : Array(String), path : String, as type : T.class) : Array(T?) forall T
      if result = @redis.run(["json.mget"] + keys + [path])
        result.as(Array).map do |value|
          if value
            T.from_json(value.as(String))
          end
        end
      else
        raise "lolwut?"
      end
    end

    # Increment the number at the specified JSONPath
    #
    # ```
    # redis.json.numincrby "product:#{id}", ".purchase_count", 1
    # ```
    def numincrby(key : String, path : String, count : String | Int)
      @redis.run({"json.numincrby", key, path, count.to_s})
    end

    # Increment the number at the specified JSONPath
    #
    # ```
    # redis.json.numincrby "product:#{id}", ".purchase_count", 1, as: Int64
    # # => 2
    # ```
    #
    # If `key` exists and an incrementable number exists at the specified
    # JSONPath (including the possibility of incrementing multiple numbers if
    # the JSONPath resolves to multiple numeric values), then this method
    # returns a value of type `T`. If the JSONPath resolves to multiple values
    # (for example, it begins with "$" or is recursive), you will need to
    # specify that it can be an `Array` of that type.
    def numincrby(key : String, path : String, count : String | Int, as type : T.class) : T forall T
      T.from_json(numincrby(key, path, count).as(String))
    end
  end

  module Commands
    # Return a `Redis::JSON` instance that wraps the current `Redis::Client` or
    # `Redis::Cluster`.
    @[Experimental("Support for the RedisJSON module is still under development and subject to change.")]
    def json
      JSON.new(self)
    end
  end
end
