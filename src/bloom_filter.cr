module Redis
  # A Bloom Filter is a probabilistic data structure that allows testing whether
  # an item is a member of a set. It differs from a traditional set in that it
  # doesn't store all members, but instead stores a few bits that determine
  # whether it's likely to have been stored. This makes the Bloom Filter much
  # more space-efficient than a traditional set.
  #
  # ```
  # user_ids = Array.new(50) { UUID.v7 }
  #
  # # Add the same user ids to both the Bloom Filter and the set
  # redis.bf.madd "users-bf", user_ids.map(&.to_s)
  # redis.sadd "users-set", user_ids.map(&.to_s)
  #
  # # Both the Bloom Filter and the set have the same cardinality
  # redis.bf.card("users-bf") # => 50
  # redis.scard("users-set")  # => 50
  #
  # # The Bloom Filter uses 6x less memory
  # redis.dump("users-bf").as(String).bytesize  # => 206
  # redis.dump("users-set").as(String).bytesize # => 1199
  # ```
  #
  # NOTE: Bloom Filters are probabilistic, not a perfect representation of the members of the set. You also cannot see what's in the set, only test whether a string is likely to be in it and check its cardinality. If you need to be able to inspect the set or have 100% precision on whether a string is a member of a set, then a Bloom Filter is not the appropriate data structure.
  #
  # When checking whether a string is a member of a Bloom Filter, you are guaranteed never to get a false negative, but you may get false positives. That is, if `redis.bf.exists(key, item)` returns `0` then `item` is *definitely* not in the Bloom Filter, but if it returns `1` then it *probably* is, to within the `error_rate` specified in `reserve` or `insert`. The `error_rate` argument allows you to optimize for memory or precision. The smaller the error rate, the more memory your Bloom Filter will use.
  struct BloomFilter
    private getter redis : Commands

    def initialize(@redis)
    end

    # Create a Bloom Filter in `key` with the given `error_rate` and `capacity`. You can tune the `expansion` factor or prevent expansion entirely by setting `nonscaling: true`.
    #
    # ```
    # redis.bf.reserve "user:#{user_id}:products_ordered",
    #   error_rate: 0.01,
    #   capacity: 100
    # ```
    def reserve(
      key : String,
      error_rate : String | Float64,
      capacity : String | Int64,
      *,
      nonscaling : Bool = false,
      expansion : String | Int64 | Nil = nil,
    )
      command = {"bf.reserve", key, error_rate.to_s, capacity.to_s}
      command += {"nonscaling"} if nonscaling
      command += {"expansion", expansion} if expansion

      run command
    end

    # Insert `items` into the Bloom Filter stored in `key`, creating it if it doesn't already exist. The `capacity`, `error_rate`, `nonscaling`, and `expansion` arguments are the same as for `reserve`.
    #
    # ```
    # redis.bf.insert "user:#{user_id}:products_ordered", products.map(&.id),
    #   error: 0.01,
    #   capacity: 100
    # ```
    def insert(
      key : String,
      items : Array(String),
      *,
      capacity : String | Int64 = nil,
      error error_rate : String | Float64 | Nil = nil,
      nonscaling : Bool = false,
      expansion : String | Int64 | Nil = nil,
      nocreate : Bool = false,
    )
      options_size = {
        (capacity ? 2 : 0),
        (error_rate ? 2 : 0),
        (nonscaling ? 1 : 0),
        (expansion ? 2 : 0),
        (nocreate ? 1 : 0),
      }.sum

      command = Array(String).new(initial_capacity: 3 + items.size + options_size)
      command << "bf.insert" << key
      command << "capacity" << capacity.to_s if capacity
      command << "error" << error_rate.to_s if error_rate
      command << "expansion" << expansion if expansion
      command << "nocreate" if nocreate
      command << "nonscaling" if nonscaling
      command << "items"
      command.concat items
      run command
    end

    # Add `item` to the Bloom Filter stored in `key`.
    #
    # ```
    # redis.bf.add "user:#{user.id}:products_ordered", product.id
    # ```
    def add(key : String, item : String)
      run({"bf.add", key, item})
    end

    # Add `items` to the Bloom Filter stored in `key`.
    #
    # ```
    # redis.bf.madd "user:#{user.id}:products_ordered", products.map(&.id)
    # ```
    def madd(key : String, items : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + items.size)
      command << "bf.madd" << key
      command.concat items
      run command
    end

    # Returns `1` if `item` is a member of the Bloom Filter stored in `key`, `0` otherwise.
    #
    # ```
    # key = "user:#{user.id}:products_ordered"
    # redis.bf.exists(key, product.id) # => 0
    # redis.bf.add key, product.id     # => 1
    # redis.bf.exists(key, product.id) # => 1
    # ```
    def exists(key : String, item : String)
      run({"bf.exists", key, item})
    end

    # Returns an array corresponding to each member of `items` containing `1` if
    # the item was added or `0` if it was not.
    #
    # ```
    # redis.bf.madd key, %w[one two]
    # redis.bf.mexists(key, %w[one two three]) # => [1, 1, 0]
    # ```
    def mexists(key : String, items : Enumerable(String))
      command = Array(String).new(initial_capacity: 2 + items.size)
      command << "bf.mexists" << key
      command.concat items

      run command
    end

    # Returns the number of items added to the Bloom Filter that have been
    # detected as unique.
    #
    # ```
    # key = "user:#{user.id}:products_ordered"
    # redis.bf.card(key)           # => 0
    # redis.bf.add key, product.id # => 1
    # redis.bf.card(key)           # => 1
    # ```
    def card(key : String)
      run({"bf.card", key})
    end

    # Returns an array (convertible to a hash via `Redis.to_hash`) containing information about the Bloom Filter stored in `key`.
    #
    # ```
    # redis.bf.reserve(key, error_rate: 0.01, capacity: 10)
    # redis.bf.info(key)
    # # => ["Capacity",
    # #     10,
    # #     "Size",
    # #     112,
    # #     "Number of filters",
    # #     1,
    # #     "Number of items inserted",
    # #     0,
    # #     "Expansion rate",
    # #     2]
    # ```
    def info(key : String)
      redis.run({"bf.info", key})
    end

    private def run(command)
      @redis.run command
    end
  end

  module Commands
    # Return a `BloomFilter`, allowing you to run commands for Bloom Filters.
    #
    # ```
    # redis.bf.add "my-bloom-filter", "value"
    # ```
    def bf
      BloomFilter.new(self)
    end
  end
end
