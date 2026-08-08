require "./commands"

module Redis
  struct TopK
    private getter redis : Commands

    def initialize(@redis)
    end

    def reserve(key : String, topk : Int | String)
      redis.run({"topk.reserve", key, topk.to_s})
    end

    def reserve(
      key : String,
      topk : Int | String,
      width : Int | String,
      depth : Int | String,
      decay : Float | String,
    )
      redis.run({"topk.reserve", key, topk.to_s, width.to_s, depth.to_s, decay.to_s})
    end

    def info(key : String)
      redis.run({"topk.info", key})
    end

    def add(key : String, *items : String)
      redis.run({"topk.add", key} + items)
    end

    def add(key : String, items : Enumerable(String))
      command = Array(String).new(items.size + 2)
      command << "topk.add" << key
      command.concat items

      redis.run command
    end

    def list(key : String, *, withcount = false)
      command = {"topk.list", key}
      command += {"withcount"} if withcount

      redis.run command
    end

    def query(key : String, *items : String)
      redis.run({"topk.query", key} + items)
    end

    def query(key : String, items : Enumerable(String))
      command = Array(String).new(items.size + 2)
      command << "topk.query" << key
      command.concat items

      redis.run command
    end

    def incrby(key : String, item : String, amount : Int | String)
      redis.run({"topk.incrby", key, item, amount.to_s})
    end

    struct Immediate
      private getter topk : TopK

      def initialize(@topk)
      end

      private macro cast(**methods)
        {% for method, type in methods %}
          # Executes `TopK#{{method.id}}` and casts down to `{{type.id}}`.
          def {{method.id}}(*args, **kwargs)
            topk.{{method.id}}(*args, **kwargs).as({{type}})
          end
        {% end %}
      end

      cast(
        reserve: String,
        info: Array,
        add: Array,
        list: Array,
        query: Array,
        incrby: Array,
      )
    end
  end

  module Commands::Deferred
    # Returns a `TopK` to allow running [`TOPK.*` commands](https://redis.io/docs/latest/commands/?group=topk)
    # in Redis.
    #
    # ```
    # require "redis/topk"
    #
    # # Create a TopK data structure with a large width and depth to be more
    # # accurate with larger data sets.
    # redis.topk.reserve "most-ordered-items", 10,
    #   width: 1_000,
    #   depth: 1_000,
    #   decay: 0.9
    #
    # # Run through all the line items in the database to get the product ids,
    # # storing them in the TopK in Redis.
    # LineItemQuery.new.product_ids.each do |product_id|
    #   redis.topk.add "most-ordered-items", product_id
    # end
    #
    # # Get the most ordered products
    # ProductQuery.new.find(redis.topk.list("most-ordered-items"))
    # ```
    def topk
      TopK.new self
    end
  end

  module Commands::Immediate
    # Returns a `TopK::Immediate`, which automatically downcasts results from
    # `TopK` into the appropriate type for those methods.
    def topk
      TopK::Immediate.new TopK.new self
    end
  end
end
