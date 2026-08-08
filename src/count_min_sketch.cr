require "./commands"

module Redis
  struct CountMinSketch
    private getter redis : Commands

    def initialize(@redis)
    end

    # Initializes a `CountMinSketch` at `key` with the specified `width` and `depth`.
    #
    # ```
    # redis.cms.initbydim "orders:#{product_id}:#{date}"
    # ```
    def initbydim(key : String, width : Int | String, depth : Int | String)
      redis.run({"cms.initbydim", key, width.to_s, depth.to_s})
    end

    def initbyprob(key : String, error : Float | String, probability : Float | String)
      redis.run({"cms.initbyprob", key, error.to_s, probability.to_s})
    end

    def incrby(key : String, item : String, increment : Int | String)
      redis.run({"cms.incrby", key, item, increment.to_s})
    end

    def query(key : String, *items : String)
      redis.run({"cms.query", key} + items)
    end

    def merge(destination : String, sources : Enumerable(String))
      command = Array(String).new(sources.size + 2)
      command << "cms.merge" << destination << sources.size.to_s
      command.concat sources

      redis.run command
    end

    def info(key : String)
      redis.run({"cms.info", key})
    end

    struct Immediate
      def initialize(@cms : CountMinSketch)
      end

      private macro cast(**methods)
        {% for method, type in methods %}
          # Executes `CountMinSketch#{{method.id}}` and casts down to `{{type.id}}`.
          def {{method.id}}(*args, **kwargs)
            @cms.{{method.id}}(*args, **kwargs).as({{type}})
          end
        {% end %}
      end

      cast(
        initbyprob: String,
        initbydim: String,
        incrby: Array,
        query: Array,
        merge: Array,
        info: Array,
      )
    end
  end

  module Commands
    def cms
      CountMinSketch.new self
    end
  end

  module Commands::Immediate
    def cms
      CountMinSketch::Immediate.new CountMinSketch.new self
    end
  end
end
