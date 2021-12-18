require "./redis"

module Redis
  # `Redis::FullText` wraps a `Redis::Client` or `Redis::Cluster` to execute
  # commands against a fulltext search index located on a given server.
  #
  # ```
  # redis = Redis::Client.new
  # redis.ft.create <<-INDEX
  #   people-index ON HASH
  #     PREFIX 1 person:
  #   SCHEMA
  #     name TEXT NOSTEM SORTABLE
  #     email TEXT NOSTEM SORTABLE
  #     location GEO
  # INDEX
  # ```
  #
  # If your Redis server is running in Cluster mode, you can
  # `require "redis/cluster/search"` to send read-only `FullText` commands to
  # shard replicas.
  @[Experimental("RediSearch support is still under development. Some APIs may change while details are discovered.")]
  struct FullText(Runnable)
    # :nodoc:
    def initialize(@redis : Runnable)
    end

    # Pass to the `search` method's `sortby` argument to sort the results on the
    # given attribute.
    #
    # ```
    # redis.ft.search "people-index", "@name:Jamie",
    #   sortby: Redis::FullText::SortBy.new("name", :asc)
    # ```
    record SortBy, attribute : String, direction : SortDirection = :asc

    # Pass to the `search` method's `geofilter` argument to filter within the
    # specified range. For example, to search for pizza places within 25 miles
    # of the Baltimore Ravens stadium:
    #
    # ```
    # redis.ft.search "places-index", "pizza",
    #   geofilter: {"location", -76.622400, 39.277751, 25, :mi}
    # ```
    enum GeoUnit
      M
      KM
      MI
      FT
    end

    # Pass to a `SortBy` constructor to specify a fulltext search sort direction.
    enum SortDirection
      ASC
      DESC
    end

    # Create a search index using the syntax specified in the [RediSearch
    # `FT.CREATE` docs](https://oss.redis.com/redisearch/Commands/#ftcreate).
    #
    # ```
    # redis = Redis::Client.new
    # redis.ft.create <<-INDEX
    #   people-index ON HASH
    #     PREFIX 1 person:
    #   SCHEMA
    #     name TEXT NOSTEM SORTABLE
    #     email TEXT NOSTEM SORTABLE
    #     location GEO
    # INDEX
    # ```
    #
    # NOTE: This method returns immediately, before the index is complete. You can run searches against an incomplete index, but you will also have incomplete results. To find how far along the index is, you can use the `info` method.
    # TODO: Add a method that generates the string passed into this overload.
    def create(string : String)
      @redis.run ["ft.create"] + string.split
    end

    # Get information about the search index contained in `index`. For more
    # details, see [the `FT.INFO` documentation](https://oss.redis.com/redisearch/Commands/#ftinfo).
    def info(index : String)
      @redis.run({"ft.info", index})
    end

    # Run the specified `query` against `index`. Customize the search with various other arguments. For details about what each one does and the return value, see [the `FT.SEARCH`
    # documentation](https://oss.redis.com/redisearch/Commands/#ftsearch).
    #
    # ```
    # result = redis.ft.search "people-index", "@name:Jamie",
    #   return: %w[name email],
    #   sortby: Redis::FullText::SortBy.new("name", :asc)
    # ```
    def search(
      index : String,
      query : String,
      nocontent = false,
      verbatim = false,
      nostopwords = false,
      withscores = false,
      withpayloads = false,
      withsortkeys = false,
      filter : {String, Int | String, Int | String}? = nil,
      geofilter : {String, Float, Float, Numeric, GeoUnit}? = nil,
      inkeys : Array(String)? = nil,
      infields : Array(String)? = nil,
      return return_value : Array(String)? = nil,
      # summarize : idk what to do here
      # highlight : idk what to do here
      slop : Int? = nil,
      inorder : Bool? = nil,
      language : String? = nil,
      expander : String? = nil,
      scorer : String? = nil,
      explainscore = nil,
      payload : String | Bytes | Nil = nil,
      sortby : SortBy? = nil,
      limit : {Int, Int}? = nil
    )
      command = ["ft.search", index, query]

      command << "nocontent" if nocontent
      command << "verbatim" if verbatim
      command << "nostopwords" if nostopwords
      command << "withscores" if withscores
      command << "withpayloads" if withpayloads
      command << "withsortkeys" if withsortkeys
      if filter
        command << "filter"
        attr, min, max = filter
        command << attr << min.to_s << max.to_s
      end
      if geofilter
        command << "geofilter"
        geofilter.each do |(attr, lon, lat, radius, unit)|
          command << attr << lon.to_s << lat.to_s << radius.to_s << unit.to_s
        end
      end
      if inkeys
        command << "inkeys"
        command.concat inkeys
      end
      if infields
        command << "infields"
        command.concat infields
      end

      if return_value
        command += ["return", return_value.size.to_s] + return_value
      end

      command << "slop" << slop.to_s if slop
      command << "inorder" if inorder
      command << "language" << language if language
      command << "expander" << expander if expander
      command << "scorer" << scorer if scorer
      command << "explainscore" if explainscore
      case payload
      when String
        command << payload
      when Bytes
        command << String.new(payload)
      end
      if sortby
        command << "sortby" << sortby.attribute << sortby.direction.to_s
      end
      if limit
        command << "limit"
        command.concat limit.map(&.to_s).to_a
      end

      @redis.run command
    end

    # Profile the given search. For further details, see [the `FT.PROFILE`
    # documentation](https://oss.redis.com/redisearch/Commands/#ftprofile).
    def profile(index : String, query : String)
      @redis.run({"ft.profile", index, "search", "query", query})
    end

    # Drop the specified `index`.
    def drop(index : String, keepdocs = false)
      dropindex index, keepdocs
    end

    # :ditto:
    def dropindex(key : String, keepdocs = false)
      command = ["ft.dropindex", key]
      command << "keepdocs" if keepdocs

      @redis.run command
    end
  end

  module Commands
    @[Experimental("RediSearch support is still under development. Some APIs may change while details are discovered.")]
    def ft
      FullText.new(self)
    end
  end
end
