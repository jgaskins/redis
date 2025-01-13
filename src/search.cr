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
      command = [] of String
      start = 0
      in_quotes = false

      start.upto(string.size - 1) do |index|
        if string[index] == '"'
          if in_quotes
            part = string[start...index].strip
            command << part unless part.empty?
            in_quotes = false
          else
            in_quotes = true
          end

          start = index + 1
        elsif string[index].whitespace? && !string[index - 1].whitespace? && !in_quotes
          part = part = string[start...index].strip
          command << part unless part.empty?
          start = index + 1
        end
      end
      command << string[start..-1]

      @redis.run ["ft.create"] + command
    end

    # Get information about the search index contained in `index`. For more
    # details, see [the `FT.INFO` documentation](https://oss.redis.com/redisearch/Commands/#ftinfo).
    def info(index : String)
      @redis.run({"ft.info", index})
    end

    def tagvals(index : String, field : String)
      @redis.run({"ft.tagvals", index, field})
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
      filter : Array(Filter)? = nil,
      geofilter : GeoFilter? = nil,
      inkeys : Array(String)? = nil,
      infields : Array(String)? = nil,
      return return_value : Array(String)? = nil,
      summarize : Summarize? = nil,
      highlight : Highlight? = nil,
      slop : Int? = nil,
      timeout : Time::Span? = nil,
      inorder : Bool? = nil,
      language : String? = nil,
      expander : String? = nil,
      scorer : String? = nil,
      explainscore : Bool? = nil,
      payload : String | Bytes | Nil = nil,
      sortby : SortBy? = nil,
      limit : {Int, Int}? = nil,
      params : NamedTuple | Hash(String, String) | Nil = nil,
      dialect : Int? = nil
    )
      # Pre-allocate the command buffer based on args so it performs as few
      # heap allocations as possible.
      command = Array(String).new(
        3 + # ft.search index query
        (nocontent ? 1 : 0) +
        (verbatim ? 1 : 0) +
        (nostopwords ? 1 : 0) +
        (withscores ? 1 : 0) +
        (withpayloads ? 1 : 0) +
        (filter ? 4 : 0) +
        (geofilter ? 6 : 0) +
        (inkeys.try(&.size) || 0) + 1 +
        (infields.try(&.size) || 0) + 1 +
        (return_value.try(&.size) || 0) + 1 +
        (summarize.try(&.fields).try(&.size) || 0) + 8 +
        (highlight.try(&.fields).try(&.size) || 0) + 6 +
        (slop ? 2 : 0) +
        (timeout ? 2 : 0) +
        (inorder ? 1 : 0) +
        (language ? 2 : 0) +
        (expander ? 2 : 0) +
        (scorer ? 2 : 0) +
        (explainscore ? 1 : 0) +
        (payload ? 2 : 0) +
        (sortby ? 3 : 0) +
        (limit ? 3 : 0) +
        (params ? (1 + (params.try { |params| params.size * 2 } || 0)) : 0) +
        2 # dialect
      )
      command << "ft.search" << index << query

      command << "nocontent" if nocontent
      command << "verbatim" if verbatim
      command << "nostopwords" if nostopwords
      command << "withscores" if withscores
      command << "withpayloads" if withpayloads
      command << "withsortkeys" if withsortkeys
      if filter
        filter.each do |f|
          command << "filter" << f.field << f.min << f.max
        end
      end
      if geofilter
        command << "geofilter" << geofilter.field << geofilter.longitude.to_s << geofilter.latitude.to_s << geofilter.radius.to_s << geofilter.unit.to_s.downcase
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
        command << "return" << return_value.size.to_s
        command.concat return_value
      end

      if summarize
        command << "summarize"

        # fields : {Int32, String}? = nil,
        if fields = summarize.fields
          command << "fields" << fields.size.to_s
          command.concat fields
        end
        # frags : Int32? = nil,
        if frags = summarize.frags
          command << "frags" << frags.to_s
        end
        # len : Int32? = nil,
        if len = summarize.len
          command << "len" << len.to_s
        end
        # separator : String? = nil
        if separator = summarize.separator
          command << "separator" << separator
        end
      end

      if highlight
        command << "highlight"
        if fields = highlight.fields
          field_names = fields
          command << "fields" << fields.size.to_s
          if field_names
            command.concat field_names
          end
        end
        if tags = highlight.tags
          command << "tags"
          command.concat tags
        end
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

      if params
        command << "params" << (params.size * 2).to_s
        case params
        in NamedTuple
          # I understand *why* NamedTuple#each has a different block signature,
          # but I don't love it.
          params.each { |key, value| command << key.to_s << value.to_s }
        in Hash
          params.each { |(key, value)| command << key << value }
        in Nil
          # This should never happen
        end
        dialect ||= 2
      end

      if dialect
        command << "dialect" << dialect.to_s
      end

      @redis.run(command).as Array
    end

    record Summarize,
      # SUMMARIZE [FIELDS {num} {field}] [FRAGS {numFrags}] [LEN {fragLen}] [SEPARATOR {sepstr}]
      fields : Array(String)? = nil,
      frags : Int32? = nil,
      len : Int32? = nil,
      separator : String? = nil

    record Highlight,
      # HIGHLIGHT [FIELDS {num} {field}] [TAGS {openTag} {closeTag}]
      fields : Array(String)? = nil,
      tags : {String, String}? = nil

    # Profile the given search. For further details, see [the `FT.PROFILE`
    # documentation](https://oss.redis.com/redisearch/Commands/#ftprofile).
    def profile(index : String, query : String)
      @redis.run({"ft.profile", index, "search", "query", query})
    end

    # Drop the specified `index`.
    @[Deprecated("Redis has removed the `FT.DROP` command. It will soon be removed from this client.")]
    def drop(index : String, keepdocs = false)
      dropindex index, keepdocs
    end

    # Drop the specified `index` and, if `dd: true` is passed, deletes the
    # indexed documents from Redis.
    def dropindex(index : String, dd = false)
      command = ["ft.dropindex", key]
      command << "dd" if dd

      @redis.run command
    end

    record Filter, field : String, min : String, max : String do
      def self.new(field : String, range : Range(B, E)) forall B, E
        new field,
          min: (range.begin || "-inf").to_s,
          max: (range.end || "+inf").to_s
      end
    end
    record GeoFilter,
      field : String,
      longitude : Float64,
      latitude : Float64,
      radius : Int64 | Float64,
      unit : GeoUnit
  end

  module Commands
    @[Experimental("RediSearch support is still under development. Some APIs may change while details are discovered.")]
    def ft
      FullText.new(self)
    end
  end
end
