module Redis
  struct FullText(Runnable)
    def aggregate!(*args, **kwargs)
      aggregate(*args, **kwargs).as(Array)
    end

    def aggregate(
      index : String,
      query : String,
      load fields : Enumerable(String)? = nil,
      timeout : Time::Span? = nil,
      groupby : GroupBy? = nil,
      apply : Apply? = nil,
      sortby : SortByAggregate | String | Nil = nil,
      params : NamedTuple | Hash(String, String) | Nil = nil,
      dialect : Int? = nil
    )
      command = Array(String).new
      command << "ft.aggregate" << index << query

      # [LOAD count field [field ...]]
      if fields
        command << "load" << fields.size.to_s
        command.concat fields
      end

      # [TIMEOUT timeout]
      if timeout
        command << "timeout" << timeout.total_milliseconds.to_i64.to_s
      end

      # [ GROUPBY nargs property [property ...] [ REDUCE function nargs arg [arg ...] [AS name] [ REDUCE function nargs arg [arg ...] [AS name] ...]] ...]]
      if groupby
        command << "groupby" << groupby.properties.size.to_s
        command.concat groupby.properties
        groupby.reducers.each do |reducer|
          command << "reduce"
          command.concat reducer
        end
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
        end
        dialect ||= 2
      end

      if apply.is_a? Apply
        apply = {apply}
      end
      apply.try &.each do |apply|
        command << "apply" << apply.expression << "as" << apply.name
      end

      # [ SORTBY nargs [ property ASC | DESC [ property ASC | DESC ...]] [MAX num]
      case sortby
      in String
        command << "sortby" << "2" << sortby << "asc"
      in SortByAggregate
        command << "sortby" << (sortby.properties.size * 2).to_s
        sortby.properties.each do |property|
          command << property.name << property.order.to_s
        end
        if max = sortby.max
          command << "max" << max.to_s
        end
      in Nil
      end

      if dialect
        command << "dialect" << dialect.to_s
      end

      @redis.run command
    end

    struct Apply
      getter expression : String
      getter name : String

      def initialize(@expression, as @name)
      end
    end

    struct SortByAggregate
      getter properties : Array(Property)
      getter max : Int32?

      def self.new(property : String, order : Order, max : Int32? = nil)
        new [Property.new(property, order)], max: max
      end

      def initialize(@properties, @max)
      end

      record Property, name : String, order : Order

      enum Order
        ASC
        DESC
      end
    end

    struct GroupBy
      protected getter properties : Array(String)
      protected getter reducers = [] of Array(String)

      def initialize(property : String)
        initialize [property]
      end

      def initialize(@properties)
      end

      def reduce
        @reducers << yield Reducer.new
        self
      end

      struct Reducer
        macro define(*names)
          {% for name in names %}
            def {{name}}(property : String)
              ["{{name}}", "1", property]
            end

            def {{name}}(property : String, as name : String)
              ["{{name}}", "1", property, "as", name]
            end
          {% end %}
        end

        define sum, count_distinct, count_distinctish, min, max, avg, stddev, tolist, first_value

        def count
          ["count", "0"]
        end

        def count(as name : String)
          ["count", "0", "as", name]
        end

        def quantile(property : String, quantile : Float64)
          quantile property, quantile.to_s
        end

        def quantile(property : String, quantile : String)
          ["quantile", "2", property, quantile]
        end

        def quantile(property : String, quantile : Float64, as name : String)
          quantile property, quantile.to_s, as: name
        end

        def quantile(property : String, quantile : String, as name : String)
          ["quantile", "2", property, quantile, "as", name]
        end

        def first_value(property : String, by order_property : String, order : Order)
          ["first_value", "4", property, "by", order_property, order.to_s]
        end

        def first_value(property : String, by order_property : String, order : Order, as name : String)
          ["first_value", "4", property, "by", order_property, order.to_s, "as", name]
        end

        def random_sample(property : String, sample_size : Int)
          random_sample property, sample_size.to_s
        end

        def random_sample(property : String, sample_size : String)
          ["random_sample", "2", property, sample_size]
        end

        def random_sample(property : String, sample_size : Int, as name : String)
          random_sample property, sample_size.to_s, as: name
        end

        def random_sample(property : String, sample_size : String, as name : String)
          ["random_sample", "2", property, sample_size, "as", name]
        end

        enum Order
          ASC
          DESC
        end
      end
    end
  end
end
