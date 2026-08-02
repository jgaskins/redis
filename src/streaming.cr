require "./value"

module Redis
  module Streaming
    struct Message
      include Enumerable({String, String})

      getter id : String
      getter values : Hash(String, String)
      getter last_delivered_at : Time
      getter delivery_count : Int64

      def initialize(message : Array)
        if message.size >= 4
          id, values, last_delivered_at, delivery_count = message
        else
          id, values = message
          last_delivered_at = 0i64
          delivery_count = 0i64
        end
        values = values.as(Array)

        @id = id.as(String)
        @values = Hash(String, String).new(initial_capacity: values.size // 2)
        @last_delivered_at = Time.unix_ms(last_delivered_at.as(Int64))
        @delivery_count = delivery_count.as(Int64)

        (0...values.size).step(2) do |index|
          @values[values[index].as(String)] = values[index + 1].as(String)
        end
      end

      def initialize(
        @id,
        @values,
        *,
        @last_delivered_at = Time::UNIX_EPOCH,
        @delivery_count = 0,
      )
      end

      delegate each, to: @values

      def [](field : String) : String
        @values[field]
      end

      def []?(field : String) : String?
        @values[field]?
      end

      def dig(field : String)
        self[field]?
      end
    end

    struct XRangeResponse
      include Enumerable(Message)

      getter messages : Array(Message)

      def initialize(messages : Array(Redis::Value))
        @messages = messages.map do |message|
          Message.new(message.as(Array))
        end
      end

      delegate each, to: messages
    end

    class XPendingResponse
      getter count, earliest, latest, data : Array(Data)

      def initialize(response : Array)
        count, first_id, last_id, data = response
        @count = count.as(Int64)
        @earliest = first_id.as(String)
        @latest = last_id.as(String)
        data = data.as(Array)
        @data = data.map do |kv|
          key, value = kv.as(Array)

          Data.new(key.as(String), value.as(String).to_i64)
        end
      end

      record Data, consumer : String, pending_count : Int64
    end

    @[Deprecated("Please use `Redis::Streaming::XPendingResponse`")]
    class XPendingBaseResponse < XPendingResponse
    end

    struct XPendingExtendedResponse
      getter messages : Array(MessageData)

      def initialize(data : Array, now : Time = Time.utc)
        @messages = data.map do |result|
          id, consumer, last_delivered_ago, delivery_count = result.as(Array)

          MessageData.new(
            id: id.as(String),
            consumer: consumer.as(String),
            last_delivered_at: now - last_delivered_ago.as(Int64).milliseconds,
            delivery_count: delivery_count.as(Int64),
          )
        end
      end

      struct MessageData
        getter id : String
        getter consumer : String
        getter last_delivered_at : Time
        getter delivery_count : Int64

        def initialize(@id, @consumer, @last_delivered_at, @delivery_count)
        end

        def age
          Time.utc - last_delivered_at
        end
      end
    end

    # The `XReadResponse` is a convenience object that you can pass the result
    # of a `Commands#xread` call to. Traversing nested `Array(Redis::Value)`
    # structures isn't a great experience, so we provide these objects to make
    # it easier to work with.
    #
    # ```
    # if response = redis.xread(streams: {my_stream: "0"})
    #   Redis::Streaming::XReadResponse.new(response).each do |stream, messages|
    #     messages.each do |msg|
    #       # ...
    #     end
    #   end
    # end
    # ```
    struct XReadResponse
      include Enumerable({String, Array(Message)})

      @streams : Hash(String, Array(Message))

      def initialize(response : Array(Redis::Value))
        @streams = Hash(String, Array(Message)).new(initial_capacity: response.size)
        response.each do |stream_list_item|
          stream_key, events = stream_list_item.as(Array)
          stream_key = stream_key.as(String)
          events = events.as(Array)

          @streams[stream_key] = events.map do |event_array|
            Message.new(event_array.as(Array))
          end
        end
      end

      delegate each, to: @streams

      def [](stream_name : String) : Array(Message)
        @streams[stream_name]
      end

      def []?(stream_name : String) : Array(Message)?
        @streams[stream_name]?
      end

      def dig(stream_name : String, *rest)
        @streams.dig stream_name, *rest
      end
    end

    # Transform the `XREADGROUP` result into a more friendly object.
    struct XReadGroupResponse
      struct Result
        getter messages : Array(Message)
        getter key : String

        def initialize(@key, @messages)
        end
      end

      include Enumerable(Result)

      getter results : Array(Result)

      def self.hash(response : Array(Redis::Value))
        results = Hash(String, Array(Message)).new(initial_capacity: response.size)
        response.each do |row|
          key, data = row.as(Array)
          messages = data.as(Array).map { |msg| Message.new(msg.as(Array)) }
          results[key.as(String)] = messages
        end
        results
      end

      def initialize(response : Array(Redis::Value))
        @results = Array(Result).new(initial_capacity: response.size)

        response.each do |row|
          key, data = row.as(Array)
          messages = data.as(Array).map { |msg| Message.new(msg.as(Array)) }
          @results << Result.new(key.as(String), messages)
        end
      end

      delegate each, to: results

      def each_message
        MessageIterator.new(results)
      end

      def each_message(&)
        results.each do |result|
          result.messages.each do |message|
            yield message
          end
        end
      end

      class MessageIterator
        include Iterator(Message)

        @results : Array(Result)

        def initialize(@results)
          @result_index = 0
          @message_index = -1
        end

        def next : Message | Stop
          if result = @results[@result_index]?
            if message = result.messages[@message_index += 1]?
              message
            else
              @message_index = -1
              @result_index += 1
              self.next
            end
          else
            stop
          end
        end
      end
    end

    struct XAutoClaimResponse
      getter id : String
      getter messages : Array(Message)

      def initialize(response : Array)
        id, messages = response
        @id = id.as(String)
        messages = messages.as(Array)
        messages.compact!
        @messages = messages.map do |message_data|
          Message.new(message_data.as(Array))
        end
      end
    end

    # The `Properties.define` macro is intended to make defining some of the
    # structs returned as a Redis hash (array in RESP2) easier.
    #
    # ```
    # struct Thing
    #   Properties.define(
    #     one : String,
    #     two : Int64,
    #   )
    # end
    #
    # Thing.new(["one", "value goes here", "two", 2])
    # # => Thing(@one="value goes here", @two=2)
    # ```
    private module Properties
      macro define(*properties)

        include Properties

        {% for prop in properties %}
          getter {{prop}}
        {% end %}

        def initialize(response : Array)
          {% for prop in properties %}
            {{prop.var}} : {{prop.type}}? = nil
          {% end %}

          response.each_slice 2, reuse: true do |(attribute, value)|
            case attribute
            {% for prop in properties %}
            when {{prop.var.stringify.gsub(/_/, "-")}} then {{prop.var}} = get(value, {{prop.type}})
            {% end %}
            end
          end

          {% for prop in properties %}
            if {{prop.var}}
              @{{prop.var}} = {{prop.var}}
            {% if !prop.type.is_a?(Union) || (prop.type.is_a?(Union) && !prop.type.types.any? { |type| type.names.any? { |name| name.stringify == "Nil" } }) %}
            else
              raise NilAssertionError.new("{{prop.var}} must be {{prop.type}}, but was either not provided by the Redis server or was sent as nil ({{prop.type}} - {{prop.type.class_name.id}})")
            {% end %}
            end
          {% end %}
        end
      end

      protected def get(value : Int64, type : Int64.class) : Int64
        value
      end

      protected def get(value : Value, type : Int64.class) : Int64
        raise TypeCastError.new("Expected Int64, got: #{value.inspect}")
      end

      protected def get(value : String, type : String.class) : String
        value
      end

      protected def get(value : Value, type : String.class) : String
        raise TypeCastError.new("Expected String, got: #{value.inspect}")
      end

      protected def get(value : String?, type : String?.class) : String?
        value
      end

      protected def get(value : Value, type : String?.class) : String?
        raise TypeCastError.new("Expected String?, got: #{value.inspect}")
      end

      protected def get(value : Int64?, type : Int64?.class) : Int64?
        value
      end

      protected def get(value : Value, type : Int64?.class) : Int64?
        raise TypeCastError.new("Expected Int64?, got: #{value.inspect}")
      end

      protected def get(value : Array(Value), type : Array(Value).class)
        value.as Array(Value)
      end

      protected def get(value : Value, type : Time.class) : Time
        Time.unix_ms value.as(Int64)
      end
    end

    @[Experimental]
    struct XInfoStreamResponse
      Properties.define(
        length : Int64,
        radix_tree_keys : Int64,
        radix_tree_nodes : Int64,
        last_generated_id : String,
        max_deleted_entry_id : String,
        idmp_duration : Int64?,
        idmp_maxsize : Int64?,
        entries_added : Int64,
        recorded_first_entry_id : String,
        pids_tracked : Int64?,
        iids_tracked : Int64?,
        iids_added : Int64?,
        iids_duplicates : Int64?,
        groups : Int64,
        first_entry : Message?,
        last_entry : Message?,
      )

      protected def get(value : Redis::Value, type : Message?.class)
        Message.new(value.as(Array))
      end
    end

    @[Experimental]
    struct XInfoStreamFullResponse
      Properties.define(
        length : Int64,
        radix_tree_keys : Int64,
        radix_tree_nodes : Int64,
        last_generated_id : String,
        max_deleted_entry_id : String,
        entries_added : Int64,
        recorded_first_entry_id : String,
        idmp_duration : Int64?,
        idmp_maxsize : Int64?,
        pids_tracked : Int64?,
        iids_tracked : Int64?,
        iids_added : Int64?,
        iids_duplicates : Int64?,
        entries : Array(Message),
        groups : Array(Group),
      )

      protected def get(value : Redis::Value, type : Array(Message).class) : Array(Message)
        value.as(Array).map { |data| Message.new(data.as(Array)) }
      end

      protected def get(value : Redis::Value, type : Array(Group).class) : Array(Group)
        value.as(Array).map { |data| Group.new(data.as(Array)) }
      end

      struct Group
        Properties.define(
          name : String,
          last_delivered_id : String?,
          entries_read : Int64?,
          lag : Int64,
          pel_count : Int64,
          nacked_count : Int64?,
          pending : Array(PendingEntry),
          consumers : Array(Consumer),
        )

        protected def get(value : Redis::Value, type : Array(PendingEntry).class) : Array(PendingEntry)
          value.as(Array).map do |data|
            id, consumer, last_delivered_at, delivery_count = data.as(Array)

            PendingEntry.new(
              id: id.as(String),
              consumer: consumer.as(String),
              last_delivered_at: Time.unix_ms(last_delivered_at.as(Int64)),
              delivery_count: delivery_count.as(Int64),
            )
          end
        end

        protected def get(value : Redis::Value, type : Array(Consumer).class) : Array(Consumer)
          value.as(Array).map { |data| Consumer.new(data.as(Array)) }
        end

        struct PendingEntry
          getter id : String
          getter consumer : String
          getter last_delivered_at : Time
          getter delivery_count : Int64

          def initialize(@id, @consumer, @last_delivered_at, @delivery_count)
          end
        end
      end

      struct Consumer
        Properties.define(
          name : String,
          seen_time : Time,
          active_time : Time,
          pel_count : Int64,
          pending : Array(PendingEntry),
        )

        protected def get(value : Redis::Value, type : Array(PendingEntry).class) : Array(PendingEntry)
          value.as(Array).map do |entry|
            data = entry.as(Array)
            PendingEntry.new(data[0].as(String), data[1].as(Int64), data[2].as(Int64))
          end
        end

        struct PendingEntry
          getter id : String
          getter last_delivered_at : Time
          getter delivery_count : Int64

          def initialize(@id, last_delivered_at : Int64, @delivery_count)
            @last_delivered_at = Time.unix_ms last_delivered_at
          end
        end
      end
    end

    @[Experimental]
    struct XInfoGroupsResponse
      getter groups : Array(Group)

      def initialize(response : Array)
        @groups = response.map { |data| Group.new data.as(Array) }
      end

      struct Group
        Properties.define(
          name : String,
          consumers : Int64,
          pending : Int64,
          last_delivered_id : String?,
          entries_read : Int64?,
          lag : Int64,
        )
      end
    end

    @[Experimental]
    struct XInfoConsumersResponse
      getter consumers : Array(Consumer)

      def initialize(response : Array)
        @consumers = response.map { |data| Consumer.new data.as(Array) }
      end

      struct Consumer
        Properties.define(
          name : String,
          pending : Int64,
          idle : Int64,
          inactive : Int64,
        )
      end
    end
  end
end
