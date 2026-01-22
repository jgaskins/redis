require "./value"

module Redis
  module Streaming
    struct Message
      getter id : String
      getter values : Hash(String, String)

      def initialize(message : Array)
        id, values = message
        values = values.as(Array)
        @id = id.as(String)
        @values = Hash(String, String).new(initial_capacity: values.size // 2)
        (values.size // 2).times do |index|
          @values[values[index].as(String)] = values[index + 1].as(String)
        end
      end

      def initialize(@id, @values)
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

    struct XReadResponse
      struct Event
        include Enumerable({String, String})
        getter id : String
        getter fields : Hash(String, String)

        def initialize(array : Array(Redis::Value))
          id, fields = array
          @id = id.as(String)
          fields = fields.as(Array)
          @fields = Hash(String, String).new(initial_capacity: fields.size // 2)
          fields.each_slice(2, reuse: true) do |(key, value)|
            @fields[key.as(String)] = value.as(String)
          end
        end

        delegate each, to: @fields

        def [](field : String) : String
          @fields[field]
        end

        def []?(field : String) : String?
          @fields[field]?
        end

        def dig(field : String)
          self[field]?
        end
      end

      include Enumerable({String, Array(Event)})
      @results : Hash(String, Array(Event))

      def initialize(response : Array(Redis::Value))
        @results = Hash(String, Array(Event)).new(initial_capacity: response.size)
        response.each do |stream_list_item|
          stream_key, events = stream_list_item.as(Array)
          stream_key = stream_key.as(String)
          events = events.as(Array)

          @results[stream_key] = events.map do |event_array|
            Event.new(event_array.as(Array))
          end
        end
      end

      delegate each, to: @results

      def [](stream_name : String) : Array(Event)
        @results[stream_name]
      end

      def []?(stream_name : String) : Array(Event)?
        @results[stream_name]?
      end

      def dig(stream_name : String, *rest)
        @results.dig stream_name, *rest
      end
    end

    # Transform the `XREADGROUP` result into a more friendly object.
    struct XReadGroupResponse
      getter results

      def initialize(response : Array(Redis::Value))
        @results = Array(Result).new(initial_capacity: response.size)

        response.each do |row|
          key, data = row.as(Array)
          @results << Result.new(key.as(String), data.as(Array))
        end
      end

      struct Result
        getter messages : Array(Message)
        getter key : String

        def initialize(@key : String, messages : Array)
          @messages = messages.map { |msg| Message.new(msg.as(Array)) }
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
  end
end
