require "./value"

module Redis
  module Streaming
    struct XPendingBaseResponse
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

    struct Message
      getter id, values

      def initialize(message : Array)
        id, values = message
        values = values.as(Array)
        @id = id.as(String)
        @values = Hash(String, String).new(initial_capacity: values.size // 2)
        (values.size // 2).times do |index|
          @values[values[index].as(String)] = values[index + 1].as(String)
        end
      end
    end
  end
end
