require "mutex"
require "log"

module Streamer
  class Client
    getter consumer

    def initialize(@redis : Redis::Client, @consumer : String = UUID.random.to_s, @log = ::Log.for(self.class))
    end

    def subscribe(
      stream : String,
      group : String,
      timeout : Time::Span = 30.minutes,
      &block : Message ->
    ) : Nil
      @redis.xgroup_create stream, group, id: "$", mkstream: true rescue nil
      @redis.xgroup_create_consumer stream, group, consumer
      queue = Channel(Message).new

      # RETRIEVE+ENQUEUE FIBER
      spawn do
        loop do
          response = @redis.xreadgroup group, consumer,
            streams: {stream => ">"},
            count: "10",
            block: 2.seconds

          if response
            response = Redis::Streaming::XReadGroupResponse.new(response)
            response.results.each do |result|
              # This *shouldn't* be necessary because we specified only this stream, but let's just be sure
              next unless result.key == stream

              result.messages.each do |msg|
                queue.send Message.new(msg)
              end
            end
          end
        end
      end

      # ENQUEUE PENDING
      spawn do
        earliest_id = "-"
        loop do
          response = Redis::Streaming::XAutoClaimResponse.new(@redis.xautoclaim(stream, group, consumer, min_idle_time: timeout, start: "-", count: 10))

          response.messages.each do |msg|
            queue.send Message.new(msg)
          end

          sleep timeout if response.messages.empty?
        end
      end

      successful_ids = [] of String
      success_mutex = Mutex.new
      # PROCESS MSG FIBER
      spawn do
        loop do
          msg = queue.receive

          begin
            block.call msg
            success_mutex.synchronize { successful_ids << msg.id }
          rescue ex
            @log.error { "Error while handling message #{msg.id} on #{stream}/#{group}/#{consumer}: #{ex}" }
            # FIXME: Handle the error
          end
        end
      end

      # ACK FIBER
      spawn do
        loop do
          @log.debug { "ACK" }
          if successful_ids.any?
            @log.trace { "Sending ack for #{successful_ids.size} messages" }
            success_mutex.synchronize do
              @redis.xack stream, group, successful_ids

              # TODO: Throw the array away periodically to make it somewhat elastic.
              # If we get an abnormal tidal wave of messages we don't want it to
              # remain at that massive size forever.
              successful_ids.clear
            end
          end

          sleep 1.second
        end
      end
    end

    def publish(stream : String, message : String)
      @redis.xadd stream, id: "*", body: message
    end
  end

  struct Message
    getter id, body
    getter timestamp : Time { Time.unix_ms(id.to_i64(strict: false)) }
    getter age : Time::Span { Time.utc - timestamp }

    def self.new(msg : Redis::Streaming::Message)
      new id: msg.id, body: msg.values["body"]
    end

    def initialize(@id : String, @body : String)
    end
  end
end
