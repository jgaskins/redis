require "socket"
require "openssl"

require "./commands"
require "./errors"
require "./parser"
require "./pipeline"
require "./value"
require "./transaction"
require "./writer"

module Redis
  # The connection wraps the TCP connection to the Redis server.
  class Connection
    include Commands

    @socket : TCPSocket | OpenSSL::SSL::Socket::Client

    # We receive all connection information in the URI.
    #
    # SSL connections require specifying the `rediss://` scheme.
    # Password authentication uses the URI password.
    # DB selection uses the URI path.
    def initialize(@uri = URI.parse("redis:///"))
      host = uri.host.presence || "localhost"
      port = uri.port || 6379
      socket = TCPSocket.new(host, port)
      socket.sync = false

      # Check whether we should use SSL
      if uri.scheme == "rediss"
        socket = OpenSSL::SSL::Socket::Client.new(socket)
        socket.sync = false
      end

      @socket = socket
      @writer = Writer.new(socket)
      @parser = Parser.new(@socket)

      pipeline do |redis|
        # Authentication
        if (username = uri.user) && (password = uri.password)
          auth = {"auth", username, password}
        elsif password = uri.password
          auth = {"auth", password}
        end
        redis.run auth

        # DB select
        db = if {"", "/"}.includes?(uri.path)
               "0"
             else
               uri.path[1..-1]
             end
        unless db == "0"
          redis.run({"select", db})
        end
      end
    end

    # Execute a pipeline of commands. A pipeline sends all commands to the
    # server before reading any of the results.
    #
    # ```
    # redis.pipeline do |redis|
    #   redis.set "foo", "bar"
    #   redis.incr "counter"
    # end
    # ```
    def pipeline
      pipeline = Pipeline.new(self)
      error = nil
      begin
        yield pipeline
      rescue ex
        error = ex
      end

      flush
      result = pipeline.commit

      if error
        raise error
      else
        result
      end
    end

    # Execute a transaction within the server. The transaction is automatically
    # committed at the end of the block or can be rolled back with
    # `Transaction#discard`. Transactions are also rolled back if an exception
    # is raised.
    #
    # ```
    # redis.multi do |redis|
    #   redis.set "foo", "bar"
    #   redis.incr "counter"
    #   raise "Oops!"
    # end
    #
    # redis.get "foo"     # => nil
    # redis.get "counter" # => nil
    # ```
    def multi(retries = 5)
      loop do
        txn = Transaction.new(self)

        begin
          txn.start!
          yield txn
          if txn.discarded?
            return [] of Value
          else
            return txn.exec.as(Array)
          end
        rescue ex
          txn.discard
          raise ex
        end
      rescue ex : IO::Error
        if retries > 0
          retries -= 1
          initialize @uri
        else
          raise ex
        end
      end
    end

    {% for command in %w[subscribe psubscribe] %}
      # Subscribe to the given pubsub channels. The block yields a subscription
      # object and the connection. You can setup `on_message`, `on_subscribe`,
      # and `on_unsubscribe` on the subscription.
      #
      # ```
      # redis.subscribe "channel1", "channel2" do |subscription, connection|
      #   subscription.on_message do |channel, message|
      #     if message == "unsubscribe"
      #       connection.unsubscribe channel
      #     end
      #
      #     # ...
      #   end
      #
      #   # Respond to new subscribers
      #   subscription.on_subscribe do |channel, sub_count|
      #     connection.set "sub_count:#{channel}", sub_count.to_s
      #   end
      #
      #   # Respond to losing subscribers
      #   subscription.on_unsubscribe do |channel, sub_count|
      #     connection.set "sub_count:#{channel}", sub_count.to_s
      #   end
      # end
      # ```
      #
      # For more information, see the documentation for:
      # - [`SUBSCRIBE`](https://redis.io/commands/subscribe/)
      # - [`PSUBSCRIBE`](https://redis.io/commands/psubscribe/)
      def {{command.id}}(*channels : String, &block : Subscription, self ->)
        subscription = Subscription.new(self)
        @writer.encode({"{{command.id}}"} + channels)
        flush

        yield subscription, self

        subscription.call
      end
    {% end %}

    # Subscribe to the given channels without having to pass a block, which
    # would block execution. This is useful to run inside of other subscription
    # blocks to add new subscriptions.
    def subscribe(*channels : String)
      @writer.encode({"subscribe"} + channels)
      flush
    end

    # Unsubscribe this connection from all subscriptions.
    def unsubscribe
      @writer.encode({"unsubscribe"})
      flush
    end

    # Unsubscribe this connection the given channels.
    def unsubscribe(*channels : String)
      @writer.encode({"unsubscribe"} + channels)
      flush
    end

    def punsubscribe(*channels : String)
      @writer.encode({"punsubscribe"} + channels)
      flush
    end

    # Put this connection in a readonly state. This is typically used when
    # talking to replicas, and used automatically by `Cluster` for cluster
    # replicas.
    def readonly! : Nil
      run({"readonly"})
    end

    # :nodoc:
    macro override_return_types(methods)
      {% for method, return_type in methods %}
        # :nodoc:
        def {{method.id}}(*args, **kwargs) : {{return_type}}
          super(*args, **kwargs){{".as(#{return_type})".id unless return_type.stringify == "Nil"}}
        end
      {% end %}
    end

    # When new commands are added to the Commands mixin, add an entry here to
    # make sure the return type is set when run directly on the connection.
    override_return_types({
      keys:   Array,
      get:    String?,
      incr:   Int64,
      decr:   Int64,
      incrby: Int64,
      decrby: Int64,
      del:    Int64,

      lrange: Array,
      lpop:   String?,
      rpop:   String?,
      lpush:  Int64,
      rpush:  Int64,

      smembers:   Array,
      ttl:        Int64,
      pttl:       Int64,
      xlen:       Int64,
      xgroup:     Nil,
      xrange:     Array,
      xpending:   Array,
      xreadgroup: Array(Value)?,
      xautoclaim: Array,
    })

    # Execute the given command and return the result from the server. Commands must be an `Enumerable`.
    #
    # ```
    # run({"set", "foo", "bar"})
    # ```
    def run(command, retries = 5) : Value
      loop do
        @writer.encode command
        flush
        return read
      rescue ex : IO::Error
        if retries > 0
          retries -= 1
          initialize @uri
        else
          raise ex
        end
      end
    end

    # Iterate over keys that match the given pattern or all keys if no pattern
    # is supplied, yielding each key to the block. This is a much more efficient
    # way to iterate over keys than `keys.each` — it avoids loading every key in
    # memory at the same time and also doesn't block the Redis server while it
    # generates the array of all those keys.
    def scan_each(match pattern : String? = nil, count : String | Int | Nil = nil, type : String? = nil) : Nil
      # SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]
      cursor = ""
      until cursor == "0"
        response = scan(cursor, match: pattern, count: count, type: type)
        cursor, results = response.as(Array)
        cursor = cursor.as(String)
        results.as(Array).each do |key|
          yield key.as(String)
        end
      end
    end

    # Close the connection to the server.
    def close
      @socket.close
    end

    # :nodoc:
    def finalize
      close
    end

    # Flush the connection buffer and make sure we've sent everything to the
    # server.
    def flush
      @socket.flush
    end

    # Read the next value from the server
    def read
      @parser.read
    end

    # Read the next value from the server, returning `nil` if the connection is
    # closed.
    def read?
      @parser.read?
    end

    # The URI
    def url
      @uri.to_s
    end

    # Send the given command over the wire without waiting for a reply. This is
    # useful for query pipelining or sending commands that have no return value.
    #
    # WARNING: Be careful with this because you can get the client out of sync
    # with the server. You should almost never have to use this, but it can be
    # useful if a command like this has not been implemented yet.
    def encode(command)
      @writer.encode command
    end
  end

  # The `Subscription` is what is yielded to a `Connection#subscribe` block. It
  # is used to setup callbacks when messages come in, the connection is
  # subscribed to other channels or patterns, or unsubscribed from any channels
  # or patterns.
  #
  # ```
  # redis.subscribe "channel1", "channel2" do |subscription, connection|
  #   subscription.on_message do |channel, message|
  #     if message == "unsubscribe"
  #       connection.unsubscribe channel
  #     end
  #
  #     # ...
  #   end
  #
  #   # Respond to new subscribers
  #   subscription.on_subscribe do |channel, sub_count|
  #     connection.incr "sub_count:#{channel}"
  #   end
  #
  #   # Respond to losing subscribers
  #   subscription.on_unsubscribe do |channel, sub_count|
  #     connection.incr "sub_count:#{channel}"
  #   end
  # end
  # ```
  #
  # For more information, see the documentation for:
  # - [`SUBSCRIBE`](https://redis.io/commands/subscribe/)
  # - [`PSUBSCRIBE`](https://redis.io/commands/psubscribe/)
  class Subscription
    @on_message = Proc(String, String, String, Nil).new { }
    @on_subscribe = Proc(String, Int64, Nil).new { }
    @on_unsubscribe = Proc(String, Int64, Nil).new { }
    @channels = [] of String

    # :nodoc:
    def initialize(@connection : Connection)
    end

    # Define a callback for when a new message is received.
    #
    # ```
    # subscription.on_message do |channel, message|
    #   pp channel: channel, message: message
    # end
    # ```
    def on_message(&@on_message : String, String, String ->)
      self
    end

    # Define a callback to execute when the connection is subscribed to another
    # channel.
    def on_subscribe(&@on_subscribe : String, Int64 ->)
      self
    end

    # Define a callback to execute when the connection is unsubscribed from
    # another channel.
    def on_unsubscribe(&@on_unsubscribe : String, Int64 ->)
      self
    end

    # :nodoc:
    def message!(channel : String, message : String)
      @on_message.call channel, message, channel
    end

    # :nodoc:
    def pmessage!(channel : String, message : String, pattern : String)
      @on_message.call channel, message, channel
    end

    # :nodoc:
    def subscribe!(channel : String, count : Int64)
      @channels << channel
      @on_subscribe.call channel, count
    end

    # :nodoc:
    def unsubscribe!(channel : String, count : Int64)
      @channels.delete channel
      @on_unsubscribe.call channel, count
    end

    # :nodoc:
    def call
      loop do
        notification = @connection.read?
        break if notification.nil?

        notification = notification.as(Array)
        action, channel, argument = notification
        action = action.as String
        if action == "pmessage"
          _, pattern, channel, argument = notification
        end
        channel = channel.as String

        case action
        when "message"
          message! channel, argument.as(String)
        when "pmessage"
          pmessage! channel, argument.as(String), pattern.as(String)
        when "subscribe", "psubscribe"
          subscribe! channel, argument.as(Int64)
        when "unsubscribe", "punsubscribe"
          unsubscribe! channel, argument.as(Int64)
          break if argument == 0
        else
          raise Subscription::InvalidMessage.new("Unknown message received for subscription: #{action}")
        end
      end

      self
    end

    def close
      @channels.each do |channel|
        @connection.unsubscribe channel
      end
    end

    class InvalidMessage < Error
    end
  end
end
