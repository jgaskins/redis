require "socket"
require "openssl"

require "./commands"
require "./parser"
require "./pipeline"
require "./value"
require "./transaction"

module Redis
  # The connection wraps the TCP connection to the Redis server.
  class Connection
    include Commands

    @socket : TCPSocket | OpenSSL::SSL::Socket::Client

    CRLF = "\r\n"

    # We receive all connection information in the URI.
    #
    # SSL connections require specifying the `rediss://` scheme.
    # Password authentication uses the URI password.
    # DB selection uses the URI path.
    def initialize(@uri = URI.parse("redis:///"))
      host = uri.host || "localhost"
      port = uri.port || 6379
      socket = TCPSocket.new(host, port)
      socket.sync = false

      # Check whether we should use SSL
      if uri.scheme == "rediss"
        socket = OpenSSL::SSL::Socket::Client.new(socket)
        socket.sync = false
      end

      @socket = socket
      @parser = Parser.new(@socket)

      pipeline do |redis|
        # Authentication
        if password = uri.password
          run({"auth", password})
        end

        # DB select
        db = if {"", "/"}.includes?(uri.path)
          "0"
        else
          uri.path[1..-1]
        end
        run({"select", db}) unless db == "0"
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

      @socket.flush
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
    # redis.get "foo" # => nil
    # redis.get "counter" # => nil
    # ```
    def multi
      txn = Transaction.new(self)

      begin
        txn.start!
        yield txn
        if txn.discarded?
          [] of Value
        else
          txn.exec.as(Array)
        end
      rescue ex
        txn.discard
        raise ex
      end
    end

    def subscribe(*channels : String, &block : Subscription, self ->)
      subscription = Subscription.new(self)
      encode({"subscribe"} + channels)
      flush

      yield subscription, self

      subscription.call
    end

    def unsubscribe(*channels : String)
      encode({"unsubscribe"} + channels)
      flush
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
      keys: Array,
      get: String?,
      incr: Int64,
      decr: Int64,
      incrby: Int64,
      decrby: Int64,
      del: Int64,
      smembers: Array,
      xlen: Int64,
      xgroup: Nil,
      xrange: Array,
      xreadgroup: Array,
    })

    # Execute the given command and return the result from the server. Commands must be an `Enumerable`.
    #
    # ```
    # run({"set", "foo", "bar"})
    # ```
    def run(command)
      encode command
      @socket.flush
      read
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

    # :nodoc:
    def encode(values : Enumerable)
      @socket << '*' << values.size << CRLF
      values.each do |part|
        encode part
      end
    end

    # :nodoc:
    def encode(string : String)
      @socket << '$' << string.bytesize << CRLF
      @socket << string << CRLF
    end

    # :nodoc:
    def encode(int : Int)
      @socket << ':' << int << CRLF
    end

    # :nodoc:
    def encode(nothing : Nil)
      @socket << "$-1" << CRLF
    end
  end

  private class Subscription
    @on_message = Proc(String, String, Nil).new {}
    @on_subscribe = Proc(String, Int64, Nil).new {}
    @on_unsubscribe = Proc(String, Int64, Nil).new {}
    @channels = [] of String

    def initialize(@connection : Connection)
    end

    def on_message(&block : String, String ->)
      @on_message = block
      self
    end

    def on_subscribe(&block : String, Int64 ->)
      @on_subscribe = block
      self
    end

    def on_unsubscribe(&block : String, Int64 ->)
      @on_unsubscribe = block
      self
    end

    def message!(channel : String, message : String)
      @on_message.call channel, message
    end

    def subscribe!(channel : String, count : Int64)
      @channels << channel
      @on_subscribe.call channel, count
    end

    def unsubscribe!(channel : String, count : Int64)
      @channels.delete channel
      @on_unsubscribe.call channel, count
    end

    def call
      loop do
        action, channel, argument = @connection.read.as(Array)
        action = action.as String
        channel = channel.as String

        case action
        when "message"
          message! channel, argument.as(String)
        when "subscribe"
          subscribe! channel, argument.as(Int64)
        when "unsubscribe"
          unsubscribe! channel, argument.as(Int64)
          break if argument == 0
        else
          raise Subscription::InvalidMessage.new("Unknown message received for subscription: #{action}")
        end
      end

      self
    end

    class InvalidMessage < Exception
    end
  end
end
