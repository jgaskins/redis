require "socket"
require "openssl"

require "./commands"
require "./parser"
require "./pipeline"
require "./value"
require "./transaction"

module Redis
  class Connection
    include Commands

    @socket : TCPSocket | OpenSSL::SSL::Socket::Client

    CRLF = "\r\n"

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

    macro override_return_types(methods)
      {% for method, return_type in methods %}
        def {{method.id}}(*args, **kwargs) : {{return_type}}
          super(*args, **kwargs){{".as(#{return_type})".id unless return_type.stringify == "Nil"}}
        end
      {% end %}
    end

    override_return_types({
      keys: Array,
      get: String?,
      incr: Int64,
      decr: Int64,
      incrby: Int64,
      decrby: Int64,
      del: Int64,
      xgroup: Nil,
      xrange: Array,
      xreadgroup: Array,
    })

    def run(command)
      encode command
      @socket.flush
      read
    end

    def close
      @socket.close
    end

    def finalize
      close
    end

    def flush
      @socket.flush
    end

    def read
      @parser.read
    end

    def encode(values : Enumerable)
      @socket << '*' << values.size << CRLF
      values.each do |part|
        encode part
      end
    end

    def encode(string : String)
      @socket << '$' << string.bytesize << CRLF
      @socket << string << CRLF
    end

    def encode(int : Int)
      @socket << ':' << int << CRLF
    end

    def encode(nothing : Nil)
      @socket << "$-1" << CRLF
    end
  end
end
