require "./commands"
require "./connection"
require "./value"
require "./errors"

module Redis
  class Pipeline
    include Commands

    @futures = [] of Future

    # Wraps a connection so that our `run` and `commit` methods can execute against it.
    def initialize(@connection : Connection)
    end

    # The `run` method is required by the `Commands` mixin. When you run a Redis
    # command, it gets pushed to the server via this method.
    def run(command)
      @connection.encode command
      future = Future.new
      @futures << future
      future
    end

    # Read all of the return values from all of the commands we've sent to Redis
    # and resolve all `Redis::Future`s with them in the order they were sent.
    def commit
      @futures.map_with_index do |future, index|
        future.resolve(@connection.parser.read)
      rescue ex
        raise ResolutionError.new("Failed reading pipeline item #{index}: #{ex.message.inspect}", cause: ex)
      end
    end

    class ResolutionError < Error
    end
  end

  # A `Redis::Future` is what pipelined commands return. They will be resolved
  # with the value of the command that spawned them.
  class Future
    @value = uninitialized Value
    getter? resolved = false

    # Resolves a Future with the supplied Redis::Value
    def resolve(@value : Value)
      @resolved = true
    end

    # Read the value contained in this Future after it is resolved. If this
    # method is called before the Future is resolved, it will raise a
    # `Redis::Future::NotResolved` exception.
    def value
      if resolved?
        @value
      else
        raise NotResolved.new("Attempted to get the value of an unresolved Redis::Future")
      end
    end

    # A `Redis::Future::NotResolved` is raised when attempting to access the
    # value of a `Future` that has not been resolved yet. For example, calling
    # `pipeline.get("my-key").value`.
    class NotResolved < Exception
    end
  end
end
