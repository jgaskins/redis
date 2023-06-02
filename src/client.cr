require "db/pool"

require "./connection"

module Redis
  # The Redis client is the expected entrypoint for this shard. By default, it will connect to localhost:6379, but you can also supply a `URI` to connect to an arbitrary Redis server. SSL, password authentication, and DB selection are all supported.
  #
  # ```
  # # Connects to localhost:6379
  # redis = Redis::Client.new
  #
  # # Connects to a server at "redis.example.com" on port 6000 over a TLS
  # # connection, authenticates with the password "password", and uses DB 3
  # redis = Redis::Client.new(URI.parse("rediss://:password@redis.example.com:6000/3"))
  #
  # # Connects to a server at the URL in `ENV["REDIS_URL"]`
  # redis = Redis::Client.from_env("REDIS_URL")
  # ```
  class Client
    include Commands

    @pool : DB::Pool(Connection)

    def self.from_env(env_var)
      new(URI.parse(ENV[env_var]))
    end

    # The client holds a pool of connections that expands and contracts as
    # needed.
    def initialize(uri : URI = URI.parse(ENV.fetch("REDIS_URL", "redis:///")))
      # defaults as per https://github.com/crystal-lang/crystal-db/blob/v0.11.0/src/db/pool.cr
      initial_pool_size = uri.query_params.fetch("initial_pool_size", 1).to_i
      max_pool_size = uri.query_params.fetch("max_pool_size", 0).to_i
      checkout_timeout = uri.query_params.fetch("checkout_timeout", 5.0).to_f
      retry_attempts = uri.query_params.fetch("retry_attempts", 1).to_i
      retry_delay = uri.query_params.fetch("retry_delay", 0.2).to_f

      # default is 1, but we want to be able to use 25 minimum
      max_idle_pool_size = uri.query_params.fetch("max_idle_pool_size", 25).to_i

      @pool = DB::Pool.new(
        initial_pool_size: initial_pool_size,
        max_pool_size: max_pool_size,
        max_idle_pool_size: max_idle_pool_size,
        checkout_timeout: checkout_timeout,
        retry_attempts: retry_attempts,
        retry_delay: retry_delay,
      ) do
        Connection.new(uri)
      end
    end

    Connection.set_return_types!

    def scan_each(match pattern : String? = nil, count : String | Int | Nil = nil, type : String? = nil) : Nil
      @pool.checkout(&.scan_each(match: pattern, count: count, type: type) { |key| yield key })
    end

    # All Redis commands invoked on the client check out a connection from the
    # connection pool, invoke the command on that connection, and then check the
    # connection back into the pool.
    #
    # ```
    # redis = Redis::Client.new
    # ```
    def run(command)
      @pool.checkout(&.run(command))
    end

    def pipeline
      @pool.checkout(&.pipeline { |pipe| yield pipe })
    end

    def multi
      @pool.checkout(&.multi { |txn| yield txn })
    end

    def subscribe(*channels)
      @pool.checkout(&.subscribe(*channels) { |subscription, conn| yield subscription, conn })
    end

    def close
      @pool.close
    end
  end
end
