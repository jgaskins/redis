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
    @pool : DB::Pool(Connection)

    def self.from_env(env_var)
      new(URI.parse(ENV[env_var]))
    end

    # The client holds a pool of connections that expands and contracts as
    # needed.
    def initialize(uri : URI)
      max_idle_pool_size = uri.query_params.fetch("max_idle_pool_size", "25").to_i
      @pool = DB::Pool.new(
        max_idle_pool_size: max_idle_pool_size,
      ) { Connection.new(uri) }
    end

    # All Redis commands invoked on the client check out a connection from the
    # connection pool, invoke the command on that connection, and then check the
    # connection back into the pool.
    #
    # ```
    # redis = Redis::Client.new
    # ```
    macro method_missing(call)
      @pool.checkout do |connection|
        connection.{{call}}
      end
    end
  end
end
