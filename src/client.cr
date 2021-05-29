require "db/pool"

require "./connection"

module Redis
  # Set a global current client
  #
  # Example
  # ```
  # Redis.current = Redis::Client.new(URI.parse(ENV.fetch("REDIS_URL", "redis://127.0.0.1:6379/0")))
  #
  # # somewhere in the app
  # def redis
  #   Redis.current
  # end
  #
  # # or just use a simple local scope
  #
  # redis = Redis.current
  # ```
  # > Note: Setting `Redis.current` can only happen once to avoid accidental override
  def self.current=(client : Redis::Client)
    raise ArgumentError.new("Warning! `Redis.current =` can only be set once to avoid accidental override") if @@current
    @@current = client
  end

  def self.current : Redis::Client
    @@current || Redis::Client.new
  end

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
    def initialize(uri : URI = URI.parse("redis:///"))
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
      @pool.checkout(&.{{call}})
    end
  end
end
