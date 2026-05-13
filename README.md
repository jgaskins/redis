# redis

A pure-Crystal implementation of the Redis protocol

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     redis:
       github: jgaskins/redis
   ```

2. Run `shards`

## Usage

This shard has 4 `Redis::Commands::Immediate` types for different topologies:


- `Redis::Connection`: Single direct connection to the Redis server. Not sharable between fibers.
- `Redis::Client`: Full client that supports many connections to the Redis server simultaneously using a connection pool. Sharable between fibers.
- `Redis::ReplicationClient`: Like `Redis::Client`, but sends all known read-only commands to a replica to spare the primary/master node. Automatically discovers the replication topology and adapts to changes in that topology. Sharable between fibers.
- `Redis::Cluster`: Like `Redis::Client`, but for talking to Redis servers running in cluster mode. Automatically discovers the cluster topology and adapts to changes in that topology. Sharable between fibers.

```crystal
require "redis"

redis = Redis::Client.new # Defaults to `localhost` port 6379

redis.set "foo", "bar"
redis.get "foo" # => "bar"

redis.incr "counter" # => 1
redis.incr "counter" # => 2
redis.decr "counter" # => 1

redis.del "foo", "counter" # => 2
```

### Deferred commands

There are two different types for deferring commands (`Redis::Commands::Deferred`):

- `Redis::Pipeline`: Runs a set of commands on the Redis server without waiting for the response from each one.
- `Redis::Transaction`: Runs a set of commands atomically. Either all of the commands run or none of them do.

#### Pipelined queries

To mitigate latency with multiple queries whose inputs and outputs are completely independent of each other, you can "pipeline" your queries by sending them all at once before reading them. To do this, you can use the `Redis::Client#pipeline` method:

```crystal
require "redis"

redis = Redis::Client.new

redis.pipeline do |pipe|
  pipe.incr "foo"
  pipe.set "bar", "baz"
  pipe.lpush "my-list", "my value"
end
```

The return value of `pipeline` will be an array containing the values of each of those calls in the order they were sent. So in this case, it might be `[1, "OK", 2]` to match the return values of `incr`, `set`, and `lpush`, respectively.

#### Transactions

The Redis [`MULTI` command](https://redis.io/commands/multi) begins a transaction, so you can use the `multi` method to execute a transaction against the server:

```crystal
redis.multi do |txn|
  txn.set "foo", "bar"
  txn.incr "baz"
  txn.lpush "my-list", "my value"
end
```

The transaction is automatically committed with [`EXEC`](https://redis.io/commands/exec) at the end of the block. If an exception occurs within the block, the transaction will be rolled back with [`DISCARD`](https://redis.io/commands/discard) before exiting the block.

You may also call `txn.discard`, which will effectively disable the transaction (all further methods called on the transaction do nothing), but will not exit the block. You will need to exit the block explicitly with `break` if there are operations within the block that cannot be rolled back, such as sending an email or sending a request to a third-party API.

The reason for this is that the only way to exit a containing block from an inner method in Crystal is to raise an exception, and this library chooses not to use exceptions for flow control.

### Beyond `localhost`

By default, this shard connects to `redis://localhost:6379`. If you need to connect to a server on a different host and/or port, you can either set the `REDIS_URL` environment variable or pass a `URI` to the client. For example, if you store it in your shell environment:

```crystal
redis = Redis::Client.new(URI.parse(ENV["REDIS_CACHE_URL"]))

# ... or ...

redis = Redis::Client.from_env("REDIS_CACHE_URL")
```

To connect via SSL, make sure you use the `rediss://` URL scheme. If your Redis server requires a password or uses a different database slot than `0`, make sure you include them in the URL:

```crystal
redis = Redis::Client.new(URI.parse("rediss://:my_password@redis.example.com/3"))
```

### Connection Pool

The `Redis::Client` maintains a connection pool, so there is no need to run your own within your application. When you execute a command on the `Redis::Client`, it is automatically executed against a connection. When you execute a pipeline or transaction with `multi`, all commands within that block will automatically be routed to the same connection.

**Configuration**

For this shard, we use the following default setting (outside of the stdlib defaults);

```
max_idle_pool_size = 25
```

> You can override this manually using the URI parameters.
> All other settings follow the DB::Pool defaults.

The behaviour of the connection pool can be configured from a set of query string parameters in the connection URI:

- `initial_pool_size`: Size of the connection pool when instantiating the `Redis::Client`, `Redis::Cluster`, or `Redis::ReplicationClient`. Defaults to 1.
- `max_pool_size`: Maximum size of the connection pool. Trying to check out a connection when there are already this many connections in the pool will block until a connection becomes available. Set to 0 for unlimited size. Defaults to 0 (unlimited).
- `max_idle_pool_size`: Maximum number of idle connections in the connection pool. Tune this number for an elastic pool size. Defaults to 25.
- `checkout_timeout`: Maximum number of seconds to wait while checking out a connection from the pool. Defaults to 5.
- `retry_attempts`: Number of times we'll retry reconnecting to a Redis server if the connection is lost. Defaults to 1.
- `retry_delay`: Number of seconds between retry attempts. Defaults to 1.

See [Crystal guides](https://crystal-lang.org/reference/1.20/database/connection_pool.html) to learn more.

**Example**

```crystal
pool_params = URI::Params{
  "initial_pool_size" => "1",
  "max_pool_size" => "10",
  "checkout_timeout" => "10",
  "retry_attempts" => "2",
  "retry_delay" => "0.5",
  "max_idle_pool_size" => "50",
}
redis = Redis::Client.new(URI.parse("redis://localhost:6379/0#{pool_params}"))
```

**Recommendations**

If you encounter any issues, keep these setting the same:

- `initial_pool_size`
- `max_pool_size`
- `max_idle_pool_size`

Example:

```
initial_pool_size = 50
max_pool_size = 50
max_idle_pool_size = 50
```

### TCP Keep-Alive

The `Redis::Client` uses a pool of `Redis::Connection` under the hood.
Within `Redis::Connection` we create a `TCPSocket`, which can accept keepalive params.
The TCP keepalive settings can help you mitigate Redis connection stability issues.

> NOTE: This behaviour is disabled by default. See Configuration below on how to enable it.

**Configuration**

For this shard, we use the following override setting;

| Name | Default value |
| :--- | :--- |
| keepalive | false |
| keepalive\_count | 3 |
| keepalive\_idle | 60 |
| keepalive\_interval | 30 |

> You can override this manually using the URI parameters.
> The settings above have proven to have good results in production environments. However, every environment is different, so tweaking these settings may be necessary.

See [Crystal API](https://crystal-lang.org/api/1.6.0/TCPSocket.html) to learn more.

**Example**

```crystal
params = "?keepalive=true&keepalive_count=5&keepalive_idle=10&keepalive_interval=15"

redis = Redis::Client.new(URI.parse("redis://localhost:6379/0#{params}"))
# or direct connections
redis = Redis::Connection.new(URI.parse("redis://localhost:6379/0#{params}"))
```

**Recommendations**

Enable this setting with the defaults if you are encountering connection issues.

Example:

```crystal
params = "?keepalive=true"

redis = Redis::Client.new(URI.parse("redis://localhost:6379/0#{params}"))
# or direct connections
redis = Redis::Connection.new(URI.parse("redis://localhost:6379/0#{params}"))
```

## Development

Make sure you have a Redis or KeyDB server running locally on port 6379.

Redis must be installed with a stack server for the full text search (`ft`) and time series modules (`ts`) in order for all specs to run.

### With Docker

You can use this for your docker-compose file

```yaml
redis:
  image: redis/redis-stack-server
  ports:
    - "6379:6379"
```

### With Homebrew

Install the [`redis-stack` from homebrew](https://github.com/redis-stack/homebrew-redis-stack)


## Contributing

1. Fork it (<https://github.com/jgaskins/redis/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
