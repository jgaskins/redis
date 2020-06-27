# redis

A pure-Crystal implementation of the Redis protocol

## Installation

1. Add the dependency to your `shard.yml`:

   ```yaml
   dependencies:
     redis:
       github: jgaskins/redis
   ```

   Note that this shard currently depends on a fork of `crystal-db` for its connection pool. I'm in the process of getting those changes merged upstream so it can depend on the mainline implementation of that shard.

2. Run `shards`

## Usage

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

### Connection Pool

The `Redis::Client` maintains its own connection pool, so there is no need to run your own within your application. When you execute a command on the `Redis::Client`, it is automatically being executed against a connection. When you execute a pipeline or transaction with `multi`, all commands within that block will automatically be routed to the same connection.

## Development

Make sure you have a Redis or KeyDB server running locally on port 6379.

## Contributing

1. Fork it (<https://github.com/jgaskins/redis/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## Contributors

- [Jamie Gaskins](https://github.com/jgaskins) - creator and maintainer
