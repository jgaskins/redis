module Redis::Commands::HyperLogLog
  # Add all of the `values` to the HyperLogLog stored at `key`.
  def pfadd(key : String, *values : String)
    run({"pfadd", key} + values)
  end

  # Add all of the `values` to the HyperLogLog stored at `key`.
  def pfadd(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "pfadd" << key
    run command.concat(values)
  end

  # Return the estimated number of items in the HyperLogLogs specified at `keys`.
  #
  # WARNING: In the Redis server, [`PFCOUNT`](https://redis.io/docs/latest/commands/pfcount/) is implemented differently depending on whether you supply 1 key or multiple keys — using multiple keys is significantly slower so it should not occur in a hot loop. See the official documentation for more details.
  def pfcount(*keys : String)
    run({"pfcount"} + keys)
  end

  # Return the estimated number of items in the HyperLogLogs specified at `keys`.
  #
  # WARNING: In the Redis server, [`PFCOUNT`](https://redis.io/docs/latest/commands/pfcount/) is implemented differently depending on whether you supply 1 key or multiple keys — using multiple keys is significantly slower so it should not occur in a hot loop. See the official documentation for more details.
  def pfcount(keys : Enumerable(String))
    command = Array(String).new(initial_capacity: 1 + keys.size)
    command << "pfcount"
    run command.concat(keys)
  end

  # Merge one or more HyperLogLogs specified by `source_keys` into the
  # `destination_key`.
  def pfmerge(destination_key target : String, *source_keys : String)
    run({"pfmerge", target} + source_keys)
  end

  # Merge one or more HyperLogLogs specified by `source_keys` into the
  # `destination_key`.
  def pfmerge(destination_key target : String, source_keys sources : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + sources.size)
    command << "pfmerge" << target
    run command.concat(sources)
  end
end
