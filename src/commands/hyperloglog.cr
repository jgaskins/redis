module Redis::Commands::HyperLogLog
  # Add all of the `values` to the HyperLogLog value at 
  def pfadd(key : String, *values : String)
    run({"pfadd", key} + values)
  end

  def pfadd(key : String, values : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + values.size)
    command << "pfadd" << key
    run command.concat(values)
  end

  def pfcount(*keys : String)
    run({"pfcount"} + keys)
  end

  def pfcount(keys : Enumerable(String))
    command = Array(String).new(initial_capacity: 1 + keys.size)
    command << "pfcount"
    run command.concat(keys)
  end

  def pfmerge(destination_key target : String, *source_keys : String)
    run({"pfmerge", target} + source_keys)
  end

  def pfmerge(destination_key target : String, source_keys sources : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + sources.size)
    command << "pfmerge" << target
    run command.concat(sources)
  end
end
