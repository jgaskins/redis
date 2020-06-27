require "./value"

module Redis
  module Commands
    abstract def run(command)

    def keys(pattern = "*")
      run({"keys", pattern})
    end

    def set(key : String, value : String, ex = nil, px = nil, nx = false, xx = false, keepttl = false) : Nil
      command = {"set", key, value}
      command += {"ex", ex.to_s} if ex
      command += {"px", px.to_s} if px
      command += {"nx"} if nx
      command += {"xx"} if xx
      command += {"keepttl"} if keepttl

      run command
    end

    def get(key : String)
      run({"get", key})
    end

    def incr(key : String)
      run({"incr", key})
    end

    def decr(key : String)
      run({"decr", key})
    end

    def incrby(key : String, amount : Int | String)
      run({"incrby", key, amount.to_s})
    end

    def decrby(key : String, amount : Int | String)
      run({"decrby", key, amount.to_s})
    end

    def del(*keys : String)
      run({"del"} + keys)
    end

    def lpush(key, *values)
      run({"lpush", key} + values)
    end

    def rpoplpush(source : String, destination : String)
      run({"rpoplpush", source, destination})
    end

    def rpop(key : String)
      run({"rpop", key})
    end

    def brpop(*keys : String, timeout : Time::Span)
      brpop(*keys, timeout: timeout.total_seconds)
    end

    def brpop(*keys : String, timeout : Int | Float)
      timeout = timeout.to_i if timeout == timeout.to_i
      brpop(*keys, timeout: timeout.to_s)
    end

    def brpop(*keys : String, timeout : String)
      run({"brpop"} + keys + {timeout})
    end

    def xadd(key : String, id : String, maxlen = nil, **data)
      command = Array(Value).new(initial_capacity: data.size * 2 + 5)
      command << "xadd" << key
      command << "maxlen" << maxlen if maxlen
      command << id
      data.each do |key, value|
        command << key.to_s << value
      end

      run command
    end

    def xadd(key : String, id : String, data : Hash(String, String?))
      # TODO: See if we can make this work to avoid the array allocation
      # encode Command.new size: data.size * 2 + 3
      # encode "xadd"
      # encode key
      # encode id
      # data.each do |key, value|
      #   encode key
      #   encode value
      # end
      command = Array(Value).new(initial_capacity: data.size * 2 + 3)
      command << "xadd" << key << id
      data.each do |key, value|
        command << key << value
      end

      run command
    end

    def xlen(key : String)
      run({"xlen", key})
    end

    def xrange(key : String, start min, end max, count = nil)
      command = {"xrange", key, min, max}
      if count
        command += {"count", count}
      end

      run command
    end

    # For some reason the *args version doesn't recognize this signature, so we
    # just run a separate method signature without it.
    def xgroup(command : String, key : String, groupname : String)
      run({"xgroup", command, key, groupname})
    end

    def xgroup(command : String, key : String, groupname : String, *args : String)
      run({"xgroup", command, key, groupname} + args)
    end

    def xreadgroup(group : String, consumer : String, count : String | Int | Nil = nil, streams : NamedTuple = NamedTuple.new)
      command = Array(Value).new(initial_capacity: 7 + streams.size * 2)
      command << "xreadgroup" << "group" << group << consumer
      command << "count" << count if count
      command << "streams"
      streams.each do |key, value|
        # Symbol#to_s does not allocate a string on the heap, so the only
        # allocation in this method is the array.
        command << key.to_s << value
      end

      run command
    end
  end
end
