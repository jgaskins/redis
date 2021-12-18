module Redis
  ERROR_MAP = Hash(String, Error.class).new(default_value: Error)

  class Error < ::Exception
  end

  class NoGroup < Error
    ERROR_MAP["NOGROUP"] = self
  end

  class BusyGroup < Error
    ERROR_MAP["BUSYGROUP"] = self
  end

  class Cluster
    class Error < ::Redis::Error
    end

    class Moved < Error
      ERROR_MAP["MOVED"] = self
    end

    class Ask < Error
      ERROR_MAP["ASK"] = self
    end

    class CrossSlot < Error
      ERROR_MAP["CROSSSLOT"] = self
    end
  end
end
