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
end
