module Redis
  ERROR_MAP = Hash(String, Error.class).new(default_value: Error)

  class Error < ::Exception
    macro define(type, code = name.gsub(/\A.*::/, "").upcase)
      class {{type}} < {{@type}}
        {% if code != nil %}
          ERROR_MAP[{{code}}] = self
        {% end %}
      end
    end

    def_equals_and_hash message
  end

  Error.define NoGroup
  Error.define BusyGroup
  Error.define ReadOnly
  # Raised when using a command that requires a key to exist.
  #
  # ```
  # redis.get! "nonexistent"
  # # => Assertion that the key "nonexistent" exists failed. (Redis::MissingKey)
  # ```
  Error.define MissingKey, nil

  class Cluster
    Error.define Error, nil
    Error.define Moved
    Error.define Ask
    Error.define CrossSlot
  end
end
