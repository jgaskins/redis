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

    def_equals_and_hash
  end

  Error.define NoGroup
  Error.define BusyGroup

  class Cluster
    Error.define Error, nil
    Error.define Moved
    Error.define Ask
    Error.define CrossSlot
  end
end
