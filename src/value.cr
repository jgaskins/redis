require "./errors"

module Redis
  # Values consumed and emitted by Redis can be strings, 64-bit integers, `nil`,
  # or an array of any of these types.
  alias Value = String |
                Int64 |
                BigInt |
                Float64 |
                Bool |
                Nil |
                Error |
                Hash(Value, Value) |
                Set(Value) |
                Attributes |
                Array(Value)

  struct Attributes
    include Enumerable({Value, Value})
    @hash : Hash(Value, Value)

    def initialize(@hash)
    end

    def [](key : String) : Value
      @hash[key]
    end

    def []?(key : String) : Value
      @hash[key]?
    end

    def each
      @hash.each do |key, value|
        yield({key, value})
      end
    end
  end
end
