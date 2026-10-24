require "./errors"
require "big/big_int"

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
                Push |
                Hash(Value, Value) |
                Set(Value) |
                Array(Value)

  struct Push
    include Indexable(Value)

    def initialize(@values : Array(Value))
    end

    def size
      @values.size
    end

    def unsafe_fetch(index : Int)
      @values.unsafe_fetch(index)
    end
  end

  struct Attributes
    include Enumerable({Value, Value})
    @hash : Hash(Value, Value)

    def initialize(@hash = {} of Value => Value)
    end

    def [](key : String) : Value
      @hash[key]
    end

    def []?(key : String) : Value
      @hash[key]?
    end

    def each(&)
      @hash.each do |key, value|
        yield({key, value})
      end
    end

    def merge!(other : self)
      @hash.merge other.@hash
    end
  end
end
