require "socket"
require "openssl"

require "./client"
require "./value"

module Redis
  VERSION = "0.6.1"

  protected def self.to_hash(array : Array)
    unless array.size.even?
      raise ArgumentError.new("Array must have an even number of arguments to convert to a hash")
    end

    hash = Hash(String, Value).new(initial_capacity: array.size // 2)
    (0...array.size).step 2 do |index|
      hash[array[index].as(String)] = array[index + 1]
    end

    hash
  end
end
