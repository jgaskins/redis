module Redis
  def self.to_hash(array : ::Array)
    to_hash array, value_type: Value
  end

  def self.to_hash(array : Array, value_type : T.class) : ::Hash(String, T) forall T
    unless array.size.even?
      raise ArgumentError.new("Array must have an even number of arguments to convert to a hash")
    end

    hash = ::Hash(String, T).new(initial_capacity: array.size // 2)
    (0...array.size).step 2 do |index|
      hash[array[index].as(String)] = array[index + 1].as(T)
    end

    hash
  end
end
