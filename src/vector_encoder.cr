module Redis
  struct VectorEncoder
    def call(vector : Array(T)) : Bytes forall T
      encoded = Bytes.new(vector.size * sizeof(T))
      vector.each_with_index do |f32, index|
        IO::ByteFormat::LittleEndian.encode f32, encoded + (index * sizeof(T))
      end
      encoded
    end
  end
end
