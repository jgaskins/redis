module Redis
  struct Writer
    CRLF = "\r\n"

    getter io : IO

    def initialize(@io)
    end

    def encode(values : Enumerable(String) | Enumerable(Bytes) | Enumerable(String | Bytes))
      io << '*' << values.size << CRLF
      values.each do |part|
        encode part
      end
    end

    def encode(data : String | Bytes)
      io << '$' << data.bytesize << CRLF
      io.write data.to_slice
      io << CRLF
    end
  end
end
