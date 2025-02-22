module Redis
  struct Writer
    CRLF = "\r\n"

    getter io

    def initialize(@io : IO)
    end

    # :nodoc:
    def encode(values : Enumerable(String) | Enumerable(Bytes) | Enumerable(String | Bytes))
      io << '*' << values.size << CRLF
      values.each do |part|
        encode part
      end
    end

    # :nodoc:
    def encode(string : String)
      io << '$' << string.bytesize << CRLF
      io << string << CRLF
    end

    def encode(bytes : Slice)
      io << '$' << bytes.size << CRLF
      io.write bytes
      io << CRLF
    end
  end
end
