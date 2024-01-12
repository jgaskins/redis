module Redis
  struct Writer
    CRLF = "\r\n"

    getter io

    def initialize(@io : IO)
    end

    # :nodoc:
    def encode(values : Enumerable(String))
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
  end
end
