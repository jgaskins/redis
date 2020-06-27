require "./value"

module Redis
  struct Parser
    # Initialize a parser to read from the given IO
    def initialize(@io : IO)
    end

    # Read a `Redis::Value` from the parser's `IO`
    #
    # Example:
    #
    # ```
    # io = IO::Memory.new
    # io << "$3\r\n"
    # io << "foo\r\n"
    # io.rewind
    #
    # Parser.new(io).read # => "foo"
    # ```
    def read : Value
      case byte_marker = @io.read_byte
      when ':'
        @io.read_line.to_i64
      when '*'
        length = @io.read_line.to_i
        Array.new(length) { read }
      when '$'
        length = @io.read_line.to_i
        if length > 0
          bytes = Bytes.new(length)
          @io.read_fully(bytes)
          value = String.new(bytes)
          @io.skip 2 # Skip CRLF
          value
        end
      when '+'
        @io.read_line
      when '-'
        raise @io.read_line
      when nil
        raise IO::Error.new("Connection closed")
      else
        raise "Invalid byte marker: #{byte_marker.chr}"
      end
    end
  end
end
