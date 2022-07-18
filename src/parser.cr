require "./value"
require "./errors"

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
      read { raise IO::Error.new("Connection closed") }
    end

    # Reads a value from the `IO`, returning `nil` on EOF.
    def read?
      return if @io.closed?
      read { nil }
    end

    private def read
      case byte_marker = @io.read_byte
      when ':'
        parse_int.tap { @io.skip 2 }
      when '*'
        length = parse_int
        @io.skip 2
        if length >= 0
          Array.new(length) { read }
        end
      when '$'
        length = parse_int
        @io.skip 2
        if length >= 0
          value = @io.read_string length
          @io.skip 2 # Skip CRLF
          value
        end
      when '+'
        @io.read_line
      when '-'
        type, message = @io.read_line.split(' ', 2)
        raise ERROR_MAP[type].new("#{type} #{message}")
      when nil
        yield
      else
        raise "Invalid byte marker: #{byte_marker.chr.inspect}"
      end
    rescue ex : IO::Error
      yield
    end

    private def parse_int
      int = 0i64
      negative = false
      loop do
        if peek = @io.peek
          case next_byte = peek[0]
          when nil
            break
          when '-'
            negative = true
            @io.skip 1
          when '0'.ord..'9'.ord
            int = (int * 10) + (next_byte - '0'.ord)
            @io.skip 1
          else
            break
          end
        else
          break
        end
      end

      if negative
        -int
      else
        int
      end
    end
  end
end
