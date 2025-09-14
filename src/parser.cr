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

    private def read(&)
      case byte_marker = @io.read_byte
      when ':'
        parse_int.tap { @io.skip 2 }
      when '*'
        length = parse_int
        @io.skip 2
        if length >= 0
          Array(Value).new(length) { read }
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
        # Most of the time, RESP simple strings are just "OK", so we can
        # optimize for that case to avoid heap allocations. If it is *not* the
        # "OK" string, this does an extra heap allocation, but that seems like
        # a decent tradeoff considering the vast majority of times a simple
        # string will be returned from the server is from a SET call.
        buffer = uninitialized UInt8[4] # "OK\r\n"
        slice = buffer.to_slice
        read = @io.read_fully slice[0, 2] # Just trying to check whether we got "OK"
        if read == 2 && slice[0, 2] == "OK".to_slice && (second_read = @io.read_fully(slice + 2)) && slice == "OK\r\n".to_slice
          "OK"
        elsif read == 2 && slice[0, 2] == "\r\n".to_slice
          ""
        else
          String.build do |str|
            str.write slice[0...read + (second_read || 0)]
            str << @io.read_line
          end.chomp
        end
      when '-'
        type, message = @io.read_line.split(' ', 2)
        ERROR_MAP[type].new("#{type} #{message}")
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
