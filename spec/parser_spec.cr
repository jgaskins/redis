require "./spec_helper"

require "../src/parser"

module Redis
  describe Parser do
    it "reads ints as Int64" do
      [1, 12, 1234, 12345678, 123456789012345678_i64, -1, -12345678, 0].each do |int|
        io = IO::Memory.new(":#{int}\r\n")
        Parser.new(io).read.should eq int
      end
    end

    it "reads simple strings as String" do
      io = IO::Memory.new("+OK\r\n+QUEUED\r\n+.\r\n+\r\n" * 2)
      parser = Parser.new(io)

      2.times do
        parser.read.should eq "OK"
        parser.read.should eq "QUEUED"
        # With the optimization made in aafe485, we need to ensure we can read
        # simple strings with a size of less than 2 bytes.
        parser.read.should eq "."
        parser.read.should eq ""
      end
    end

    it "reads bulk strings as String" do
      io = IO::Memory.new("$11\r\nHello world\r\n")
      Parser.new(io).read.should eq "Hello world"

      io = IO::Memory.new("$0\r\n\r\n")
      Parser.new(io).read.should eq ""
    end

    it "reads nil" do
      io = IO::Memory.new("$-1\r\n")
      Parser.new(io).read.should eq nil

      io = IO::Memory.new("*-1\r\n")
      Parser.new(io).read.should eq nil
    end

    it "reads arrays as Array" do
      io = IO::Memory.new
      io << "*3\r\n"
      io << "$4\r\n"     # Bulk string, length 4
      io << "foo!\r\n"   # Value of that bulk string
      io << ":12345\r\n" # Int value 12345
      io << "$-1\r\n"    # nil

      Parser.new(io.rewind).read.should eq ["foo!", 12345, nil]
    end

    it "reads arrays that contain errors" do
      io = IO::Memory.new
      io << "*3\r\n"
      io << "$3\r\n" << "foo\r\n"
      io << "-OOPS The thing broke\r\n"
      io << ":1234\r\n"

      Parser.new(io.rewind).read.should eq ["foo", Error.new("OOPS The thing broke"), 1234]
    end

    it "reads nil" do
      io = IO::Memory.new("_\r\n")

      Parser.new(io).read.should eq nil
    end

    it "reads doubles as Float64" do
      io = IO::Memory.new(",12.34\r\n,56.78\r\n,inf\r\n,-inf\r\n,nan\r\n,1.234567890e4\r\n")
      parser = Parser.new(io)

      parser.read.should eq 12.34
      parser.read.should eq 56.78
      parser.read.should eq Float64::INFINITY
      parser.read.should eq -Float64::INFINITY
      parser.read.as(Float).nan?.should eq true
      parser.read.should eq 1.234567890e4
    end

    it "reads booleans as Bool" do
      io = IO::Memory.new("#t\r\n#f\r\n")
      parser = Parser.new(io)

      parser.read.should eq true
      parser.read.should eq false
    end

    it "reads blob errors as Redis::Error" do
      io = IO::Memory.new("!15\r\nOMG Hello World!\r\n!11\r\nOK Computer\r\n")
      parser = Parser.new(io)

      parser.read.should eq Error.new("OMG Hello World!")
      parser.read.should eq Error.new("OK Computer")
    end

    it "reads verbatim strings as String" do
      io = IO::Memory.new("=9\r\ntxt:Hello\r\n=10\r\nmkd:World!\r\n")
      parser = Parser.new(io)

      parser.read.should eq "Hello"
      parser.read.should eq "World!"
    end

    it "reads big numbers" do
      io = IO::Memory.new("(123456789012345678901234567890\r\n(98765432109876543210987654321\r\n")
      parser = Parser.new(io)

      parser.read.should eq BigInt.new("123456789012345678901234567890")
      parser.read.should eq BigInt.new("98765432109876543210987654321")
    end

    it "reads maps as Hash(Redis::Value, Redis::Value)" do
      io = IO::Memory.new("%2\r\n+one\r\n:1\r\n$3\r\ntwo\r\n:2\r\n%1\r\n+foo\r\n+bar\r\n")
      parser = Parser.new(io)

      parser.read.should eq({
        "one" => 1,
        "two" => 2,
      })
      parser.read.should eq({"foo" => "bar"})
    end

    it "reads sets as Set(Redis::Value)" do
      io = IO::Memory.new("~3\r\n+one\r\n:2\r\n(3\r\n")
      parser = Parser.new(io)

      parser.read.should eq Set(Value){"one", 2i64, BigInt.new("3")}
    end

    it "reads attributes" do
      io = IO::Memory.new("|1\r\n+foo\r\n+bar\r\n|2\r\n+one\r\n:1\r\n+two\r\n:2\r\n")
      parser = Parser.new(io)

      parser.read.should eq Attributes.new({"foo" => "bar"} of Value => Value)
      parser.read.should eq Attributes.new({"one" => 1i64, "two" => 2i64} of Value => Value)
    end



    it "can read without failing if the IO is closed" do
      reader, writer = IO.pipe
      begin
        parser = Parser.new(reader)
        writer.close

        parser.read?.should eq nil
      ensure
        reader.close
      end
    end
  end
end
