require "./spec_helper"

require "../src/parser"

module Redis
  describe Parser do
    it "reads ints" do
      [1, 12, 1234, 12345678, 123456789012345678_i64, -1, -12345678, 0].each do |int|
        io = IO::Memory.new(":#{int}\r\n")
        Parser.new(io).read.should eq int
      end
    end

    it "reads simple strings" do
      io = IO::Memory.new("+OK\r\n+QUEUED\r\n+OK\r\n+QUEUED\r\n")
      parser = Parser.new(io)

      2.times do
        parser.read.should eq "OK"
        parser.read.should eq "QUEUED"
      end
    end

    it "reads bulk strings" do
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

    it "reads arrays" do
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
