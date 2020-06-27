require "./spec_helper"

require "../src/parser"

module Redis
  describe Parser do
    it "reads ints" do
      io = IO::Memory.new(":1234\r\n")
      Parser.new(io).read.should eq 1234
    end

    it "reads simple strings" do
      io = IO::Memory.new("+OK\r\n")
      Parser.new(io).read.should eq "OK"
    end

    it "reads bulk strings" do
      io = IO::Memory.new("$11\r\nHello world\r\n")
      Parser.new(io).read.should eq "Hello world"
    end

    it "reads nil" do
      io = IO::Memory.new("$-1\r\n")
      Parser.new(io).read.should eq nil
    end

    it "reads arrays" do
      io = IO::Memory.new
      io << "*3\r\n"
      io << "$4\r\n" # Bulk string, length 4
      io << "foo!\r\n" # Value of that bulk string
      io << ":12345\r\n" # Int value 12345
      io << "$-1\r\n" # nil

      Parser.new(io.rewind).read.should eq ["foo!", 12345, nil]
    end
  end
end
