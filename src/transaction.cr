require "./commands"
require "./commands/deferred"
require "./connection"

module Redis
  class Transaction
    include Commands
    include Commands::Deferred

    getter? discarded

    def initialize(@connection : Connection)
      @command_count = 0
    end

    def discard : Nil
      @discarded = true
      finish "discard"
    end

    def run(command) : Nil
      return if discarded?
      @connection.encode command
      @command_count += 1
    end

    def exec
      finish("exec").as(Array)
    end

    def start!
      @connection.encode({"multi"})
    end

    private def finish(command)
      @connection.encode({command})
      @connection.flush
      @connection.read # MULTI
      @command_count.times { @connection.read }
      @connection.read
    end
  end
end
