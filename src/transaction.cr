require "./commands"
require "./commands/deferred"
require "./connection"

module Redis
  class Transaction
    include Commands
    include Commands::Deferred

    getter status : Status

    def initialize(@connection : Connection)
      @command_count = 0
      @status = :queued
    end

    def discard : Nil
      @status = :discarded
      finish "discard"
    end

    def run(command) : Nil
      return if status.discarded?
      @connection.encode command
      @command_count += 1
    ensure
      @connection.log.debug &.emit "multi", command: command
        .map { |part|
          case part
          in String
            part
          in Bytes
            String.new(part).inspect
          end
        }
        .join(' ')
    end

    def exec
      start = Time.monotonic
      begin
        finish("exec").as(Array(Value)?)
      ensure
        @connection.log.debug &.emit "exec",
          commands: @command_count,
          duration_ms: (Time.monotonic - start).total_milliseconds
        @status = :committed
      end
    end

    def start!
      @connection.encode({"multi"})
    end

    delegate discarded?, to: status

    private def finish(command)
      @connection.encode({command})
      @connection.flush
      @connection.read # MULTI
      @command_count.times { @connection.read }
      @connection.read
    end

    enum Status
      Queued
      Committed
      Discarded
    end
  end
end
