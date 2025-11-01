require "uri"
require "set"

require "./client"
require "./connection"
require "./commands"
require "./commands/immediate"
require "./read_only_commands"

# If you're using Redis replication, you can use `ReplicationClient` to send
# read commands to replicas and reduce load on the primary. This can be important
# when your Redis primary is CPU-bound.
#
# The commands that will be routed to replicas are listed in
# `Redis::READ_ONLY_COMMANDS`.
#
# NOTE: Redis replication does not provide consistency guarantees. Every
# mechanism in Redis to improve consistency, such as
# [WAIT](https://redis.io/commands/wait/#consistency-and-wait), is best-effort,
# but not guaranteed. If you require strong consistency from Redis, stick to
# using `Redis::Client`. if you require strong consistency but your Redis primary
# is CPU-bound, you may need to either choose between consistency and performance
# or move that workload out of Redis.
#
# This client is useful for operations where strong consistency isn't typically
# needed, such as caching, full-text search with `Redis::FullText#search`,
# querying time-series data with `Redis::TimeSeries#mrange`, checking the current
# state of larger data structures without blocking the primary, etc.
#
# ## Explicitly routing commands to a primary or replica
#
# This class provides `on_primary` and `on_replica` methods to ensure your
# command is routed to the server type you want. This is useful in several
# scenarios:
#
# - you want to ensure you retrieve a value that is consistent with the state of
#   the primary server — for example a value that changes frequently and you
#   need the canonical state for observability purposes
# - a read-only command is routed to a primary because this client does not yet
#   know about it
#   - You can add commands to `Redis::READ_ONLY_COMMANDS` in one-off cases
#   - Feel free to [open an issue](https://github.com/jgaskins/redis/issues) or
#     [pull request](https://github.com/jgaskins/redis/pulls) to add it, as well
#
# ## Topology changes
#
# If the replication topology changes (for example, new replicas are added,
# existing ones removed, or the primary failed over), `ReplicationClient` will
# automatically pick up the changes. You can set how often it checks for these
# changes with the `topology_ttl` argument to the constructor or leave it at its
# default of 10 seconds.
@[Experimental("`ReplicationClient` is currently in alpha testing. There may be rough edges.")]
class Redis::ReplicationClient
  include Commands
  include Commands::Immediate

  Log = ::Log.for(self)

  @master : Client
  @replicas : Array(Client)
  @master_uri : URI
  @replica_uris : Array(URI)
  getter topology_ttl : Time::Span

  def self.new
    new(entrypoint: URI.parse("redis:///"))
  end

  # Have the `ReplicationClient` discover the master and replicas on its own
  # when given the URI of a single entrypoint. The cluster topology will be
  # refreshed with a max staleness of `topology_ttl`.
  #
  # ```
  # redis = Redis::ReplicationClient.new(
  # ```
  def initialize(entrypoint : URI, topology_ttl : Time::Span = 10.seconds)
    connection = Connection.new(entrypoint, log: Log.for("redis.replication_client"))

    begin
      result = connection.run({"info", "replication"}).as(String)
    ensure
      connection.close
    end

    parsed = self.class.parse_replication_section(result)
    case parsed.role
    in .master?
      initialize(
        master_uri: entrypoint,
        replica_uris: parsed
          .replicas
          .map do |replica|
            entrypoint.dup.tap do |uri|
              uri.host = replica.ip
              uri.port = replica.port
              # TODO: Should we ignore excessively lagged replicas?
            end
          end
          .sort_by!(&.host.not_nil!),
        topology_ttl: topology_ttl,
      )
    in .replica?
      initialize(
        entrypoint.dup.tap do |uri|
          uri.host = parsed.master_host
          # Dragonfly seems to report 9999 as a default port?
          if parsed.master_port != 9999
            uri.port = parsed.master_port
          end
        end,
        topology_ttl: topology_ttl,
      )
    end
  end

  # Initialize the client with known master and replica URIs, keeping the
  # toplogy up to date with at most `topology_ttl` staleness. If you don't wish
  # to keep the replication topology up to date, you can simply set
  # `topology_ttl` to `0.seconds`.
  def initialize(@master_uri, @replica_uris, @topology_ttl = 10.seconds)
    @master = Client.new(@master_uri, log: ::Log.for("redis.primary"))

    @replicas = @replica_uris.map do |uri|
      Client.new(uri, log: ::Log.for("redis.replica"))
    end

    if topology_ttl > 0.seconds
      spawn do
        replication = self.class.parse_replication_section(@master.run({"info", "replication"}).as(String))

        until closed?
          sleep @topology_ttl
          # Check topology and update if needed
          new_replication = self.class.parse_replication_section(@master.run({"info", "replication"}).as(String))
          if new_replication != replication
            Log.info &.emit "Topology is changed, updating Redis::ReplicationClient"
            topology_ttl = @topology_ttl
            # Avoid re-spawning this fiber
            initialize entrypoint: @master_uri, topology_ttl: 0.seconds
            @topology_ttl = topology_ttl
            replication = new_replication
          end
        end
      end
    end
  end

  # :nodoc:
  def finalize
    close
  end

  # Close all connections to both the primary and all replicas.
  def close
    @master.close rescue nil
    @replicas.each do |replica|
      replica.close rescue nil
    end

    @closed = true
  end

  # Returns `true` if this `ReplicationClient` has been explicitly closed,
  # `false` otherwise.
  getter? closed = false

  protected def self.parse_replication_section(text : String)
    Info::Replication.new text
  end

  module Info
    struct Replication
      getter role : Role
      getter connected_replicas = 0
      getter replicas : Array(Info::Replica) { [] of Info::Replica }
      getter master_replid : String?
      getter master_host : String?
      getter master_port : Int32?
      getter master_link_status : String?
      getter master_last_io : Time?
      getter? master_sync_in_progress : Bool?

      def initialize(text : String)
        found_role = false
        role = ""

        found_connected_replicas = false
        connected_replicas = 0

        master_replid = ""

        text.each_line(chomp: true).with_index do |line, index|
          next if line.starts_with? '#'

          case line
          when .starts_with? "role:"
            found_role = true
            role = line[5..]
          when .starts_with? "connected_slaves:"
            @connected_replicas = line["connected_slaves:".bytesize..].to_i
            @replicas = Array(Info::Replica).new(initial_capacity: connected_replicas)
          when .starts_with? "slave"
            if separator_index = line.index(':')
              replicas << Replica.new(line[separator_index + 1..])
            else
              raise ArgumentError.new("Cannot read line: #{line.inspect}")
            end
          when .starts_with? "master_host:"
            @master_host = line["master_host:".bytesize..]
          when .starts_with? "master_port:"
            @master_port = line["master_port:".bytesize..].to_i
          when .starts_with? "master_link_status:"
            @master_link_status = line["master_link_status:".bytesize..]
          when .starts_with? "master_last_io_seconds_ago:"
            @master_last_io = line["master_last_io_seconds_ago:".bytesize..].to_i.seconds.ago
          when .starts_with? "master_sync_in_progress:"
            # No need to create a substring, we just need to check the last byte
            @master_sync_in_progress = line.ends_with? '1'
          end
        end

        if found_role
          @role = Role.parse(role)
        else
          raise ArgumentError.new("Missing role")
        end
      end

      enum Role
        Master
        Replica
      end
    end

    struct Replica
      getter ip : String
      getter port : Int32
      getter state : State
      getter lag : Time::Span

      def initialize(text : String)
        found_ip = false
        ip = ""

        found_port = false
        port = 0

        found_state = false
        state : State = :stable_sync

        found_lag = false
        lag = 0.seconds

        token_start = 0
        parse_state = ParseState::ReadingKey
        key = ""
        value = ""
        text.size.times do |cursor|
          case text[cursor]
          when '='
            parse_state = ParseState::KVSeparator
          when ','
            parse_state = ParseState::EntrySeparator
          end
          if cursor == text.size - 1
            parse_state = ParseState::End
          end

          case parse_state
          in .reading_key?
          in .reading_value?
          in .kv_separator?
            key = text[token_start...cursor]
            parse_state = ParseState::ReadingValue
            token_start = cursor + 1
          in .entry_separator?, .end?
            value = text[token_start..(parse_state.entry_separator? ? cursor - 1 : cursor)]
            parse_state = ParseState::ReadingKey
            token_start = cursor + 1

            case key
            when "ip"
              found_ip = true
              ip = value
            when "port"
              found_port = true
              port = value.to_i
            when "state"
              found_state = true
              state = State.parse(value)
            when "lag"
              found_lag = true
              lag = value.to_i.seconds
            end
          end
        end

        if found_ip && found_port && found_state && found_lag
          initialize(ip: ip, port: port, state: state, lag: lag)
        else
          raise ArgumentError.new("Replica info string must contain ip, port, state, and lag. Received: #{text.inspect}.")
        end
      end

      def initialize(@ip, @port, @state, @lag)
      end

      def ==(other : self)
        ip == other.ip && port == other.port
      end

      enum State
        StableSync
        Online
      end

      private enum ParseState
        ReadingKey
        ReadingValue
        KVSeparator
        EntrySeparator
        End
      end
    end
  end

  def run(command full_command)
    if full_command.empty?
      raise ArgumentError.new("Redis commands must have at least one component")
    end

    if READ_ONLY_COMMANDS.includes? full_command[0].downcase
      on_replica(&.run full_command)
    else
      @master.run full_command
    end
  end

  # Route one or more commands to replicas. This should rarely be necessary since
  # read-only commands (which can only be executed on replicas) are automatically
  # routed to replicas, but if it's a command this shard does not know about (see
  # `Redis::READ_ONLY_COMMANDS`) this may be necessary. Alternatively, you can
  # shovel additional commands into `Redis::READ_ONLY_COMMANDS` to avoid having to
  # perform this explicit routing.
  def on_replica(&)
    if @replicas.empty?
      yield @master
    else
      yield @replicas.sample
    end
  end

  # Route one or more commands to the primary to avoid consistency issues arising
  # from replication latency.
  #
  # ```
  # require "redis/replication_client"
  #
  # redis = Redis::ReplicationClient.new
  #
  # redis.incr "counter"
  # value = redis.on_primary &.get("counter")
  # ```
  #
  # This is useful for pipelining commands or executing transactions:
  #
  # ```
  # redis.on_primary &.transaction do |txn|
  #   txn.incr "counter:#{queue}"
  #   txn.sadd "queues", queue
  #   txn.lpush "queue:#{queue}", job_data
  # end
  # ```
  #
  # … which is shorthand for this and removes the need for nesting blocks:
  #
  # ```
  # redis.on_primary do |primary|
  #   primary.transaction do |txn|
  #     txn.incr "counter:#{queue}"
  #     txn.sadd "queues", queue
  #     txn.lpush "queue:#{queue}", job_data
  #   end
  # end
  # ```
  #
  # If you need to route many commands to the primary without necessarily
  # pipelining or opening transactions, you can omit the `&.transaction` and
  # call methods directly on the primary's `Redis::Client` in the block:
  #
  # ```
  # redis.on_primary do |primary|
  #   counter = primary.incr "counter:#{queue}"
  #   primary.sadd "queues", queue
  # end
  # ```
  #
  # NOTE: The object yielded to the block is a `Redis::Client`, but if you try
  # to use it outside the block you may run into errors because the replication
  # topology could change, in which case this `Redis::Client` might not be the
  # primary anymore.
  def on_primary(&)
    on_master { |redis| yield redis }
  end

  # Alias of `on_primary`.
  def on_master(&)
    yield @master
  end
end
