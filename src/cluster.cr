# require "./client"
require "./connection"
require "./commands"
require "./commands/immediate"
require "./read_only_commands"
require "db/pool"
require "set"

module Redis
  # Use in place of a `Redis::Client` when talking to Redis clusters. This class
  # will discover all nodes in a Redis cluster when given a URI for any of them,
  # route commands to appropriate shards based on the keys they operate on, and
  # route commands which do not change state to shard replicas to spread the
  # load across the cluster.
  #
  # As nodes are added or removed, replicas are promoted, or hash slots migrate
  # between shards, the cluster client will adapt to the new topology
  # automatically. Commands that receive a `MOVED` redirection are retried
  # against the new node, and topology is re-discovered in the background so
  # subsequent commands route correctly without requiring another redirection.
  # Commands that receive an `ASK` redirection (slot mid-migration) are retried
  # against the importing node with an `ASKING` prefix.
  #
  # It's important that, when using commands which operate on multiple keys (for
  # example: `MGET`, `DEL`, `RPOPLPUSH`, etc) that _all_ specified keys reside
  # on the same shard in the cluster. Usually, this means designing your key
  # names with curly braces around parts of them to ensure they hash to the same
  # [key slot](https://redis.io/commands/cluster-keyslot). For example:
  #
  # ```
  # redis.del "{comment}:1", "{comment}:2"
  # value = redis.rpoplpush "{queue}:default", "{queue}:default:pending"
  # ```
  #
  # If you want to use a Redis module that provides custom commands, you can
  # register them as read-only with `Redis::Cluster.register_read_only_commands`
  # and they will automatically be routed to replicas. See
  # [`redis/cluster/json.cr`](https://github.com/jgaskins/redis/blob/3e874563dd524df9af6827f72edecca13a615802/src/cluster/json.cr#L5-L15)
  # for example usage.
  class Cluster
    include Commands
    include Commands::Immediate

    # :nodoc:
    LOG = ::Log.for(self)

    # :nodoc:
    private alias Pool = DB::Pool(Connection)
    private alias PoolOptions = DB::Pool::Options

    # :nodoc:
    alias Slots = Range(Int32, Int32)

    @write_pools : Array({Slots, Pool})
    @read_pools : Array({Slots, Pool})

    # Pools are kept in these hashes across topology refreshes so we don't drop
    # warm connections when a node's slot range changes. They also let us
    # create a pool on demand for a node we discover via a `MOVED`/`ASK`
    # redirection before our next topology refresh runs.
    @write_pools_by_address : ::Hash(String, Pool)
    @read_pools_by_address : ::Hash(String, Pool)

    @topology_lock : Mutex
    @topology_refresh_throttle : Time::Span
    {% if compare_versions(Crystal::VERSION, "1.19.0") >= 0 %}
      @last_topology_refresh : Time::Instant
    {% else %}
      @last_topology_refresh : Time::Span
    {% end %}
    @topology_refresh_in_flight = false
    @closed = false

    # Tell the cluster driver that all the specified Redis commands can be
    # routed to read-only replicas.
    #
    # ```
    # Redis::Cluster.register_read_only_commands %w[
    #   mymodule.get
    #   mymodule.mget
    # ]
    # ```
    def self.register_read_only_commands(commands : Enumerable(String))
      commands.each { |command| register_read_only_command command }
    end

    # Tell the cluster driver that the specified Redis command can be routed to
    # read-only replicas.
    #
    # ```
    # Redis::Cluster.register_read_only_command "mymodule.get"
    # ```
    def self.register_read_only_command(command : String)
      READ_ONLY_COMMANDS << command
    end

    # Pass a `URI` (defaulting to the `REDIS_CLUSTER_URL` environment variable)
    # to connect to the specified Redis cluster — the URI can point to _any_
    # server in the cluster and `Redis::Cluster` will discover the rest.
    #
    # *topology_refresh_throttle* controls the minimum interval between
    # automatic background topology refreshes. When `MOVED` redirections come
    # in faster than this interval, refreshes are coalesced.
    def initialize(
      @uri : URI = URI.parse(ENV["REDIS_CLUSTER_URL"]? || "redis:///"),
      @topology_refresh_throttle : Time::Span = 1.second,
    )
      @write_pools_by_address = {} of String => Pool
      @read_pools_by_address = {} of String => Pool
      @write_pools = [] of {Slots, Pool}
      @read_pools = [] of {Slots, Pool}
      @topology_lock = Mutex.new(:reentrant)
      @last_topology_refresh = instant_time - 1.hour

      @topology_lock.synchronize { discover_topology! }
    end

    # Force a refresh of the cluster's topology. This is normally done
    # automatically in response to `MOVED` redirections, but it can be useful
    # to call this method explicitly when an external process tells you that
    # the cluster has changed.
    def refresh_topology(force : Bool = true) : Nil
      @topology_lock.synchronize do
        unless force
          return if instant_time - @last_topology_refresh < @topology_refresh_throttle
        end
        discover_topology!
      end
    end

    # Get all key across all shards. This executes a `keys` command on every
    # shard in the cluster. Probably not a good idea in production since this
    # will block every Redis shard or replica for the duration of the query, but
    # we're supporting it because you may have a reasonable use case for it at
    # _some_ point and it's just not easy to do otherwise.
    def keys : Array(String)
      keys = [] of String
      each_unique_replica(&.keys.each { |key| keys << key.as(String) })

      keys
    end

    # Run a pipeline for the specified key
    #
    # ```
    # cluster.pipeline "{widgets}" do |pipe|
    #   widget_ids.each do |id|
    #     pipe.get "{widgets}:#{id}"
    #   end
    # end
    # ```
    #
    # WARNING: All keys that this pipeline operates on _MUST_ reside on the same
    # shard. It's best to pass a pre-hashed key (one containing `{}`) to this
    # method. See the example above.
    def pipeline(key : String, &)
      write_pool_for(key).checkout(&.pipeline { |pipe| yield pipe })
    end

    # Execute `Commands#scan_each` on each shard, yielding any matching keys.
    def scan_each(*args, **kwargs, &) : Nil
      each_unique_replica(&.scan_each(*args, **kwargs) { |key| yield key })
    end

    # Executes `flushdb` on each shard in the cluster.
    def flushdb
      each_master(&.run({"flushdb"}))
    end

    def run(command full_command)
      if full_command.empty?
        raise ArgumentError.new("Redis commands must have at least one component")
      end

      # Typical Redis commands follow this format:
      #   COMMAND KEY ...ARGS
      # That's not strictly true, though, so we need to come up with a more
      # robust way to determine keys so we can figure out whether to route a
      # given query to a primary vs replicas.
      command = full_command[0]
      key = full_command[1]?
      unless key
        raise Error.new("No key was specified for this command, so the cluster driver cannot route it to an appropriate Redis shard. A cluster-specific method must be added to handle cases like these until a generalized solution is added.")
      end

      # Redis commands are case-insensitive, so if someone provides an all-caps
      # command or something we can still route it properly here by downcasing
      # it before checking.
      command = command.downcase if command =~ /[A-Z]/

      started_at = instant_time
      retries = 5
      redirect_pool : Pool? = nil
      asking = false

      loop do
        pool = redirect_pool || initial_pool_for(command, key)

        begin
          return pool.checkout do |connection|
            connection.run({"asking"}) if asking
            connection.run(full_command)
          end
        rescue ex : Cluster::Moved
          raise ex if retries <= 0
          retries -= 1
          redirect_pool = write_pool_for_redirect(parse_redirect_address(ex.message))
          asking = false
          schedule_topology_refresh
        rescue ex : Cluster::Ask
          raise ex if retries <= 0
          retries -= 1
          redirect_pool = write_pool_for_redirect(parse_redirect_address(ex.message))
          asking = true
        end
      end
    ensure
      LOG.debug &.emit(command: full_command.join(' '), duration: (instant_time - started_at).total_seconds) if started_at
    end

    # Close all connections to this Redis cluster
    def close
      @topology_lock.synchronize do
        @write_pools_by_address.each_value(&.close)
        @read_pools_by_address.each_value(&.close)
        @write_pools_by_address.clear
        @read_pools_by_address.clear
        @write_pools = [] of {Slots, Pool}
        @read_pools = [] of {Slots, Pool}
      end
    ensure
      @closed = true
    end

    private def initial_pool_for(command : String, key : String) : Pool
      if READ_ONLY_COMMANDS.includes?(command)
        read_pool_for(key)
      else
        write_pool_for(key)
      end
    end

    private def read_pool_for(key : String)
      slot = slot_for(key)
      pools = @read_pools

      # Fall back to write pools if we don't have any read replicas — e.g. on
      # a single-node "cluster" or while replicas are still coming online.
      pools = @write_pools if pools.empty?

      result = pools.shuffle.find do |(slots, _)|
        slots.includes? slot
      end

      if result
        _, pool = result
        pool
      else
        raise Error.new("No Redis node available to handle hash slot #{slot} (key #{key.inspect})")
      end
    end

    private def write_pool_for(key : String)
      slot = slot_for(key)

      result = @write_pools.find do |(slots, _)|
        slots.includes? slot
      end

      if result
        _, pool = result
        pool
      else
        raise Error.new("No Redis node available to handle hash slot #{slot} (key #{key.inspect})")
      end
    end

    # Return the Redis hash slot for the given key. This is useful for seeing
    # which shard your command will be routed to.
    def slot_for(key : String)
      # https://redis.io/topics/cluster-spec#overview-of-redis-cluster-main-components
      if (hash_start = key.index('{')) && (hash_end = key.index('}', hash_start + 1))
        key = key[hash_start + 1..hash_end - 1]
      end

      CRC16.checksum(key) % 16384
    end

    private def each_master(&) : Nil
      @write_pools.each do |(_, pool)|
        pool.checkout { |conn| yield conn }
      end
    end

    private def each_unique_replica(&) : Nil
      pools = @read_pools
      pools = @write_pools if pools.empty?

      # Set to the write-pool size because that's the maximum size we'll need
      # for this data structure. The number of hash-slot ranges is based on
      # what *they* use.
      slot_sets = ::Set(Slots).new(@write_pools.size)

      pools.each do |(slots, pool)|
        # Only yield a connection from this pool if we haven't already acted
        # on a replica for this slot set.
        unless slot_sets.includes? slots
          slot_sets << slots
          pool.checkout { |conn| yield conn }
        end
      end
    end

    # Parse the host:port portion of a `MOVED`/`ASK` error message. The full
    # message looks like `MOVED 1234 10.0.0.5:6379` or
    # `ASK 1234 10.0.0.5:6379`.
    private def parse_redirect_address(message : String?) : String
      unless message
        raise Error.new("Cluster redirection error has no message")
      end

      parts = message.split(' ', 3)
      unless parts.size >= 3
        raise Error.new("Could not parse redirect address from message: #{message.inspect}")
      end
      parts[2]
    end

    private def write_pool_for_redirect(address : String) : Pool
      ip, port = split_address(address)
      write_pool_for_address(ip, port)
    end

    private def split_address(address : String) : {String, Int32}
      colon = address.rindex(':')
      raise Error.new("Could not parse address: #{address.inspect}") unless colon
      ip = address[0...colon]
      port = address[colon + 1..].to_i
      {ip, port}
    end

    private def write_pool_for_address(ip : String, port : Int32) : Pool
      address = "#{ip}:#{port}"
      @topology_lock.synchronize do
        @write_pools_by_address[address] ||= build_pool(ip, port, readonly: false)
      end
    end

    private def read_pool_for_address(ip : String, port : Int32) : Pool
      address = "#{ip}:#{port}"
      @topology_lock.synchronize do
        @read_pools_by_address[address] ||= build_pool(ip, port, readonly: true)
      end
    end

    private def build_pool(ip : String, port : Int32, *, readonly : Bool) : Pool
      Pool.new(PoolOptions.new(max_idle_pool_size: 25, initial_pool_size: 0)) do
        connection_uri = @uri.dup
        connection_uri.host = ip
        connection_uri.port = port
        connection = Connection.new(connection_uri)
        connection.readonly! if readonly
        connection
      end
    end

    # Coalesce concurrent requests to refresh topology. The first MOVED in a
    # window of *topology_refresh_throttle* triggers a background refresh; any
    # further MOVEDs while that refresh is running are dropped.
    private def schedule_topology_refresh : Nil
      should_run = @topology_lock.synchronize do
        next false if @closed
        next false if @topology_refresh_in_flight
        next false if instant_time - @last_topology_refresh < @topology_refresh_throttle
        @topology_refresh_in_flight = true
        true
      end
      return unless should_run

      spawn do
        begin
          refresh_topology(force: true)
        rescue ex
          LOG.warn &.emit("failed to refresh cluster topology", error: ex.message)
        ensure
          @topology_lock.synchronize { @topology_refresh_in_flight = false }
        end
      end
    end

    # Caller must hold `@topology_lock`.
    private def discover_topology! : Nil
      nodes_response = fetch_cluster_nodes

      write_nodes = {} of String => Node
      read_nodes = {} of String => Node
      replica_map = {} of String => String

      nodes_response.each_line do |line|
        next if line.blank?
        parsed = parse_node_line(line)
        next unless parsed

        node, master_id = parsed
        replica_map[node.id] = master_id if master_id

        if node.flags.master?
          write_nodes[node.id] = node
        elsif node.flags.replica? && !node.flags.fail? && !node.flags.no_addr?
          read_nodes[node.id] = node
        end
      end

      read_nodes.each_value do |node|
        if (master_id = replica_map[node.id]?) && (master = write_nodes[master_id]?)
          node.replica_of = master
        end
      end

      new_write_pools = write_nodes.each_value.map { |node|
        {node.slots, write_pool_for_address(node.ip, node.port)}
      }.to_a

      new_read_pools = [] of {Slots, Pool}
      read_nodes.each_value do |node|
        # Skip orphaned replicas: with no master, we don't know which slots
        # they serve, so they can't be picked by a key lookup anyway.
        next if node.replica_of.nil?
        new_read_pools << {node.slots, read_pool_for_address(node.ip, node.port)}
      end

      @write_pools = new_write_pools
      @read_pools = new_read_pools
      @last_topology_refresh = instant_time
    end

    # Caller must hold `@topology_lock`.
    private def fetch_cluster_nodes : String
      # Try existing pools first so we don't have to open a brand-new
      # connection on every refresh. Also, if our entrypoint URI has gone away
      # (e.g. the node behind it left the cluster), we can still recover as
      # long as some other known node is still reachable.
      pools = @write_pools_by_address.values + @read_pools_by_address.values
      pools.each do |pool|
        begin
          return pool.checkout(&.run({"cluster", "nodes"})).as(String)
        rescue ex
          LOG.debug &.emit("could not fetch cluster topology from a known pool, trying another", error: ex.message)
        end
      end

      # No existing pool worked (e.g. on first run). Open a fresh connection.
      connection = Connection.new(@uri)
      begin
        connection.run({"cluster", "nodes"}).as(String)
      ensure
        connection.close
      end
    end

    private def parse_node_line(line : String) : {Node, String?}?
      id, host_info, flags_str, master, last_ping, last_pong, config, connected = line.split(' ', 8)
      # The "slots" parameter might not be provided for replicas, so we handle
      # that here and use a bogus hash-slot range that will parse into a valid
      # Int32 range.
      if connected.index(' ')
        connected, slots = connected.split
      else
        slots = "0-0"
      end

      # Format: <ip>:<port>@<cport>[,<hostname>[,<aux>=<value>]*]
      # We only need ip/port/cport here; drop hostname and aux fields.
      ip, port, cluster_port = host_info.split(',', 2).first.split(/[:@]/)
      return nil if ip.empty?
      port = port.to_i
      cluster_port = cluster_port.to_i

      flags_strings = flags_str.split(',').to_set
      flags = Node::Flags.new(0)
      flags |= Node::Flags::Master if flags_strings.includes?("master")
      flags |= Node::Flags::Replica if flags_strings.includes?("slave")
      flags |= Node::Flags::PFail if flags_strings.includes?("fail?")
      flags |= Node::Flags::Fail if flags_strings.includes?("fail")
      flags |= Node::Flags::Handshake if flags_strings.includes?("handshake")
      flags |= Node::Flags::NoAddr if flags_strings.includes?("noaddr")
      flags |= Node::Flags::NoFailover if flags_strings.includes?("nofailover")
      flags |= Node::Flags::NoFlags if flags_strings.includes?("noflags")

      master_id = master == "-" ? nil : master

      last_ping_t = Time::UNIX_EPOCH + last_ping.to_i64.milliseconds
      last_pong_t = Time::UNIX_EPOCH + last_pong.to_i64.milliseconds
      config_int = config.to_i
      connected_bool = connected == "connected"
      slots_low, slots_high = slots.split('-').map(&.to_i)
      slots_range = slots_low..slots_high

      node = Node.new(
        id: id,
        ip: ip,
        port: port,
        cluster_port: cluster_port,
        flags: flags,
        replica_of: nil,
        last_ping: last_ping_t,
        last_pong: last_pong_t,
        config: config_int,
        connected: connected_bool,
        slots: slots_range,
      )

      {node, master_id}
    end

    # :nodoc:
    class Node
      getter id : String
      getter ip : String
      getter port : Int32
      getter cluster_port : Int32
      getter flags : Flags
      getter replica_of : Node?
      getter last_ping : Time
      getter last_pong : Time
      getter config : Int32
      getter? connected : Bool
      getter slots : Range(Int32, Int32)

      def initialize(@id, @ip, @port, @cluster_port, @flags, @replica_of, @last_ping, @last_pong, @config, @connected, @slots)
      end

      def replica_of=(master : self)
        @replica_of = master
        @slots = master.slots
      end

      @[::Flags]
      enum Flags
        Master
        Replica # We are *not* using the other fuckin' term for it here
        PFail
        Fail
        Handshake
        NoAddr
        NoFailover
        NoFlags
      end
    end

    private module CRC16
      extend self

      TABLE = {
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
        0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
        0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
        0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
        0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
        0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
        0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
        0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
        0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
        0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
        0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
        0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
        0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
        0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
        0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
        0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
        0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
        0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
        0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
        0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
        0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
        0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
        0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
        0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
        0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
        0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
        0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
        0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
        0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
        0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
      }

      def checksum(key : String)
        crc = 0u16
        key.each_byte do |byte|
          crc = (crc << 8) ^ TABLE[((crc >> 8) ^ byte) & 0xFF]
        end

        crc
      end
    end
  end
end
