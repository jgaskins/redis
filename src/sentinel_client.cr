require "uri"
require "log"

require "./client"
require "./connection"
require "./commands"
require "./commands/immediate"
require "./log"

module Redis
  # `SentinelClient` connects to a Redis Sentinel cluster, discovers the
  # current master automatically, and follows failovers in the background.
  # It is a drop-in replacement for `Redis::Client` in Sentinel-managed
  # environments — opt-in via `require "redis/sentinel_client"`.
  #
  # ```
  # redis = Redis::SentinelClient.new(
  #   sentinels: [URI.parse("redis://s1:26379"), URI.parse("redis://s2:26380")],
  #   master_name: "mymaster",
  #   master_uri: URI.parse("rediss://:master-pass@ignored/2?keepalive=true"),
  # )
  # redis.set "foo", "bar"
  # redis.get "foo" # => "bar"
  # ```
  #
  # **Auth separation**: sentinel passwords go in the sentinel URI
  # (`redis://:pass@sentinel:26379`); master auth, TLS, DB, and pool params
  # all go in `master_uri` — the two never mix.
  #
  # **Failover**: three background fibers maintain connectivity — a pub/sub
  # subscriber on `+switch-master` for speed, a polling heartbeat for
  # correctness, and a sentinel registry refresh for resilience. On failover
  # the internal `Redis::Client` pool is atomically swapped under a mutex; the
  # old pool is closed asynchronously so in-flight commands can drain.
  class SentinelClient
    include Commands
    include Commands::Immediate

    Log       = ::Log.for(self)
    MasterLog = ::Log.for("redis.sentinel.master")

    # One entry from a `SENTINEL sentinels <name>` response.
    record SentinelInfo, ip : String, port : Int32, flags : String

    # Parsed from a `+switch-master` notification — only the fields we act on.
    record MasterChange, master_name : String, new_host : String, new_port : Int32

    getter? closed = false
    @pubsub_conn : Connection? = nil
    @seed_sentinel_keys : ::Set(String)

    # Construct from environment variables:
    # `REDIS_SENTINEL_URLS` (comma-separated, required), `REDIS_SENTINEL_MASTER`
    # (default `"mymaster"`), `REDIS_URL` (master connection template).
    def self.from_env(
      sentinel_urls_env : String = "REDIS_SENTINEL_URLS",
      master_name_env : String = "REDIS_SENTINEL_MASTER",
      master_url_env : String = "REDIS_URL",
    ) : self
      new(
        sentinels: ENV[sentinel_urls_env].split(',').map { |u| URI.parse(u.strip) },
        master_name: ENV.fetch(master_name_env, "mymaster"),
        master_uri: URI.parse(ENV.fetch(master_url_env, "redis:///")),
      )
    end

    # *sentinels* — URI list for sentinel nodes. Passwords in the URI are used
    # for sentinel `AUTH`. At least three sentinels are recommended for quorum.
    #
    # *master_name* — the logical name of the master as configured in sentinel
    # (`sentinel monitor <name> ...`).
    #
    # *master_uri* — connection template for the master: scheme (`redis`/`rediss`),
    # auth, path (DB), and all query-param features (keepalive, pool sizes,
    # timeouts). The host and port are ignored — sentinel provides them.
    #
    # *check_interval* — how often the polling heartbeat verifies the master.
    # The pub/sub watcher detects failovers faster; polling is a backstop.
    #
    # *sentinel_refresh_interval* — how often `SENTINEL sentinels` is queried
    # to discover newly added sentinel nodes.
    def initialize(
      sentinels : Array(URI),
      @master_name : String,
      master_uri : URI = URI.parse("redis:///"),
      @check_interval : Time::Span = 5.seconds,
      @sentinel_refresh_interval : Time::Span = 30.seconds,
    )
      @master_uri_template = master_uri
      @sentinel_registry = {} of String => URI
      @seed_sentinel_keys = sentinels.map { |uri| "#{uri.host}:#{uri.port || 26379}" }.to_set
      @mutex = Mutex.new
      @registry_mutex = Mutex.new
      sentinels.each { |uri| register_sentinel(uri) }
      @client = Client.new(discover_master_uri, log: MasterLog)
      start_pubsub_watcher
      start_poll_watcher
      start_sentinel_registry_refresh
    end

    def run(command)
      current_client.run(command)
    end

    def pipeline(&)
      current_client.pipeline { |pipe| yield pipe }
    end

    def multi(&)
      current_client.multi { |txn| yield txn }
    end

    def subscribe(*channels : String, &)
      current_client.subscribe(*channels) { |sub, conn| yield sub, conn }
    end

    def psubscribe(*channels : String, &)
      current_client.psubscribe(*channels) { |sub, conn| yield sub, conn }
    end

    def watch(*keys : String, &)
      current_client.watch(*keys) { |conn| yield conn }
    end

    def scan_each(match pattern : String? = nil, count : String | Int | Nil = nil, type : String? = nil, &) : Nil
      current_client.scan_each(match: pattern, count: count, type: type) { |key| yield key }
    end

    def hscan_each(key : String, *, match pattern : String? = nil, count : String | Int | Nil = nil, &) : Nil
      current_client.hscan_each(key: key, match: pattern, count: count) { |field, value| yield field, value }
    end

    def sscan_each(key : String, *, match pattern : String? = nil, count : String | Int | Nil = nil, &) : Nil
      current_client.sscan_each(key: key, match: pattern, count: count) { |member| yield member }
    end

    def sscan_each(key : String, *, match pattern : String? = nil, count : String | Int | Nil = nil)
      current_client.sscan_each(key, match: pattern, count: count)
    end

    def zscan_each(key : String, *, match pattern : String? = nil, count : String | Int | Nil = nil, &) : Nil
      current_client.zscan_each(key: key, match: pattern, count: count) { |member, score| yield member, score }
    end

    # Retries *block* when it fails with a connection-level error — the
    # situation this class exists to handle: the master died mid-operation
    # or a failover is in progress. Each retry calls back into `block`,
    # which should call back into `self` (`run`, `get`, `set`, ...); since
    # every top-level call re-reads `current_client`, a retry picks up the
    # newly promoted master as soon as the background watchers have swapped
    # it in, without the caller needing to do anything special.
    #
    # Only rescues `IO::Error`, `DB::PoolResourceLost`,
    # `DB::PoolRetryAttemptsExceeded`, and `DB::PoolTimeout` — connection/pool
    # failures where retrying is meaningful (`DB::PoolTimeout` in particular
    # covers a bounded pool whose connections are all stuck against the dead
    # master, so a checkout can't get one in time — retrying it can land on
    # the new client's pool once the swap has happened). A `Redis::Error` (a
    # reply the server actually sent back, e.g. a syntax error or
    # `WRONGTYPE`) is deterministic and is left to propagate immediately;
    # retrying it against a different master wouldn't change the outcome.
    #
    # There's deliberately no delay between attempts here. By the time any of
    # the exceptions above reaches this method, real wall-clock time has
    # already passed: `Connection#run` (connection.cr) retries internally —
    # up to 5 reconnect attempts against the *same* dead host, each bounded
    # by `connect_timeout` (default 5s) — and `DB::Pool#retry` (crystal-db)
    # adds its own attempts with its own `retry_delay` on top of that. A
    # single failed attempt can therefore already take tens of seconds; an
    # additional artificial sleep here wouldn't meaningfully improve the odds
    # of the next attempt landing on a promoted master, it would just stack
    # more latency on latency that's already substantial. `attempts` defaults
    # to 1 (i.e. no retry) for the same reason — since one attempt is already
    # expensive, silently retrying more than once should be something the
    # caller opts into deliberately, not a default that can turn one slow
    # failure into a much slower one.
    #
    # **This is opt-in, not automatic** — a connection error never reveals
    # whether the command reached the old master before it died, so only
    # wrap commands you know are safe to run more than once (`SET`, `GET`,
    # `DEL`, ...). Wrapping a non-idempotent command (`INCR`, `LPUSH`, ...)
    # risks silently applying it twice.
    #
    # ```
    # redis.with_retry(attempts: 2) { redis.set("foo", "bar") }
    # ```
    def with_retry(attempts : Int32 = 1, &)
      self.class.retry_connection_errors(attempts) { yield }
    end

    # The retry loop behind `with_retry`, extracted as a class method (no
    # instance state involved) so it can be unit tested without a live
    # sentinel cluster.
    def self.retry_connection_errors(attempts : Int32, &)
      attempts.times do |i|
        begin
          return yield
        rescue ex : IO::Error | DB::PoolResourceLost | DB::PoolRetryAttemptsExceeded | DB::PoolTimeout
          raise ex if i == attempts - 1
        end
      end
      raise Error.new("unreachable: with_retry exhausted its loop without returning or raising")
    end

    # Returns the URI of the currently active master.
    def master_uri : URI
      @mutex.synchronize { @client.uri }
    end

    # Returns the number of sentinel nodes currently tracked.
    def sentinel_count : Int32
      @registry_mutex.synchronize { @sentinel_registry.size }
    end

    # Refresh the sentinel registry by querying all known sentinels.
    # Drops entries that no reachable sentinel reports anymore, except the
    # originally configured seeds, which are always kept so a total outage
    # can't empty the registry. Called automatically in the background;
    # exposed for testing.
    def refresh_sentinel_registry : Nil
      reachable = ::Set(String).new
      found_any = false
      registry_snapshot.each do |uri|
        next unless query_sentinel_for_peers(uri, reachable)
        found_any = true
        reachable << "#{uri.host}:#{uri.port || 26379}"
      end
      return unless found_any
      @registry_mutex.synchronize do
        @sentinel_registry.select! { |key, _| reachable.includes?(key) || @seed_sentinel_keys.includes?(key) }
      end
    end

    def close : Nil
      @closed = true
      client, pubsub = @mutex.synchronize { {@client, @pubsub_conn} }
      pubsub.try { |c| c.close rescue nil }
      client.close rescue nil
    end

    # :nodoc:
    def finalize
      close rescue nil
    end

    # Parse the flat key-value array returned by `SENTINEL sentinels <name>`.
    def self.parse_sentinel_list(raw : Value) : Array(SentinelInfo)
      return [] of SentinelInfo unless raw.is_a?(Array)
      raw.compact_map do |entry|
        next unless entry.is_a?(Array)
        ip = ""
        port = 26379
        flags = ""
        entry.each_slice(2) do |kv|
          next unless kv.size == 2
          val = kv[1]
          case kv[0].as?(String)
          when "ip"    then ip = val.as(String)
          when "port"  then port = val.as(String).to_i
          when "flags" then flags = val.as(String)
          end
        end
        SentinelInfo.new(ip: ip, port: port, flags: flags) unless ip.empty?
      end
    end

    # Parse a `+switch-master` pub/sub message.
    # Wire format: `<master-name> <old-ip> <old-port> <new-ip> <new-port>`
    def self.parse_switch_master_message(message : String) : MasterChange
      parts = message.split(' ', 5)
      raise Error.new("malformed +switch-master message (expected 5 parts): #{message.inspect}") if parts.size < 5
      MasterChange.new(master_name: parts[0], new_host: parts[3], new_port: parts[4].to_i)
    end

    # Build a master URI from *template*, replacing host and port only.
    def self.build_master_uri(template : URI, host : String, port : Int32) : URI
      uri = template.dup
      uri.host = host
      uri.port = port
      uri
    end

    private def current_client : Client
      @mutex.synchronize { @client }
    end

    private def registry_snapshot : Array(URI)
      @registry_mutex.synchronize { @sentinel_registry.values.shuffle }
    end

    private def discover_master_uri : URI
      last_error = nil
      registry_snapshot.each do |sentinel_uri|
        conn = nil
        begin
          conn = open_sentinel_connection(sentinel_uri)
          result = conn.run({"sentinel", "get-master-addr-by-name", @master_name})
          next unless result.is_a?(Array)
          host, port = result
          return self.class.build_master_uri(@master_uri_template, host.as(String), port.as(String).to_i)
        rescue ex
          Log.debug &.emit "Sentinel did not provide master address",
            sentinel: "#{sentinel_uri.host}:#{sentinel_uri.port}",
            error: ex.message.to_s
          last_error = ex
        ensure
          conn.try { |c| c.close rescue nil }
        end
      end
      raise Error.new(
        "No sentinel could provide master address for #{@master_name.inspect}" +
        (last_error ? " — last error: #{last_error.message}" : ""),
      )
    end

    private def swap_master_if_changed(new_host : String, new_port : Int32) : Nil
      old_client = @mutex.synchronize do
        # Don't create a new pool after close() — it would never be closed.
        return if @closed
        current = @client.uri
        return if current.host == new_host && current.port == new_port
        Log.info &.emit "Sentinel: master changed",
          from: "#{current.host}:#{current.port}",
          to: "#{new_host}:#{new_port}"
        old = @client
        @client = Client.new(
          self.class.build_master_uri(@master_uri_template, new_host, new_port),
          log: MasterLog,
        )
        old
      end
      spawn { old_client.close rescue nil }
    end

    private def register_sentinel(uri : URI) : Nil
      key = "#{uri.host}:#{uri.port || 26379}"
      @registry_mutex.synchronize { @sentinel_registry[key] ||= uri }
    end

    private def open_sentinel_connection(uri : URI) : Connection
      Connection.new(uri, log: Log)
    end

    # Queries *sentinel_uri* for its known peers, registering each one and
    # recording it in *reachable*. Returns `true` if the query succeeded.
    private def query_sentinel_for_peers(sentinel_uri : URI, reachable : ::Set(String)) : Bool
      conn = nil
      begin
        conn = open_sentinel_connection(sentinel_uri)
        self.class.parse_sentinel_list(conn.run({"sentinel", "sentinels", @master_name})).each do |info|
          # Build a clean sentinel URI — only scheme/auth from the known sentinel,
          # no path so we don't accidentally send SELECT to a sentinel node.
          register_sentinel(URI.new(
            scheme: sentinel_uri.scheme || "redis",
            host: info.ip,
            port: info.port,
            user: sentinel_uri.user,
            password: sentinel_uri.password,
          ))
          reachable << "#{info.ip}:#{info.port}"
        end
        true
      rescue ex
        Log.debug &.emit "Could not query sentinel for peers",
          sentinel: "#{sentinel_uri.host}:#{sentinel_uri.port}",
          error: ex.message.to_s
        false
      ensure
        conn.try { |c| c.close rescue nil }
      end
    end

    private def start_pubsub_watcher : Nil
      spawn do
        until closed?
          begin
            try_sentinel_pubsub
          rescue ex
            Log.warn &.emit "Sentinel pub/sub error, will reconnect", error: ex.message.to_s
            sleep 1.second unless closed?
          end
        end
      end
    end

    # Tries each sentinel in turn until one accepts a subscription. Returns as
    # soon as the subscription ends (cleanly or via error) so the outer loop can
    # immediately reconnect. Sleeps 1 s when every sentinel is unreachable, or
    # when a subscription connected but dropped almost immediately — otherwise
    # a flapping sentinel would cause a tight, unbounded reconnect loop.
    private def try_sentinel_pubsub : Nil
      registry_snapshot.each do |sentinel_uri|
        break if closed?
        connected_at = instant_time
        next unless subscribe_to_sentinel(sentinel_uri)
        sleep 1.second if !closed? && instant_time - connected_at < 1.second
        return
      end
      sleep 1.second unless closed?
    end

    # Opens a subscription to *sentinel_uri* and blocks until the connection
    # closes. Returns `true` if a connection was established (even if it later
    # dropped), `false` if the connection could not be made at all.
    # The connection is always closed in `ensure`, preventing leaks.
    private def subscribe_to_sentinel(sentinel_uri : URI) : Bool
      conn = open_sentinel_connection(sentinel_uri)
      @mutex.synchronize { @pubsub_conn = conn }
      return false if closed?
      conn.subscribe("+switch-master") do |sub, _|
        sub.on_message { |_, msg| handle_switch_master(msg) unless closed? }
      end
      true
    rescue ex
      Log.debug &.emit "Could not subscribe to sentinel",
        sentinel: "#{sentinel_uri.host}:#{sentinel_uri.port}",
        error: ex.message.to_s
      false
    ensure
      conn.try { |c| c.close rescue nil }
      @mutex.synchronize { @pubsub_conn = nil }
    end

    private def handle_switch_master(message : String) : Nil
      change = self.class.parse_switch_master_message(message)
      swap_master_if_changed(change.new_host, change.new_port) if change.master_name == @master_name
    rescue ex
      Log.warn &.emit "Error handling +switch-master", payload: message, error: ex.message.to_s
    end

    # Runs *block* on a background fiber at *interval* until closed. Each run
    # happens on its own fiber so a slow call (e.g. several unreachable
    # sentinels) can't delay the timer itself; a run still in flight when the
    # next tick fires is skipped rather than stacked.
    private def start_watcher_fiber(interval : Time::Span, label : String, &block : -> Nil) : Nil
      running = false
      spawn do
        until closed?
          sleep interval
          next if closed? || running
          running = true
          spawn do
            begin
              block.call
            rescue ex
              Log.warn &.emit "#{label} failed", error: ex.message.to_s
            ensure
              running = false
            end
          end
        end
      end
    end

    private def start_poll_watcher : Nil
      start_watcher_fiber(@check_interval, "Sentinel poll heartbeat") do
        new_uri = discover_master_uri
        swap_master_if_changed(new_uri.host.not_nil!, new_uri.port.not_nil!)
      end
    end

    private def start_sentinel_registry_refresh : Nil
      start_watcher_fiber(@sentinel_refresh_interval, "Sentinel registry refresh") do
        refresh_sentinel_registry
      end
    end
  end
end
