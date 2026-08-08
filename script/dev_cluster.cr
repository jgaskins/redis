# Spins up an ephemeral Redis cluster on 127.0.0.1:6380-6385 (3 masters, each
# with 1 replica), waits for input on stdin, then tears the cluster down and
# removes the temporary data directory.
#
# Usage:
#   crystal run script/dev_cluster.cr
#
# Once the cluster is ready, point the cluster spec at it with:
#   REDIS_CLUSTER_URL=redis://127.0.0.1:6380 crystal spec spec/cluster_spec.cr

require "file_utils"
require "socket"

HOST  = "127.0.0.1"
PORTS = (6380..6385).to_a

base_dir = File.tempname("redis-cluster")
Dir.mkdir_p(base_dir)

class Node
  getter port : Int32
  getter dir : String
  getter process : Process

  def initialize(@port : Int32, base_dir : String)
    @dir = File.join(base_dir, port.to_s)
    Dir.mkdir_p(@dir)

    @process = Process.new(
      "redis-server",
      [
        "--port", port.to_s,
        "--bind", "127.0.0.1",
        "--cluster-enabled", "yes",
        "--cluster-config-file", "nodes.conf",
        "--cluster-node-timeout", "2000",
        "--appendonly", "no",
        "--save", "",
        "--dir", @dir,
        "--logfile", File.join(@dir, "redis.log"),
        "--daemonize", "no",
      ],
      output: Process::Redirect::Close,
      error: Process::Redirect::Close,
    )
  end

  def address : String
    "#{HOST}:#{port}"
  end

  def stop : Nil
    @process.signal(Signal::TERM) rescue nil
    @process.wait rescue nil
  end
end

def wait_for_port(port : Int32, timeout : Time::Span = 10.seconds) : Nil
  deadline = Time.instant + timeout
  loop do
    begin
      TCPSocket.new(HOST, port).close
      return
    rescue IO::Error | Socket::Error
      if Time.instant > deadline
        raise "Timed out waiting for port #{port} to accept connections"
      end
      sleep 100.milliseconds
    end
  end
end

puts "Spawning Redis nodes (data dir: #{base_dir})..."
nodes = PORTS.map { |port| Node.new(port, base_dir) }

torn_down = false
teardown = ->do
  unless torn_down
    torn_down = true
    puts "\nShutting down cluster..."
    nodes.each(&.stop)
    FileUtils.rm_rf(base_dir) rescue nil
  end
end

at_exit { teardown.call }

{Signal::INT, Signal::TERM}.each do |signal|
  signal.trap do
    teardown.call
    exit 0
  end
end

PORTS.each { |port| wait_for_port(port) }
puts "All nodes accepting connections."

puts "Forming cluster..."
status = Process.run(
  "redis-cli",
  ["--cluster", "create"] + nodes.map(&.address) + ["--cluster-replicas", "1", "--cluster-yes"],
  output: Process::Redirect::Inherit,
  error: Process::Redirect::Inherit,
)
unless status.success?
  abort "redis-cli --cluster create exited with code #{status.exit_code}"
end

puts
puts "Redis cluster is ready:"
nodes.each { |node| puts "  #{node.address}" }
puts
puts "Entrypoint: REDIS_CLUSTER_URL=redis://#{HOST}:#{PORTS.first}"
puts
puts "Press Enter to tear down the cluster..."
STDIN.gets
