require "spec"
require "uuid"
require "../src/redis"

macro define_test(redis)
  private macro random_key
    UUID.random.to_s
  end

  private macro test(msg, **options, &block)
    it(\{{msg}}, \{{options.double_splat}}) do
      key = random_key

      begin
        \{{yield}}
      ensure
        redis.unlink key
      end
    end
  end
end

def default_keepalive_count
  # TODO: Verify these on non-macOS/Linux OSes
  {% if flag? :linux %}
    9
  {% else %}
    8
  {% end %}
end

struct TestRunner
  def initialize(@redis : Redis::Commands::Immediate)
  end

  def has_module?(name : String)
    module_info = @redis.info("modules")
    module_info.includes? "module:name=#{name}"
  end

  def server_version : Version
    Version[@redis.info("server").match!(/^redis_version:([\d\.]+)/m)[1]]
  end
end

struct Version
  include Comparable(self)

  getter major : Int32
  getter minor : Int32
  getter patch : Int32

  def self.[](string : String) : self
    split = {String, String, String}.from(string.split('.', 3))
    new(*split.map(&.to_i))
  end

  def initialize(@major, @minor, @patch)
  end

  def <=>(other : self) : Int32
    {major, minor, patch} <=> {other.major, other.minor, other.patch}
  end
end

module Spec::Expectations
  def be_within(delta, of expected)
    be_close(expected, delta)
  end
end
