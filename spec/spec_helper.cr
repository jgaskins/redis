require "spec"
require "uuid"

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

module Spec::Expectations
  def be_within(delta, of expected)
    be_close(expected, delta)
  end
end
