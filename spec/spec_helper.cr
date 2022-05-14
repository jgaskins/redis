require "spec"

module Spec::Expectations
  def be_within(delta, of expected)
    be_close(expected, delta)
  end
end
