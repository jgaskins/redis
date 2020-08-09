require "./spec_helper"
require "uuid"

require "../src/connection"

module Redis
  describe Connection do
    it "reconnects after being disconnected" do
      redis = Connection.new

      redis.get "foo"

      redis.close

      redis.get "foo"
    end

    it "retries transactions" do
      redis = Connection.new
      key = UUID.random.to_s

      second_pass = false

      result = redis.multi do |txn|
        txn.get key
        txn.incr key

        unless second_pass
          redis.close
          second_pass = true
        end

        txn.get key
      end

      result.should eq [nil, 1, "1"]
      second_pass.should be_true

      redis.del key
    end
  end
end
