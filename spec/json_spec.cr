require "./spec_helper"
require "uuid"
require "uuid/json"

require "../src/json"
require "../src/cluster"

struct Order
  include JSON::Serializable

  getter customer : Customer
  getter products : Array(LineItem)
  getter payment : Payment?

  struct Customer
    include JSON::Serializable

    getter name : String
    getter address : String
  end

  struct LineItem
    include JSON::Serializable

    getter product_id : UUID
    getter name : String
    getter quantity : Int32 = 1
    getter price_cents : Int32
  end

  struct Payment
    include JSON::Serializable

    getter token : String
    getter total_cents : Int32
  end
end

private macro test(name)
  it {{name}} do
    key = UUID.random.to_s

    begin
      {{yield}}
    ensure
      redis.del key
    end
  end
end

# redis = Redis::Cluster.new
redis = Redis::Client.new

describe Redis::JSON do
  test "sets and gets JSON objects" do
    redis.json.set key, ".", {foo: "bar"}

    redis.json.get(key).should eq({"foo" => "bar"}.to_json)
  end

  test "gets the value via a JSONPath" do
    redis.json.set key, ".", {
      id:       "123",
      customer: {
        name: "Jamie",
      },
    }

    redis.json.get(key, ".customer.name").should eq "Jamie".to_json
  end

  test "gets values and deserializes them as a given class" do
    redis.json.set key, ".", {
      customer: {
        name:    "Jamie",
        address: "123 Main St",
      },
      products: [
        {product_id: UUID.random, name: "Shirt", quantity: 1, price_cents: 123_45},
        {product_id: UUID.random, name: "Pants", quantity: 1, price_cents: 123_45},
        {product_id: UUID.random, name: "Socks", quantity: 2, price_cents: 123_45},
      ],
    }

    if order = redis.json.get(key, ".", as: Order)
      order.customer.name.should eq "Jamie"
      order.products[0].name.should eq "Shirt"
      order.payment.should be_nil
    else
      raise "Expected to get an order back, but got nothing"
    end
  end

  test "increments numbers" do
    redis.json.set key, ".", {
      customer: {
        name:    "Jamie",
        address: "123 Main St",
      },
      products: [
        {product_id: UUID.random, name: "Shirt", quantity: 1, price_cents: 123_45},
        {product_id: UUID.random, name: "Pants", quantity: 2, price_cents: 123_45},
        {product_id: UUID.random, name: "Socks", quantity: 4, price_cents: 123_45},
      ],
    }

    redis.json.numincrby(key, ".products[0].quantity", 1).should eq "2"
    redis.json.numincrby(key, ".products[1].quantity", 1, as: Int32).should eq 3
    # Increment *all* quantities on the order
    redis.json.numincrby(key, "$.products..quantity", 1, as: Array(Int32)).should eq [3, 4, 5]
  end
end
