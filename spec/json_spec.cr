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

private macro test(name, focus = false)
  it {{name}}, focus: {{focus}} do
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

  test "sets JSON objects only if they do not exist" do
    redis.json.set(key, ".", {foo: "bar"}, nx: true).should_not be_nil
    redis.json.set(key, ".", {foo: "bar"}, nx: true).should be_nil
    redis.json.set(key, ".", {foo: "bar"}, nx: true).should be_nil
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

  test "clears JSON values" do
    redis.json.set key, ".", {values: [1], count: 1234}

    redis.json.clear key, ".values"
    redis.json.get(key, ".values", as: Array(Int64)).not_nil!.should be_empty

    redis.json.clear key, ".count"
    redis.json.get(key, ".count", as: Int64).should eq 0
  end

  test "deletes keys from a JSON object" do
    redis.json.set key, ".", {values: [1], count: 1234}

    redis.json.del key, ".values"
    redis.json.get(key, ".").should eq({count: 1234}.to_json)
  end

  test "toggles a key in a JSON object" do
    json = redis.json
    json.set key, ".", {bool: true}

    json.toggle(key, "$.bool").should eq [0]
    json.get(key).should eq({bool: false}.to_json)
    json.toggle(key, "$.bool").should eq [1]
    json.get(key).should eq({bool: true}.to_json)
  end

  test "appends a value to an array" do
    redis.json.set key, ".", {values: [1]}

    redis.json.arrappend(key, ".values", 2).should eq 2 # now has 2 elements

    redis.json.get(key, ".values", as: Array(Int64)).should eq [1, 2]
  end

  test "appends an array of values to an array" do
    redis.json.set key, ".", {values: [1]}

    value = redis.json.arrappend(key, ".values", values: [2, 3])

    value.should eq 3 # now has 3 elements
    redis.json.get(key, ".values", as: Array(Int64)).should eq [1, 2, 3]
  end

  test "finds the index of a value in a JSON array" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    redis.json.arrindex(key, ".values", 3).should eq 2
  end

  test "finds the index of a value in a JSON array in a specified range" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    redis.json.arrindex(key, ".values", 3, between: 1..2).should eq 2
    redis.json.arrindex(key, ".values", 3, between: 1...2).should eq -1
    redis.json.arrindex(key, ".values", 3, between: 0..1).should eq -1
  end

  test "inserts a value into a JSON array" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    result = redis.json.arrinsert(key, ".values", index: 2, value: 4)

    result.should eq 4
    redis.json.get(key, ".values", as: Array(Int64)).should eq [1, 2, 4, 3]
  end

  test "inserts many values into a JSON array" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    result = redis.json.arrinsert(key, ".values", index: 2, values: [4, 5])

    result.should eq 5
    redis.json.get(key, ".values", as: Array(Int64)).should eq [1, 2, 4, 5, 3]
  end

  test "gets the length of a JSON array" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    redis.json.arrlen(key, ".values").should eq 3
  end

  test "removes and returns the last element in an array" do
    redis.json.set key, ".", {values: [1, 2, 3]}

    redis.json.arrpop(key, ".values").should eq 3.to_json
    redis.json.arrlen(key, ".values").should eq 2

    redis.json.arrpop(key, ".values", as: Int64).should eq 2
    redis.json.arrlen(key, ".values").should eq 1
  end
end
