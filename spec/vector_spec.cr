require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Commands::Vector do
  describe "#vadd" do
    test "adds a new element to the vector set" do
      redis.vadd(key, [1.234f32, 2.345f32, 3.456f32], "stuff")
        .should eq 1 # Added
      redis.vadd(key, vector(3), "stuff")
        .should eq 0 # Same element, but not added
    end

    test "adds an element and reduces the vector to the specified dimensionality" do
      redis.vadd(key, vector(3), "stuff", reduce: 2).should eq 1
      redis.vdim(key).should eq 2
    end

    test "adds an element with given attributes" do
      redis.vadd key, vector(3), "stuff", setattr: {lang: "en"}

      JSON.parse(redis.vgetattr(key, "stuff"))["lang"].should eq "en"
    end
  end

  describe "#vcard" do
    test "returns the number of elements in the vector set" do
      redis.vcard(key).should eq 0
      redis.vadd key, vector(3), "stuff"
      redis.vcard(key).should eq 1
      redis.vadd key, vector(3), "stuff"
      redis.vcard(key).should eq 1
      redis.vadd key, vector(3), "things"
      redis.vcard(key).should eq 2
      redis.vadd key, vector(3), "things"
      redis.vcard(key).should eq 2
    end
  end

  describe "#vdim" do
    test "returns the dimensionality of a vector set" do
      redis.vadd key, vector(50), "asdf"

      redis.vdim(key).should eq 50
    end
  end

  describe "#vemb" do
    test "returns the vector for the given element" do
      vector = [1.234f32, 2.345f32, 3.456f32]
      redis.vadd key, vector, "stuff"

      result = redis.vemb(key, "stuff")

      # We can't compare directly because Redis encodes floats as strings
      result.each_with_index do |value, index|
        value.as(String).to_f.should be_within 0.01, of: vector[index]
      end
    end
  end

  describe "#vgetattr" do
    test "returns a JSON string containing the attributes of the given element" do
      redis.vadd key, vector(3), "stuff", setattr: {lang: "en"}
      redis.vgetattr(key, "stuff").should eq({lang: "en"}.to_json)
    end

    test "deserializes attributes from JSON to the specified type" do
      redis.vadd key, vector(3), "stuff", setattr: {lang: "en"}

      if attrs = redis.vgetattr(key, "stuff", as: VectorSpec::ElementAttrs)
        attrs.lang.en?.should eq true
      else
        raise "vgetattr didn't return any attributes"
      end

      # Uncommenting this should fail compilation because we can't deserialize
      # this overload on a pipeline. It's infeasible to test it for real because
      # it would prevent the test suite from compiling.
      # redis.pipeline &.vgetattr(key, "stuff", as: VectorSpec::ElementAttrs)
    end
  end

  describe "#vsim" do
    test "returns elements closest to a given element with a given proximity" do
      redis.vadd key, [1.234f32, 2.345f32, 3.456f32], "included"
      redis.vadd key, [1.234f32, 2.345f32, 3.557f32], "example"
      redis.vadd key, [-1.234f32, -2.345f32, -3.456f32], "excluded"

      similar = redis.vsim(key, "example", epsilon: 0.01)

      similar.should contain "included"
      similar.should_not contain "excluded"
    end

    test "returns elements closest to a given element with a max count" do
      redis.vadd key, [1.234f32, 2.345f32, 3.456f32], "included"
      redis.vadd key, [1.234f32, 2.345f32, 3.557f32], "example"
      redis.vadd key, [-1.234f32, -2.345f32, -3.456f32], "excluded"

      similar = redis.vsim(key, "example", count: 2)

      similar.should contain "included"
      similar.should_not contain "excluded"
    end

    test "returns elements closest to a given vector with a given proximity" do
      redis.vadd key, [1.234f32, 2.345f32, 3.456f32], "included"
      redis.vadd key, [1.234f32, 2.345f32, 3.557f32], "example"
      redis.vadd key, [-1.234f32, -2.345f32, -3.456f32], "excluded"

      similar = redis.vsim(key, [1.234f32, 2.345f32, 3.557f32], epsilon: 0.01)

      similar.should contain "included"
      similar.should_not contain "excluded"
    end

    test "returns elements closest to a given vector with a max count" do
      redis.vadd key, [1.234f32, 2.345f32, 3.456f32], "included"
      redis.vadd key, [1.234f32, 2.345f32, 3.557f32], "example"
      redis.vadd key, [-1.234f32, -2.345f32, -3.456f32], "excluded"

      similar = redis.vsim(key, [1.234f32, 2.345f32, 3.557f32], count: 2)

      similar.should contain "included"
      similar.should_not contain "excluded"
    end
  end
end

private def vector(size : Int)
  Array.new(size) { rand.to_f32 }
end

struct VectorSpec::ElementAttrs
  include JSON::Serializable
  getter lang : Language

  enum Language
    EN
    SP
    FR
    AR
  end
end
