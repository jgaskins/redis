require "./spec_helper"

require "../src/redis"

redis = Redis::Client.new
define_test redis

describe Redis::Commands::Geo do
  test "adds a member to a geospatial index and retrieves it by name" do
    redis.geoadd(key, "42", "69", "foo", "12", "34", "bar").should eq 2

    foo, bar = redis.geopos(key, "foo", "bar")
    x, y = foo.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 42
    y.should be_within 0.00001, of: 69
    x, y = bar.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 12
    y.should be_within 0.00001, of: 34

    foo, bar = redis.geopos(key, {"foo", "bar"})
    x, y = foo.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 42
    y.should be_within 0.00001, of: 69
    x, y = bar.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 12
    y.should be_within 0.00001, of: 34

    foo, bar = redis.geopos(key, %w[foo bar])
    x, y = foo.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 42
    y.should be_within 0.00001, of: 69
    x, y = bar.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 12
    y.should be_within 0.00001, of: 34
  end

  test "adds `Redis::Geo::Member`s to a geospatial index" do
    added = redis.geoadd key, [
      Redis::Geo::Member.new(
        latitude: "42",
        longitude: "69",
        member: "foo",
      ),
      Redis::Geo::Member.new(
        latitude: "12",
        longitude: "34",
        member: "bar",
      ),
    ]

    added.should eq 2
    foo, bar = redis.geopos(key, "foo", "bar")
    x, y = foo.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 69
    y.should be_within 0.00001, of: 42
    x, y = bar.as(Array).map(&.as(String).to_f)
    x.should be_within 0.00001, of: 34
    y.should be_within 0.00001, of: 12
  end

  test "gets the distance between two members" do
    redis.geoadd key,
      "42", "69", "foo",
      "12", "34", "bar"

    redis.geodist(key, "foo", "bar", :mi).to_f.should be_within 0.0001, of: 2681.5198
    redis.geodist(key, "foo", "bar", :km).to_f.should be_within 0.0001, of: 4315.4770
    redis.geodist(key, "foo", "bar", :m).to_f.should be_within 0.0001, of: 4315477.0479
    redis.geodist(key, "foo", "bar", :ft).to_f.should be_within 0.0001, of: 14158389.2648
  end

  test "searches a geospatial index by coordinates (FROMLONLAT, BYRADIUS)" do
    redis.geoadd key,
      "0", "0", "a",
      "0", "0.1", "b",
      "0.1", "0", "c",
      "0.1", "0.1", "d"

    results = redis.geosearch key,
      fromlonlat: {"0", "0"},
      byradius: Redis::Geo::Radius.new(7, :mi),
      sort: :asc,
      count: 4

    results.size.should eq 3
    results.should contain "a"
    results.should contain "b"
    results.should contain "c"
    results.should_not contain "d" # Excluded because it's too far away
  end

  test "searches a geospatial index by coordinates (FROMMEMBER, BYRADIUS)" do
    redis.geoadd key,
      "0", "0", "a",
      "0", "0.1", "b",
      "0.1", "0", "c",
      "0.2", "0.2", "d"

    results = redis.geosearch key,
      frommember: "a",
      byradius: Redis::Geo::Radius.new(6, :mi),
      sort: :asc

    results.should eq %w[a]
  end

  test "searches a geospatial index by coordinates (FROMLONLAT, BYBOX)" do
    redis.geoadd key,
      "0", "0", "a",
      "0", "0.1", "b",
      "0.1", "0", "c",
      "0.2", "0.2", "d"

    wide_box = redis.geosearch key,
      fromlonlat: {"0", "0"},
      bybox: Redis::Geo::Box.new(14, 1, :mi)
    tall_box = redis.geosearch key,
      fromlonlat: {"0", "0"},
      bybox: Redis::Geo::Box.new(1, 14, :mi)

    wide_box.should eq %w[a c]
    tall_box.should eq %w[a b]
  end

  test "searches a geospatial index by coordinates (FROMMEMBER, BYBOX)" do
    redis.geoadd key,
      "0", "0", "a",
      "0", "0.1", "b",
      "0.1", "0", "c",
      "0.2", "0.2", "d"

    wide_box = redis.geosearch key,
      frommember: "a",
      bybox: Redis::Geo::Box.new(14, 1, :mi)
    tall_box = redis.geosearch key,
      frommember: "a",
      bybox: Redis::Geo::Box.new(1, 14, :mi)

    wide_box.should eq %w[a c]
    tall_box.should eq %w[a b]
  end
end
