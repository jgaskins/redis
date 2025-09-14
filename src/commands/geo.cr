# Namespace for types that support `Redis::Commands::Geo` commands.
module Redis::Geo
  record Radius, magnitude : String, unit : Unit do
    def self.new(magnitude : Int, unit : Unit)
      new magnitude.to_s, unit
    end

    # :nodoc:
    def to_tuple
      {magnitude, unit.to_s}
    end
  end

  record Box, width : String, height : String, unit : Unit do
    def self.new(width : Number, height : Number, unit : Unit)
      new width.to_s, height.to_s, unit
    end

    # :nodoc:
    def to_tuple
      {width, height, unit.to_s}
    end
  end

  record Member, longitude : String, latitude : String, member : String

  enum Unit
    M
    KM
    FT
    MI
  end

  enum Sort
    ASC
    DESC
  end
end

module Redis::Commands::Geo
  def geoadd(key : String, *entries : String, nx = nil, xx = nil, ch = nil)
    command = {"geoadd", key}
    command += {"nx"} if nx
    command += {"xx"} if xx
    command += {"ch"} if ch
    command += entries

    run command
  end

  def geoadd(key : String, entries : Enumerable(Redis::Geo::Member), nx = nil, xx = nil, ch = nil)
    return 0i64 if entries.empty?

    command = Array(String).new(5 + entries.size * 3)
    command << "geoadd" << key
    command << "nx" if nx
    command << "xx" if xx
    command << "ch" if ch
    entries.each do |entry|
      command << entry.longitude << entry.latitude << entry.member
    end

    run command
  end

  def geopos(key : String, *members : String)
    run({"geopos", key, *members})
  end

  def geodist(key : String, member1 : String, member2 : String, unit : Redis::Geo::Unit? = nil)
    command = {"geodist", key, member1, member2}
    command += {unit.to_s} if unit
    run command
  end

  def geosearch(
    key : String,
    *,
    fromlonlat lonlat : Tuple(String, String),
    byradius radius : Redis::Geo::Radius,
    sort : Redis::Geo::Sort? = nil,
    count : Int | String | Nil = nil,
    withcoord : Bool = false,
    withdist : Bool = false,
  )
    command = {
      "geosearch",
      key,
      "fromlonlat",
      *lonlat,
      "byradius",
      *radius.to_tuple,
    }

    command += {sort.to_s} if sort
    command += {"count", count.to_s} if count
    command += {"withcoord"} if withcoord
    command += {"withdist"} if withdist

    run command
  end

  def geosearch(
    key : String,
    *,
    fromlonlat lonlat : Tuple(String, String),
    bybox box : Redis::Geo::Box,
    sort : Redis::Geo::Sort? = nil,
    count : Int | String | Nil = nil,
    withcoord : Bool = false,
    withdist : Bool = false,
  )
    command = {
      "geosearch",
      key,
      "fromlonlat",
      *lonlat,
      "bybox",
      *box.to_tuple,
    }

    command += {sort.to_s} if sort
    command += {"count", count.to_s} if count
    command += {"withcoord"} if withcoord
    command += {"withdist"} if withdist

    run command
  end

  def geosearch(
    key : String,
    *,
    frommember member : String,
    byradius radius : Redis::Geo::Radius,
    sort : Redis::Geo::Sort? = nil,
    count : Int | String | Nil = nil,
    withcoord : Bool = false,
    withdist : Bool = false,
  )
    command = {
      "geosearch",
      key,
      "frommember",
      member,
      "byradius",
      *radius.to_tuple,
    }

    command += {sort.to_s} if sort
    command += {"count", count.to_s} if count
    command += {"withcoord"} if withcoord
    command += {"withdist"} if withdist

    run command
  end

  def geosearch(
    key : String,
    *,
    frommember member : String,
    bybox box : Redis::Geo::Box,
    sort : Redis::Geo::Sort? = nil,
    count : Int | String | Nil = nil,
    withcoord : Bool = false,
    withdist : Bool = false,
  )
    command = {
      "geosearch",
      key,
      "frommember",
      member,
      "bybox",
      *box.to_tuple,
    }

    command += {sort.to_s} if sort
    command += {"count", count.to_s} if count
    command += {"withcoord"} if withcoord
    command += {"withdist"} if withdist

    run command
  end
end
