# Namespace for types that support `Redis::Commands::Geo` commands.
module Redis::Geo
  record Radius, magnitude : String, unit : Unit do
    def self.new(magnitude : Int, unit : Unit)
      new magnitude.to_s, unit
    end

    def to_tuple
      {magnitude, unit.to_s}
    end

    enum Unit
      M
      KM
      FT
      MI
    end
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

  def geopos(key : String, *members : String)
    run({"geopos", key, *members})
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
end
