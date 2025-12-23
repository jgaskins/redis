# Namespace for types that support `Redis::Commands::Geo` commands.
module Redis::Geo
  # Represent a radius for a `geosearch` command.
  #
  # ```
  # redis.geosearch "drivers",
  #   fromlonlat: {longitude, latitude},
  #   byradius: Redis::Geo::Radius.new(5, :mi)
  # ```
  record Radius, magnitude : String, unit : Unit do
    def self.new(magnitude : Number, unit : Unit)
      new magnitude.to_s, unit
    end

    # :nodoc:
    def to_tuple
      {magnitude, unit.to_s}
    end
  end

  # Represent a bounding box for a `geosearch` command.
  #
  # ```
  # redis.geosearch "drivers",
  #   fromlonlat: {longitude, latitude},
  #   bybox: Redis::Geo::Box.new(10, 5, :mi) # 10 miles wide by 5 miles heigh
  # ```
  record Box, width : String, height : String, unit : Unit do
    def self.new(width : Number, height : Number, unit : Unit)
      new width.to_s, height.to_s, unit
    end

    # :nodoc:
    def to_tuple
      {width, height, unit.to_s}
    end
  end

  # Object that makes adding entries to a geospatial index more expressive than
  # using variadic positional arguments.
  #
  # ```
  # redis.geoadd "drivers", [
  #   Redis::Geo::Member.new(
  #     longitude: longitude,
  #     latitude: latitude,
  #     member: driver.id,
  #   ),
  # ]
  # ```
  record Member, longitude : String, latitude : String, member : String

  # Units of measurement for various geospatial commands
  enum Unit
    # Meters
    M

    # Kilometers
    KM

    # Feet
    FT

    # Miles
    MI
  end

  # Sort directions for `geosearch` comands.
  enum Sort
    # Sort results in ascending order
    ASC

    # Sort results in descending order
    DESC
  end
end

module Redis::Commands::Geo
  # Add `entries` to the geospatial index (backed by a `SortedSet`) stored in
  # `key` (updating them if they already exist) and return the number of entries
  # added to the index. Note that `entries` must be strings and must be in
  # groups of 3 in the following order: `longitude, latitude, member`.
  #
  # Setting the following options to `true` will have the corresponding effect
  # on the command:
  #
  # - `nx` will only add the entries if they do *not* already exist in the geospatial index; mutually exclusive with `xx`
  # - `xx` will only add the entries if they *do* already exist in the geospatial index; mutually exclusive with `nx`
  # - `ch` alters the return value to represent the number of entries *changed* rather than the number of entries *added*
  #
  # ```
  # redis.geoadd "places",
  #   longitude, latitude, restaurant.id.to_s,
  #   nx: true
  # ```
  def geoadd(key : String, *entries : String, nx = nil, xx = nil, ch = nil)
    command = {"geoadd", key}
    command += {"nx"} if nx
    command += {"xx"} if xx
    command += {"ch"} if ch
    command += entries

    run command
  end

  # Add `entries` to the geospatial index (backed by a `SortedSet`) stored in
  # `key` (updating them if they already exist) and return the number of entries
  # added to the index. This overload is provided to avoid having to remember
  # the order of the `entries` arguments and also to provide for a dynamic
  # number of entries that is not known at compile time.
  #
  # Setting the following options to `true` will have the corresponding effect
  # on the command:
  #
  # - `nx` will only add the entries if they do *not* already exist in the geospatial index; mutually exclusive with `xx`
  # - `xx` will only add the entries if they *do* already exist in the geospatial index; mutually exclusive with `nx`
  # - `ch` alters the return value to represent the number of entries *changed* rather than the number of entries *added*
  #
  # ```
  # redis.geoadd "places",
  #   {
  #     Redis::Geo::Member.new(
  #       longitude: longitude,
  #       latitude: latitude,
  #       member: restaurant.id.to_s,
  #     ),
  #   },
  #   longitude, latitude, restaurant.id.to_s,
  #   nx: true
  # ```
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

  # Returns `[longitude, latitude]` for all `members` provided.
  def geopos(key : String, *members : String)
    run({"geopos", key, *members})
  end

  # Returns `[longitude, latitude]` for all `members` provided.
  def geopos(key : String, members : Enumerable(String))
    command = Array(String).new(initial_capacity: 2 + members.size)
    command << "geopos" << key
    command.concat members

    run command
  end

  # Return the crow-flies distance between `member1` and `member2` of the
  # geospatial index stored in `key` in the specified `unit`, defaulting to
  # meters.
  #
  # ```
  # redis.geoadd "nfl-stadiums",
  #   "-76.622852", "39.278093", "ravens",
  #   "-94.483899", "39.048868", "chiefs"
  #
  # # About 956 miles between the Baltimore Ravens and Kansas City Chiefs stadiums
  # redis.geodist("nfl-stadiums", "ravens", "chiefs", :mi) # => "955.6877"
  # ```
  def geodist(key : String, member1 : String, member2 : String, unit : Redis::Geo::Unit? = nil)
    command = {"geodist", key, member1, member2}
    command += {unit.to_s} if unit
    run command
  end

  # Search the geospatial index stored in `key` and return the members within `radius` from `lonlat`. If `count` is specified, at most that many entries will be returned. You can specify `sort` as `:asc` or `:desc` to sort them in ascending or descending order, respectively. `withcoord` also includes the coordinates as a `[longitude, latitude]` array alongside the member string. `withdist` will also return the distance between the member and `lonlat`, using the same unit of measurement specified in `radius`.
  #
  # ```
  # redis.geoadd "driver-locations",
  #   "-81.683963", "41.509156", "driver1", # downtown
  #   "-81.601267", "41.556502", "driver2", # bratenahl
  #   "-81.687367", "41.468931", "driver3"  # the house from A Christmas Story
  #
  # redis.geosearch "driver-locations",
  #   fromlonlat: {"-81.688997", "41.471040"},
  #   byradius: Redis::Geo::Radius.new(5, :mi), # 5 miles
  #   sort: :asc,
  #   withcoord: true,
  #   withdist: true
  # # => [["driver3", "0.1685", ["-81.6873648762703", "41.468931655993906"]],
  # #     ["driver1", "2.6471", ["-81.68396383523941", "41.50915514607146"]]]
  # ```
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

  # Search the geospatial index stored in `key` and return the members within the bounding `box` around `longlat`. If `count` is specified, at most that many entries will be returned. You can specify `sort` as `:asc` or `:desc` to sort them in ascending or descending order, respectively. `withcoord` also includes the coordinates as a `[longitude, latitude]` array alongside the member string. `withdist` will also return the distance between the member and `lonlat`, using the same unit of measurement specified in `box`.
  #
  # Bear in mind that `lonlat` is in the *center* of `Redis::Geo::Box`, so if you need something within 10 miles north/south/east/west, the box needs to have a `width` and `height` of 20 miles.
  #
  # ```
  # redis.geoadd "driver-locations",
  #   "-81.683963", "41.509156", "driver1", # downtown
  #   "-81.601267", "41.556502", "driver2", # bratenahl
  #   "-81.687367", "41.468931", "driver3"  # the house from A Christmas Story
  #
  # redis.geosearch "driver-locations",
  #   fromlonlat: {"-81.688997", "41.471040"},
  #   bybox: Redis::Geo::Box.new(1, 5, :mi), # 1 mile wide by 5 miles tall
  #   sort: :asc,
  #   withcoord: true,
  #   withdist: true
  # # => [["driver3", "0.1685", ["-81.6873648762703", "41.468931655993906"]],
  # #     ["driver1", "2.6471", ["-81.68396383523941", "41.50915514607146"]]]
  # ```
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

  # Search the geospatial index stored in `key` and return the members within `radius` from `member`. If `count` is specified, at most that many entries will be returned. You can specify `sort` as `:asc` or `:desc` to sort them in ascending or descending order, respectively. `withcoord` also includes the coordinates as a `[longitude, latitude]` array alongside the member string. `withdist` will also return the distance between the member and `lonlat`, using the same unit of measurement specified in `radius`.
  #
  # Note that the `member` you passed in will be returned with a distance of `"0.0000"`.
  #
  # ```
  # redis.geoadd "user-locations",
  #   "-81.683963", "41.509156", "user1", # downtown
  #   "-81.601267", "41.556502", "user2", # bratenahl
  #   "-81.687367", "41.468931", "user3"  # the house from A Christmas Story
  #
  # redis.geosearch "user-locations",
  #   frommember: "user1",
  #   byradius: Redis::Geo::Radius.new(5, :mi), # 5 miles
  #   sort: :asc,
  #   withdist: true
  # # => [["user1", "0.0000"], ["user3", "2.7855"]]
  # ```
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

  # Search the geospatial index stored in `key` and return the members within the bounding `box` around `member`. If `count` is specified, at most that many entries will be returned. You can specify `sort` as `:asc` or `:desc` to sort them in ascending or descending order, respectively. `withcoord` also includes the coordinates as a `[longitude, latitude]` array alongside the member string. `withdist` will also return the distance between the member and `lonlat`, using the same unit of measurement specified in `radius`.
  #
  # Note that the `member` you passed in will be returned, and if you pass `withdist: true`, will have a distance of `"0.0000"`.
  #
  # ```
  # redis.geoadd "user-locations",
  #   "-81.683963", "41.509156", "user1", # downtown
  #   "-81.601267", "41.556502", "user2", # bratenahl
  #   "-81.687367", "41.468931", "user3"  # the house from A Christmas Story
  #
  # redis.geosearch "user-locations",
  #   frommember: "user1",
  #   bybox: Redis::Geo::Box.new(1, 5, :mi), # 1 mile wide by 5 miles tall
  #   sort: :asc,
  #   withdist: true
  # # => [["user1", "0.0000"], ["user3", "2.7855"]]
  # ```
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
