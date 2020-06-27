require "db/pool"

require "./connection"

module Redis
  class Client
    @pool : DB::Pool(Connection)

    # The client holds a pool of connections that expands and contracts as
    # needed.
    def initialize(*args, **kwargs)
      @pool = DB::Pool.new { Connection.new(*args, **kwargs) }
    end

    macro method_missing(call)
      @pool.checkout do |connection|
        connection.{{call}}
      end
    end
  end
end
