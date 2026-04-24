require "./commands"

module Redis
  struct TDigest
    private getter redis : Commands

    def initialize(@redis)
    end

    def create(key : String, compression : Int | String | Nil = nil)
      command = {"tdigest.create", key}
      command += {"compression", compression.to_s} if compression

      @redis.run command
    end

    def add(key : String, *values : String)
      @redis.run({"tdigest.add", key} + values.map(&.to_s))
    end

    def add(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.add" << key
      command.concat values

      @redis.run command
    end

    def quantile(key : String, *quantiles : String)
      @redis.run({"tdigest.quantile", key} + quantiles)
    end

    def quantile(key : String, quantiles : Enumerable(String))
      command = Array(String).new(2 + quantiles.size)
      command << "tdigest.quantile" << key
      command.concat quantiles

      @redis.run command
    end

    def rank(key : String, *values : String)
      @redis.run({"tdigest.rank", key} + values)
    end

    def rank(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.rank" << key
      command.concat values

      @redis.run command
    end

    def revrank(key : String, *values : String)
      @redis.run({"tdigest.revrank", key} + values)
    end

    def revrank(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.revrank" << key
      command.concat values

      @redis.run command
    end

    def cdf(key : String, *values : String)
      @redis.run({"tdigest.cdf", key} + values)
    end

    def cdf(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.cdf" << key
      command.concat values

      @redis.run command
    end

    def byrank(key : String, *values : String)
      @redis.run({"tdigest.byrank", key} + values)
    end

    def byrank(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.byrank" << key
      command.concat values

      @redis.run command
    end

    def byrevrank(key : String, *values : String)
      @redis.run({"tdigest.byrevrank", key} + values)
    end

    def byrevrank(key : String, values : Enumerable(String))
      command = Array(String).new(2 + values.size)
      command << "tdigest.byrevrank" << key
      command.concat values

      @redis.run command
    end

    def reset(key : String)
      @redis.run({"tdigest.reset", key})
    end

    def max(key : String)
      @redis.run({"tdigest.max", key})
    end

    def min(key : String)
      @redis.run({"tdigest.min", key})
    end

    def merge(
      destination_key : String,
      source_keys : Array(String),
      compression : String | Int | Nil = nil,
      override : Bool = false,
    )
      command = Array(String).new(5 + source_keys.size)
      command << "tdigest.merge" << destination_key << source_keys.size.to_s
      command.concat source_keys
      command << "compression" << compression.to_s if compression
      command << "override" if override

      @redis.run command
    end

    def trimmed_mean(key : String, low_cut_quantile : String, high_cut_quantile : String)
      @redis.run({"tdigest.trimmed_mean", key, low_cut_quantile, high_cut_quantile})
    end
  end

  module Commands
    def tdigest
      TDigest.new self
    end
  end
end
