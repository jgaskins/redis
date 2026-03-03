require "json"

module Redis::Commands::Vector
  # Add `element` with its associated `vector` to the vector set stored in `key`.
  #
  # ```
  # redis.vadd "embeddings", vectorize(text), text
  # ```
  #
  # You can optionally reduce `vector` to a given dimensionality with the
  # `reduce` argument if you need to sacrifice precision to conserve space.
  #
  # ```
  # redis.vadd "embeddings", vectorize(text), text, reduce: 50
  # ```
  #
  # You can also set attributes for this vector. Any JSON-serializable value
  # can be passed into the `setattr` argument.
  #
  # ```
  # redis.vadd "embeddings", vectorize(text), text,
  #   setattr: {
  #     lang: "en",
  #   }
  # ```
  def vadd(
    key : String,
    vector : Enumerable(Float32),
    element : String,
    *,
    reduce : Int | String | Nil = nil,
    setattr attributes = nil,
  )
    encoded = VectorEncoder.new.call(vector)

    command = {"vadd", key}
    command += {"reduce", reduce.to_s} if reduce
    command += {"fp32", encoded, element}
    command += {"setattr", attributes.to_json} if attributes

    run command
  end

  # Return the number of vectors contained in the vector set stored in `key`,
  # also referred to as the cardinality of the set.
  def vcard(key : String)
    run({"vcard", key})
  end

  # Return the dimensionality of the vectors in the vector set stored in `key`.
  def vdim(key : String)
    run({"vdim", key})
  end

  # Return the vector for `element` stored in the vector set in `key`.
  def vemb(key : String, element : String)
    run({"vemb", key, element})
  end

  # Get the attributes for `element` stored in `key`, deserializing them as the
  # type `T`.
  #
  # ```
  # struct Properties
  #   include JSON::Serializable
  #
  #   getter post_id : Int64
  #   getter author_id : Int64
  # end
  #
  # if properties = redis.vgetattr("embeddings", post_text, as: Properties)
  #   author = AuthorQuery.new.find(properties.author_id)
  # end
  # ```
  #
  # NOTE: You can only run this method against `Immediate` types, such as
  # `Client`, `Connection`, `ReplicationClient`, or `Cluster`. Calling this
  # method on a `Pipeline` or `Transaction` will result in a compile-time error.
  # If you need to run this in a pipeline, you will need to deserialize the
  # string manually with the other `vgetattr` overload.
  def vgetattr(key : String, element : String, as type : T.class) : T? forall T
    {% if @type <= Redis::Commands::Immediate %}
      if string = vgetattr(key, element)
        T.from_json string
      end
    {% else %}
      {% raise "Can only deserialize attributes with `vgetattr` when calling it on a `Redis::Commands::Immediate` type, such as `Client`, `Connection`, `ReplicationClient`, or `Cluster`. It can't be called on `Pipeline` or `Transaction` objects." %}
    {% end %}
  end

  # Get the attributes for `element` stored in `key` as a JSON string.
  #
  # ```
  # struct Properties
  #   include JSON::Serializable
  #
  #   getter post_id : Int64
  #   getter author_id : Int64
  # end
  #
  # if (json = redis.vgetattr("embeddings", post_text)) && (properties = Properties.from_json(json))
  #   author = AuthorQuery.new.find(properties.author_id)
  # end
  # ```
  def vgetattr(key : String, element : String)
    run({"vgetattr", key, element})
  end

  # Return an array of elements between `start` and `end` in the vector set
  # stored in `key`, optionally limiting the result size to `count`.
  #
  # The values for `start` and `end` have identical semantics to [`zrange` with
  # the `bylex` option](https://redis.io/docs/latest/commands/zrange/#lexicographical-ranges)
  # (`by: :lex` in this shard).
  def vrange(key : String, start : String, end stop : String, count : Int | String | Nil = nil)
    command = {"vrange", key, start, stop}
    command += {count.to_s} if count

    run command
  end

  # Return the elements of the vector set stored in `key` whose vectors are most
  # similar to that of `element`, which must also be stored in `key`. You can
  # limit the similarity comparison using `epsilon` as an upper limit of cosine
  # distance or by capping the number of results with `count` (defaults to 10).
  #
  # ```
  # results = redis.vsim("embeddings", text, epsilon: 0.2, count: 25)
  # ```
  def vsim(
    key : String,
    element : String,
    *,
    epsilon : Float | String | Nil = nil,
    withscores : Bool = false,
    count : Int | String | Nil = nil,
  )
    command = {"vsim", key, "ele", element}
    command += {"withscores"} if withscores
    command += {"epsilon", epsilon.to_s} if epsilon
    command += {"count", count.to_s} if count
    run command
  end

  # Return the elements of the vector set stored in `key` whose vectors are most
  # similar to `vector`, which does not need to be stored in `key`. You can
  # limit the similarity comparison using `epsilon` as an upper limit of cosine
  # distance or by capping the number of results with `count` (defaults to 10).
  #
  # ```
  # results = redis.vsim("embeddings", vectorize(text), epsilon: 0.2, count: 25)
  # ```
  def vsim(
    key : String,
    vector : Array(Float32),
    *,
    epsilon : Float | String | Nil = nil,
    withscores : Bool = false,
    count : Int | String | Nil = nil,
  )
    encoded = VectorEncoder.new.call(vector)
    command = {"vsim", key, "fp32", encoded}
    command += {"withscores"} if withscores
    command += {"epsilon", epsilon.to_s} if epsilon
    command += {"count", count.to_s} if count
    run command
  end

  private struct VectorEncoder
    def call(vector : Array(Float32)) : Bytes
      encoded = Bytes.new(vector.size * sizeof(Float32))
      vector.each_with_index do |f32, index|
        IO::ByteFormat::LittleEndian.encode f32, encoded + (index * sizeof(Float32))
      end
      encoded
    end
  end
end
