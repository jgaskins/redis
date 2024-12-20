require "./spec_helper"
require "uuid"

require "../src/redis"
require "../src/search"
require "../src/json"

private macro get_info(index)
end

private def wait_for_indexing_complete(redis, index)
  while info = redis.ft.info(index).as(Array).in_slices_of(2).to_h
    break if info["indexing"] != "0"

    unless info["hash_indexing_failures"] == "0"
      raise "Hash indexing failures: #{info["hash_indexing_failures"]}"
    end
  end

  info
end

redis = Redis::Client.new

module Redis
  describe FullText do
    hash_index = UUID.random.to_s
    hash_prefix = UUID.random.to_s
    json_index = UUID.random.to_s
    json_prefix = UUID.random.to_s

    before_all do
      redis.ft.create <<-INDEX
        #{hash_index} ON HASH
        PREFIX 1 #{hash_prefix}:
        SCHEMA
          name TEXT NOSTEM SORTABLE
          body TEXT
          location GEO
          post_count NUMERIC SORTABLE
          section TEXT NOSTEM
      INDEX

      # Please leave the FILTER clause in there, it ensures we send the quoted
      # filter expression as a single token.
      redis.ft.create <<-INDEX
        #{json_index} ON JSON
        PREFIX 1 #{json_prefix}:
        FILTER "@post_count >= 0"
        SCHEMA
          $.name AS name TEXT NOSTEM SORTABLE
          $.body AS body TEXT
          $.location AS location GEO
          $.post_count AS post_count NUMERIC SORTABLE
          $.section AS section TEXT NOSTEM
      INDEX

      wait_for_indexing_complete redis, hash_index
      wait_for_indexing_complete redis, json_index
    end

    after_all do # you're my wonderwall
      redis.ft.dropindex hash_index
      redis.ft.dropindex json_index
      keys = redis.keys("#{hash_prefix}:*") + redis.keys("#{json_prefix}:*")
      redis.unlink keys.map(&.as(String))
    end

    describe "hashes" do
      it "does a simple search" do
        redis.hset "#{hash_prefix}:simple:match", "name", "included"
        redis.hset "#{hash_prefix}:simple:no-match", "name", "excluded"

        result = redis.ft.search(hash_index, "included")

        result[0].should eq 1
        result[2].should eq %w[name included]
      end

      it "can do geofiltering on a search" do
        redis.hset "#{hash_prefix}:geo:match:near      ", "name", "Market Place, Baltimore    ", "location", "76.60692,39.28857"
        redis.hset "#{hash_prefix}:geo:match:too-far   ", "name", "Lexington Market, Baltimore", "location", "76.62120,39.29186"
        redis.hset "#{hash_prefix}:geo:no-match:near   ", "name", "Shake Shack, Baltimore     ", "location", "76.60980,39.28692"
        redis.hset "#{hash_prefix}:geo:no-match:too-far", "name", "Brewer's Art, Baltimore    ", "location", "76.61637,39.30274"

        results = redis.ft.search hash_index, "market",
          geofilter: Redis::FullText::GeoFilter.new(
            field: "location",
            longitude: 76.60714,
            latitude: 39.28925,
            radius: 0.5,
            unit: :mi,
          )

        result_count, key, result = results

        result_count.should eq 1
        key.as(String).strip.should eq "#{hash_prefix}:geo:match:near"
        Redis.to_hash(result.as(Array))["name"].as(String).strip.should eq "Market Place, Baltimore"
      end

      it "can return a limited subset of keys" do
        redis.hset "#{hash_prefix}:return:hello",
          name: "match",
          who: "cares",
          section: "keysubset"

        results = redis.ft.search hash_index,
          %{match @section:(keysubset)},
          return: %w[name]
        count, key, result = results

        key.should eq "#{hash_prefix}:return:hello"
        result.should eq %w[name match]
      end

      describe "FILTER" do
        it "can filter results based on numeric ranges" do
          redis.hset "#{hash_prefix}:filter:match:too-high  ", name: "match", post_count: "51", section: "numeric_range"
          redis.hset "#{hash_prefix}:filter:match:goldilocks", name: "match", post_count: "50", section: "numeric_range"
          redis.hset "#{hash_prefix}:filter:match:too-low   ", name: "match", post_count: "49", section: "numeric_range"
          redis.hset "#{hash_prefix}:filter:no-match        ", name: "nope!", post_count: "50", section: "numeric_range"

          # Testing multiple FILTER clauses in a single query
          results = redis.ft.search(hash_index, "match @section:(numeric_range)", filter: [
            Redis::FullText::Filter.new("post_count", 0..50),
            Redis::FullText::Filter.new("post_count", 50..100),
          ])

          results[0].should eq 1
          results[1].should eq "#{hash_prefix}:filter:match:goldilocks"
        end

        it "can filter results based on open-ended numeric ranges" do
          prefix = "#{hash_prefix}:filter-open"
          redis.hset "#{prefix}:match:higher", name: "filter-open-match", post_count: "51"
          redis.hset "#{prefix}:match:middle", name: "filter-open-match", post_count: "50"
          redis.hset "#{prefix}:match:2-low ", name: "filter-open-match", post_count: "49"
          redis.hset "#{prefix}:no-match    ", name: "nope!", post_count: "50"

          results = redis.ft.search(hash_index, "filter open match", filter: [
            Redis::FullText::Filter.new("post_count", 50..),
          ])

          results.size.should eq 5
          count, key1, result1, key2, result2 = results
          # The results aren't ordered, so we can get them back in any order
          keys = [key1, key2]
          hashes = [result1, result2]
          count.should eq 2
          keys.should contain "#{prefix}:match:middle"
          keys.should contain "#{prefix}:match:higher"
          hashes.should contain %w[name filter-open-match post_count 51]
          hashes.should contain %w[name filter-open-match post_count 50]
        end
      end

      it "can search without content" do
        redis.hset "#{hash_prefix}:nocontent:match:1", name: "nocontent match"
        redis.hset "#{hash_prefix}:nocontent:match:2", name: "nocontent match"
        redis.hset "#{hash_prefix}:nocontent:no-match:3", name: "nope"

        results = redis.ft.search(hash_index, "nocontent match", nocontent: true)

        results.size.should eq 3
        count, match1, match2 = results
        count.should eq 2
        # Keys only, no matched content
        match1.should eq "#{hash_prefix}:nocontent:match:1"
        match2.should eq "#{hash_prefix}:nocontent:match:2"
      end

      it "can search for a verbatim string" do
        redis.hset "#{hash_prefix}:verbatim:match", name: "match my text verbatim"
        redis.hset "#{hash_prefix}:verbatim:no-match:1", name: "matching my text verbatim"
        redis.hset "#{hash_prefix}:verbatim:no-match:2", name: "texting my match verbatim"

        results = redis.ft.search(hash_index, %{match my text verbatim}, verbatim: true)

        count, key = results
        count.should eq 1
        key.should eq "#{hash_prefix}:verbatim:match"
      end

      it "can search for a inorder string" do
        redis.hset "#{hash_prefix}:inorder:match", body: "match my text inorder"
        redis.hset "#{hash_prefix}:inorder:no-match:1", body: "my inorder match text"
        redis.hset "#{hash_prefix}:inorder:no-match:2", body: "text my inorder match"

        results = redis.ft.search(hash_index, "match my text inorder", inorder: true)

        count, key = results
        count.should eq 1
        key.should eq "#{hash_prefix}:inorder:match"
      end

      it "can set highlighting and summarization for text" do
        redis.hset "#{hash_prefix}:highlight:match", name: "yeah", body: <<-TEXT
          Hello i am writing tests for the RediSearch module. The purpose is to
          ensure that highlighting works.

          Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod
          tempor incididunt ut labore et dolore magna aliqua. Commodo quis
          imperdiet massa tincidunt nunc pulvinar. Tellus in hac habitasse platea
          dictumst vestibulum rhoncus est. Arcu dui vivamus arcu felis bibendum.
          Commodo ullamcorper a lacus vestibulum sed arcu non. Senectus et netus
          et malesuada. In est ante in nibh mauris cursus mattis molestie. Et
          malesuada fames ac turpis egestas maecenas pharetra convallis. Felis
          bibendum ut tristique et egestas. Sit amet mattis vulputate enim nulla.
          Tempus urna et pharetra pharetra massa massa. Et molestie ac feugiat sed
          lectus vestibulum. Quam viverra orci sagittis eu volutpat odio facilisis.
          Here I am testing more highlighting.

          Habitant morbi tristique senectus et netus et malesuada fames ac.
          Vestibulum mattis ullamcorper velit sed ullamcorper morbi tincidunt.
          Sagittis nisl rhoncus mattis rhoncus urna neque viverra justo nec.
          Praesent semper feugiat nibh sed pulvinar. Sit amet risus nullam eget
          felis eget nunc lobortis. In nulla posuere sollicitudin aliquam ultrices
          sagittis. Aliquet bibendum enim facilisis gravida neque convallis. Lacus
          vel facilisis volutpat est. Vitae justo eget magna fermentum iaculis eu
          non diam. Volutpat maecenas volutpat blandit aliquam etiam erat velit
          scelerisque. Eros donec ac odio tempor orci dapibus ultrices. Tellus
          molestie nunc non blandit massa. Vitae congue eu consequat ac felis.
          Urna neque viverra justo nec ultrices dui sapien. More highlighting.
          Mattis nunc sed blandit libero volutpat sed cras ornare. Pellentesque
          elit eget gravida cum sociis. Arcu dui vivamus arcu felis bibendum ut.
          Facilisis leo vel fringilla est ullamcorper.
          TEXT

        fields = %w[body]
        results = redis.ft.search hash_index, "test highlight",
          return: fields,
          highlight: Redis::FullText::Highlight.new(
            fields: fields,
            tags: {"<strong>", "</strong>"},
          ),
          summarize: Redis::FullText::Summarize.new(
            fields: fields,
            frags: 3,
            len: 2,
            separator: " … ",
          )

        count, key, result = results
        count.should eq 1
        key.should eq "#{hash_prefix}:highlight:match"
        result.should eq ["body", "<strong>tests</strong> for the RediSearch module. The purpose is to ensure that <strong>highlighting</strong> … <strong>testing</strong> more <strong>highlighting</strong>. … More <strong>highlighting</strong>. … "]
      end

      it "can choose a dialect to search with" do
        prefix = "#{hash_prefix}:search-dialect"
        d1match = "#{prefix}:dialect-1-match"
        d2match = "#{prefix}:dialect-2-match"
        redis.hset d1match, name: "matches dialect 1"
        redis.hset d2match, name: "world"

        d1result = redis.ft.search(hash_index, "-hello world", dialect: 1)
        d2result = redis.ft.search(hash_index, "-hello world", dialect: 2)

        d1result.should_not eq d2result
      end

      it "can search with params" do
        prefix = "#{hash_prefix}:params-search"
        key1 = "#{prefix}:1"
        key2 = "#{prefix}:2"
        redis.hset key1, name: "hash-params", post_count: "12"
        redis.hset key2, name: "hash-params", post_count: "1024"

        result = redis.ft.search hash_index, "@name:($name) @post_count:[$min_posts +inf]",
          params: {name: "params", min_posts: 100}

        result.size.should eq 3
        count, matched_key, matched_hash = result
        count.should eq 1
        matched_key.should eq key2
        matched_hash.should eq %w[name hash-params post_count 1024]
      end
    end

    describe "JSON" do
      it "can search JSON" do
        redis.pipeline do |pipe|
          pipe.json.set "#{json_prefix}:json:match:1", ".", {body: "json match"}
          pipe.json.set "#{json_prefix}:json:no-match:1", ".", {body: "nothin to see here"}
        end

        results = redis.ft.search(json_index, "match json")

        count, key, result = results
        count.should eq 1
        key.should eq "#{json_prefix}:json:match:1"
        result.should eq ["$", {body: "json match"}.to_json]
      end
    end
  end
end
