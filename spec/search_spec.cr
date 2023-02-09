require "./spec_helper"
require "uuid"

require "../src/redis"
require "../src/search"
require "../src/json"

private macro get_info(index)
  redis.ft.info({{index}}).as(Array)
end

private macro wait_for_indexing_complete(index)
  %key_index = 40
  %failures_index = 38

  while (%info = get_info({{index}})) && %info[%key_index + 1] != "0"
    unless %info[%key_index] == "indexing"
      raise "wrong array index: #{%info[%key_index].inspect}"
    end
    unless %info[%failures_index] == "hash_indexing_failures"
      raise %{#{%info[%failures_index]} != "hash_indexing_failures"}
    end

    unless %info[%failures_index + 1] == "0"
      raise "Hash indexing failures: #{%info[%failures_index]}"
    end
  end

  %info
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
      INDEX

      wait_for_indexing_complete hash_index
      wait_for_indexing_complete json_index
    end

    after_all do # you're my wonderwall
      redis.ft.dropindex hash_index
      redis.ft.dropindex json_index
      keys = redis.keys("#{hash_prefix}:*") + redis.keys("#{json_prefix}:*")
      redis.unlink keys.map(&.as(String))
    end

    pending "gets index metadata" do
      expected = [
        "index_name", hash_index,
        "index_options", [] of String,
        "index_definition", [
          "key_type", "HASH",
          "prefixes", ["#{hash_prefix}:"],
          "default_score", "1",
        ],
        "attributes", [
          [
            "identifier", "name",
            "attribute", "name",
            "type", "TEXT",
            "WEIGHT", "1",
            "SORTABLE", "NOSTEM",
          ],
        ],
        "num_docs", "0",
        "max_doc_id", "0",
        "num_terms", "0",
        "num_records", "0",
        "inverted_sz_mb", "0",
        "vector_index_sz_mb", "0",
        "total_inverted_index_blocks", "4",
        "offset_vectors_sz_mb", "0",
        "doc_table_size_mb", "0",
        "sortable_values_size_mb", "0",
        "key_table_size_mb", "0",
        "records_per_doc_avg", "nan",
        "bytes_per_record_avg", "nan",
        "offsets_per_term_avg", "nan",
        "offset_bits_per_record_avg", "nan",
        "hash_indexing_failures", "0",
        "indexing", "0",
        "percent_indexed", "1",
        "gc_stats", [
          "bytes_collected", "0",
          "total_ms_run", "0",
          "total_cycles", "0",
          "average_cycle_time_ms", "nan",
          "last_run_time_ms", "0",
          "gc_numeric_trees_missed", "0",
          "gc_blocks_denied", "0",
        ],
        "cursor_stats", [
          "global_idle", 0,
          "global_total", 0,
          "index_capacity", 128,
          "index_total", 0,
        ],
      ]

      redis.ft.info(hash_index).should eq expected
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
          "name", "match",
          "who", "cares"

        results = redis.ft.search(hash_index, "match", return: %w[name])
        count, key, result = results

        result.should eq %w[name match]
      end

      it "can filter results based on numeric ranges" do
        redis.hset "#{hash_prefix}:filter:match:too-high  ", name: "match", post_count: "51"
        redis.hset "#{hash_prefix}:filter:match:goldilocks", name: "match", post_count: "50"
        redis.hset "#{hash_prefix}:filter:match:too-low   ", name: "match", post_count: "49"
        redis.hset "#{hash_prefix}:filter:no-match        ", name: "nope!", post_count: "50"

        results = redis.ft.search(hash_index, "match", filter: [
          Redis::FullText::Filter.new("post_count", 0..50),
          Redis::FullText::Filter.new("post_count", 50..100),
        ]).as(Array)

        results[0].should eq 1
        results[1].should eq "#{hash_prefix}:filter:match:goldilocks"
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
        redis.hset key1, post_count: "12"
        redis.hset key2, post_count: "1024"

        result = redis.ft.search hash_index, "@post_count:[$min_posts +inf]",
          params: {min_posts: "100"}

        result.size.should eq 3
        count, matched_key, matched_hash = result
        count.should eq 1
        matched_key.should eq key2
        matched_hash.should eq %w[post_count 1024]
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
