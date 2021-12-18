require "./spec_helper"
require "uuid"

require "../src/redis"
require "../src/search"

redis = Redis::Client.new

module Redis
  describe FullText do
    index = UUID.random.to_s
    prefix = UUID.random.to_s

    before_all do
      redis.ft.create <<-INDEX
        #{index} ON HASH
        PREFIX 1 #{prefix}:
        SCHEMA
          name TEXT NOSTEM SORTABLE
      INDEX
    end

    after_all do # you're my wonderwall
      redis.ft.dropindex index
    end

    it "does a thing" do
      redis.ft.info(index).should eq [
        "index_name", index,
        "index_options", [] of String,
        "index_definition", [
          "key_type", "HASH",
          "prefixes", ["#{prefix}:"],
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
        "total_inverted_index_blocks", "0",
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
    end
  end
end
