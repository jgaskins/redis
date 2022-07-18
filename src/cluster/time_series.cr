require "../cluster"
require "../time_series"

Redis::Cluster.register_read_only_commands %w[
  ts.get
  ts.info
  ts.mget
  ts.mrange
  ts.mrevrange
  ts.queryindex
  ts.range
  ts.revrange
]
