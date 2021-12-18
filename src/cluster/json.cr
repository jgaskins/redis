
require "../cluster"
require "../json"

Redis::Cluster.register_read_only_commands %w[
  json.get
  json.mget
  json.strlen
  json.arrindex
  json.arrlen
  json.objkeys
  json.objlen
  json.type
  json.resp
]
