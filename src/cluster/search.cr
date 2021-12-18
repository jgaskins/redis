require "../cluster"
require "../search"

Redis::Cluster.register_read_only_commands %w[
  ft.search
  ft.aggregate
  ft.explain
  ft.profile
  ft.info
  ft._list
  ft.tagvals
  ft.sugget
  ft.suglen
  ft.syndump
  ft.spellcheck
  ft.dictdump
]
