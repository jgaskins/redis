module Redis::Graph
  alias Value = String |
                Nil |
                Bool |
                Int64 |
                Float64 |
                Node |
                Relationship |
                Array(Value) |
                Hash(String, Value)
  alias List = Array(Value)
  alias Map = Hash(String, Value)
end