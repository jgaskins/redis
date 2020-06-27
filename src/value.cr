module Redis
  # Values consumed and emitted by Redis can be strings, 64-bit integers, `nil`,
  # or an array of any of these types.
  alias Value = String | Int64 | Nil | Array(Value)
end
