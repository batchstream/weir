# Lua ProgramTransform

Weir accepts `Transform.program` with runtime `lua.v1` on local MongoDB and
qualified Search Stores. Configure `lua_worker` with the executable path, or pass
`-lua-worker` when using local CLI flags. Product archives and the Weir container
ship `weir-lua-worker` beside `weir`. Without this configuration, local adapters
reject program transforms as unsupported; backend-expression transforms remain
independent of the Lua worker.

Each evaluation starts one worker process using a bounded JSON request/response
protocol. `current` is the existing document or typed `missing` when no record
exists; `input` is the optional input Document decoded as BSON for MongoDB or JSON
for Search. The source is UTF-8 Lua text, at most 16 KiB. Scripts return one of
`weir.keep()`, `weir.replace(object)`, `weir.delete()`, or `weir.reject(message)`.
Returning `nil` is equivalent to Keep.

```lua
local patch = weir.object(
  "count", weir.add(weir.get(current, "count"), weir.i32("1")),
  "profile", weir.object("active", weir.bool(true))
)
return weir.replace(weir.merge(current, patch))
```

`weir.merge(base, patch)` recursively merges objects, replaces arrays and scalar
values, treats a missing base record as an empty object, preserves base field
order, and appends new fields in patch order. A `missing` patch value removes that
object field; `null` remains an explicit null value. Missing array elements are
invalid rather than sparse slots.
Keep acknowledges without writing. Replace creates a missing record or replaces
the full document; MongoDB preserves the resource `_id`, and Search rejects
metadata fields in the source. Delete is a no-op when the record is absent.
Reject returns `PRECONDITION_FAILED` without writing.

The module also exposes typed constructors (`missing`, `null`, `bool`, `i32`,
`i64`, `f64`, `string`, `bytes`, `array`, and `object`), field access/update (`get`,
`set`, `kind`), checked integer arithmetic (`add`, `sub`, `mul`, `to32`, `to64`),
and `merge`.

## Bounds and safety

- Source: 16 KiB; request: 512 KiB; response: 384 KiB.
- Values: depth 32, 4,096 nodes, 256 KiB of text/byte payload.
- Each worker has a 500 ms default deadline (maximum 1 second); a backend transform
  has a 5 second overall retry budget. Caller cancellation kills and waits for the
  worker.
- At most four Lua workers run concurrently per configured Runner.
- Lua standard libraries, filesystem/network access, and implicit host bindings are
  unavailable.
- MongoDB re-reads and re-evaluates inside a majority-write transaction after
  transient conflicts; unresolved commit ambiguity is `UNKNOWN`. Search uses
  realtime reads and conditional writes with `_seq_no`/`_primary_term`, re-evaluating
  only after explicit version conflicts. Lost write acknowledgements are never
  replayed.
- Search fractional, exponent, and out-of-range integer JSON numbers remain opaque
  exact JSON-number values so unchanged values round-trip without float64 loss.

There is no hard per-process memory limit. A memory-exhausting script can still
pressure the host before its deadline, and OS-level isolation has not been qualified
across supported platforms. Enable only trusted programs; this is not a security
sandbox for untrusted scripts. Real MongoDB, Elasticsearch, and OpenSearch
fault/concurrency qualification is still required before calling the runtime
production-qualified.
