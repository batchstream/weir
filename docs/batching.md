# Batching

Weir aggregates compatible queued records at each Store. Batch size, collection
wait and execution concurrency govern different parts of this path:

```text
request -> bounded Store queue -> compatible batch -> backend -> ordered result
```

## Defaults and boundaries

| Limit | Default | Behavior |
| --- | --- | --- |
| Queue operations | 1024 (fixed) | Waiting prepared operations |
| Queue bytes | 32 MiB (fixed) | Waiting input and scheduler metadata |
| `batching.max_operations` | 128 | Selected records per batch |
| `batching.max_bytes` | 4 MiB | Input estimate and final Search NDJSON aggregation target |
| `batching.max_wait` | 20 ms | Maximum write collection age; explicit `0s` disables collection |
| `batching.max_in_flight` | 16 | Simultaneous backend executions |
| Exchange bytes | 32 MiB (fixed) | Adapter request/response grouping target |

A full queue waits for dispatch or caller cancellation. Dispatch returns queue
capacity immediately. One legal larger item can occupy an empty queue or batch
alone. Byte targets do not replace native backend document and command
boundaries.

Reads and mutations form separate batches. Reads use compatible reads already
queued without waiting for future arrivals; they do not flush write collection.
Write collection starts at the oldest eligible admission and overlaps waiting for
an execution slot. It does not restart when a slot becomes available. Full batches,
Native commands, draining Stores and shorter caller deadlines bypass collection.
Eligible groups can pass groups still collecting; ready groups retain oldest-first
selection.

An execution includes metadata checks, pre-reads, Lua, writes and requested query
visibility waits. A record batch returns its slot when the backend finishes,
independently of result consumption.

Native commands use neither the queue nor these slots. Each Store runs at most
64 Native commands at once; further ones wait for a free slot until their caller
gives up. Each one lasts until its caller has read the response or the output
stall timeout ends the RPC. Slow Native consumers can therefore delay other
Native commands, but not typed reads and mutations.

## Memory sizing

These limits bound memory. A rough upper bound for one Store is:

```text
32 MiB queue
  + max_in_flight × (max_bytes + max_operations × largest result)
  + 64 × largest Native reply
```

A MongoDB Native reply is held whole while it streams; Search Native bodies
stream in chunks. Every open stream adds the request it is receiving, and every
record stream adds up to 32 results awaiting delivery. A
stream ended by the output stall timeout keeps its last unsent message until the
client reads it or disconnects. Every message is at most 64 MiB. Real documents
are usually much smaller than these limits, so size the container from observed
document sizes and the peak number of open streams. When actual memory crosses the overload
watermark, new work receives `RESOURCE_EXHAUSTED` until it recovers; see
[operations](operations.md).

## Backend grouping

MongoDB ordinary mutations can share native `bulkWrite` across namespaces.
Reads and Lua transactions keep their collection boundary. Commands also respect
MongoDB's advertised BSON/message limits.

Search bulk writes and real-time multi-gets can span concrete indexes, with
visibility modes kept separate. Duplicate pure reads share one observation while
retaining individual results and cancellation. Oversized multi-record read
responses can safely split into smaller reads; mutation pre-reads do not use
that retry path.

Search measures final NDJSON, including metadata, newlines and generated Lua
output. It flushes at the smaller of `batching.max_bytes` and the 32 MiB exchange
target. One larger legal item proceeds alone. Full-source
Lua replacements can generate much more traffic than small incoming patches.

Same-resource mutations within a stream preserve input order. Different streams
retain backend concurrency semantics. Grouping is not an application transaction:
a MongoDB Lua transaction may include records from several RPCs, and its physical
membership varies with arrivals and available capacity.

## Workload calibration

Start with defaults and vary count, bytes, wait and concurrency independently.
Increasing `max_operations` cannot create compatible arrivals. Longer collection
can reduce backend calls while increasing latency; more execution slots can keep
the backend busy while increasing small concurrent requests and retained memory.
Query-visible Search writes include refresh waits in their execution slots.

Compare equivalent native and Weir traffic with matching durability, visibility,
retry and deadline requirements. Measure completed throughput, tail latency,
rejection, timeouts, drain time, actual backend CPU and bytes per completed unit.
Verify persisted business fields and account for unknown writes. Use the real
submission pattern, document-size distribution and key/index skew.

Useful cases include pure writes, reads, partial updates to large documents,
dependent Lua phases, mixed-size generated documents, hot keys, many cold targets,
sparse arrivals, bursts and slow consumers. Correctness and scheduling tests do
not establish throughput gains. Defaults are starting values; sustainable capacity
must be measured for the deployment and workload. Reproducible workload tooling
lives in [weir-tests](https://github.com/batchstream/weir-tests).
