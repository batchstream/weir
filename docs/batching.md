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
| `batching.max_wait` | 1 ms | Maximum write collection age; explicit `0s` disables collection |
| `batching.max_in_flight` | 16 | Simultaneous backend executions |
| `batching.min_in_flight` | 0 | Lowest [adaptive limit](#adaptive-concurrency) for batches that are not full; 0 lets it fall below one batch |
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

## Adaptive concurrency

Each Store limits how many record batches that are not full run at once, with
one limit for mutations and one for reads: a backend often queues writes behind
its journal or translog while it still serves reads from memory, and reads then
need not wait for writes. Each limit starts at `max_in_flight` and moves between
`min_in_flight` and `max_in_flight`, or down to a quarter of a batch when
`min_in_flight` is 0. Together they still run at most `max_in_flight` batches:

- Each execution is compared with the fastest recent execution of a batch of the
  same group, such as Lua programs on one collection, and the same or a larger
  size, counted in records or KiB. When the median execution of a window takes
  more than four times that baseline while the Store ran batches together or
  held one back, the backend is queueing and the limit halves. A window lasts
  at least 10 ms, one typical execution time and 8 executions, so a few slower
  records, such as reads of a large document, do not decide it. Executions
  whose callers all left, whose records all failed, or that waited for a Search
  refresh to make writes query-visible do not count.
- While executions stay under four times their baseline, the limit grows by
  half a batch per window, or by half its value below one.
- Below one, the Store runs one batch at a time and leaves the backend idle in
  between: at a half, as long as an execution typically takes, and at most a
  second. Records that would have formed a second batch join the next one, so
  this matters when one batch per replica is already more than a shared backend
  can take.

Records that are held back join larger batches, so a backend under pressure
receives fewer, larger requests. Without the limit, each replica sends a batch
whenever one of its slots is free: adding replicas, or a backend that slows down,
multiplies small concurrent requests and slows the backend further. Replicas
sharing a backend see the same queueing and back off together, so their combined
requests follow what the backend can take rather than how many replicas there
are. Scaling Weir out on CPU therefore stays safe when the database becomes the
limit. Full batches, of `max_operations` records or `max_bytes`, and a draining
Store may use every slot up to `max_in_flight`. The limit counts only batches
that are not full and do not wait for a Search refresh, so bulk ingest and
query-visible writes do not hold other work back.

The limit cannot tell queueing from a backend that slows down for another
reason, such as a stalled disk. While that lasts, records wait longer in Weir:
for a few more execution times, and at most about a second more, once the limit
falls below one. The limit grows back once executions return to their
baseline.

A replica that starts while its backend queues sees only slow executions. So
at the same wall-clock instants on every replica, a Store that kept record
batches running through the previous interval, or whose limit held one back in
it, pauses all its record batches, mutations and reads alike, for twice the
longer of their typical execution times, from 20 ms to 1 second. It then sends
one batch of each operation within one more such time, at most 250 ms, and
resumes. With every busy replica paused the backend empties, and those batches
renew the baselines. Records arriving meanwhile
wait for the pause to end. The interval is 10 seconds, doubled until the pause
takes at most 2.5% of it: a Store whose executions typically take 300 ms probes
every 40 seconds. Replicas that choose neighboring intervals still pause
together at the longer one's instants. A backend that does not empty within a
second still runs the probe faster than its queue, and the lower limit that
follows shortens the next pause's wait. Keep replica clocks synchronized with
NTP so their pauses overlap.
Baselines expire after one to two intervals, and at least 15 seconds, so a
backend that became slower for good regains its limit.

`weir_store_adaptive_executions_limit` and `weir_store_execution_latency_ratio`
show each limit and the last window's median ratio, with an `operation` label of
`mutate` or `read`. Set `min_in_flight` equal to `max_in_flight` to fix the
limits.

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
document sizes and the peak number of open streams. When live memory crosses the overload
watermark, new work receives `UNAVAILABLE` until it recovers; see
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
The short default wait adds at most about 1 ms to an isolated write. Under
sustained concurrency, compatible writes accumulate while all execution slots
are busy, so batch size follows backend execution time rather than the wait.
Increasing `max_operations` cannot create compatible arrivals. Longer collection
can reduce backend calls while increasing latency; a higher `max_in_flight` lets
full batches and an idle backend use more concurrent requests and retained memory,
while the adaptive limit keeps a queueing backend from receiving them.
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
