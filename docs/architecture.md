# Store execution architecture

Weir discovers logical Stores and executes database work in one process. Clients
initialize with ResolveStore through any node, then connect directly to the
returned business endpoints. Equal peers periodically synchronize an
in-memory directory. Business payloads never travel through another Weir node.
Lua evaluates inside the main process. Public and private peer schemas are
independent; see [protocols](protocols.md) and [discovery design](discovery-design.md).

The Node owns one map of local Stores and one ordered set of listener endpoints.
Each endpoint binds its listener, server and application/peer role. Startup,
shutdown and metrics use those same owned resources; directory Store names are
derived from the Store map.

## Public requests and completion

Execute is bidirectional. One finite stream selects a Store and one operation
kind. Each Read or Mutate request carries one canonical relative resource and a
consecutive one-based ordinal. The server validates and prepares each request,
admits it into the Store scheduler, and publishes its result in input order.
Receiving and publishing run concurrently through a queue of at most 32 tickets.
There is no complete-call request or result table on the server.

The SDK Read and Mutate conveniences preflight slice inputs without copying the
whole call into another protobuf batch. ReadStream and MutateStream offer an
incremental producer and consumer. Sending and receiving run concurrently with
at most 32 unconfirmed requests and 8 MiB of retained input; a larger legal
single request occupies the window alone. Each request is sent immediately,
without waiting for the producer to finish. Convenience methods accumulate output
on the caller.

Compatible queued records share database batches within and across streams.
Mutations to the same resource execute in stream input order, including after
item failures. Different streams retain backend concurrency semantics. Mutate is
not an atomic transaction; a later invalid request cannot undo earlier effects.
There is no durable request identity, deduplication or exactly-once guarantee.

Native sends one command with index 1 and half-closes input. It emits optional
opaque Document metadata, body chunks and NativeEnd. Adapters own the native
payload formats. Success requires the terminal event and final gRPC OK.

Individual business failures are typed results or terminal Events. RPC failures
are gRPC status. NOT_STARTED requires evidence of no backend attempt; NOT_APPLIED
requires definite rejection or evidence of no send; APPLIED requires a validated
acknowledgement; otherwise mutation outcome is UNKNOWN. Confirmed indexed results
survive a later transport failure, which still means the whole RPC failed. Neither
server nor SDK automatically replays possible writes.

## Native gRPC ingress

The server uses grpc.Server.Serve with connection lifecycle tracking. Application
listeners serve only the public StoreService; peer listeners serve only SyncDirectory.
InTapHandle checks ingress before DATA decoding without a concurrency gate.
Caller deadlines, cancellation and input-stall watchdogs apply to discovery and
streaming requests. RPC occupancy is measured through transport completion.

Deadlines and idle input cancel only their owning RPC, so other calls on the same
HTTP/2 connection continue. Native keepalive bounds a connection that cannot
advance its HTTP/2 framing. Output flow-control stalls and socket write stalls
still close the affected connection; confirmed mutation evidence remains valid.

The SDK reuses round-robin channels with native adaptive HTTP/2 flow control.
Directory and DNS refresh change selection for future RPCs without moving an
active call. Both listeners require deployment-isolated networking; the directory
contains only public ownership/address advertisements, never backend credentials.

## Memory and admission

| Capacity | Default or bound |
| --- | --- |
| Record document / ordinary Read source | Native backend boundary; no Weir size quota |
| Read/Mutate stream | No total record limit; configurable publication window (default 32 tickets) |
| Native input and output | Native backend boundary; no Weir size quota |
| Store waiting queue | 1024 operations / 32 MiB, configurable via queue |
| Physical batch input | 128 operations / 4 MiB, configurable |
| Active adapter invocations per Store | 16 by default; explicit batching.max_in_flight=0 is unlimited |
| Business connections and RPCs | No separate Weir concurrency limit |
| Process memory capacity | Automatically detected from host/process/container limits |

The waiting queue counts prepared input and scheduler metadata. Dispatch releases
its capacity immediately; active executions and completed results do not hold queue
credits. Full queues wait for capacity or caller cancellation. A stream retains a
finite publication window, so a stopped reader cannot accumulate unlimited results
inside that stream. Protocol frame boundaries are checked before protobuf construction.
The validator walks the transport's existing buffer fragments directly, including
nested message spans, without copying payloads or changing buffer references.
Queue and batch byte settings are aggregation windows. A larger individual
input or document proceeds alone; completed-result accounting grows to its actual size.

Process and container memory observations drive overload admission. Memory capacity
uses the smallest known physical host, finite process address-space, and visible
container limit. Container pressure is checked at every finite visible ancestor;
each current/limit pair keeps its own accounting. Unknown observations do not
manufacture overload. Container/OS limits remain the hard memory boundary.
The CLI explicitly owns a Go soft limit below the recovery watermark. At every
finite level it accounts for memory outside process RSS before choosing the
remaining process allowance, and follows trusted changes through this sampler.
Positive automatic targets have a 16 MiB floor, capped by the ordinary capacity
target, to bound futile GC under external pressure. Explicit runtime policy takes
precedence. Library assembly alone does not change process-wide GC policy.

Encoded output ownership is tracked until the transport frees the last buffer
reference, including after handler completion. There is no aggregate response
quota derived from a fixed RPC count. Cancellation releases application data;
independent encoded copies remain measured until transport ownership ends.

Lua runs in the main Weir process with caller context and optional VM execution
budgets. Source and value conversion have no Weir size/count quotas. There is no evaluation semaphore, fixed evaluation deadline, or hard VM
allocation sandbox. Programs must be trusted.
Allowed standard-library functions and constants are loaded once per process.
Every evaluation creates fresh mutable library tables and a string metatable,
with its own caller context and clock; no VM or caller state is reused.

## Scheduling and backend work

Each prepared record enters one Store scheduler directly. It forms separate read
and mutation batches, then combines compatible queued records by backend semantics,
actual input bytes, `batching.max_operations` and `batching.max_bytes`. Reads use
already queued reads; mutations retain the bounded write collection window. A
session/resource key serializes same-resource mutations within a stream; distinct
resources and streams can share a batch of the same kind.
Ordinary MongoDB mutations share a native bulk across qualified namespaces;
reads and Lua transactions remain scoped to one collection. Search bulk and
real-time multi-get requests can span concrete indexes. Duplicate Search reads
share an observation within a batch, while retaining individual caller results
and cancellation. Visibility modes remain separate.
Native uses the same scheduler as a singleton invocation with incremental output.
The scheduler reuses bounded candidate groups between dispatches; admissions,
queued cancellation and released session dependencies invalidate that selection
round. Queue removal preserves FIFO order without compacting the entire queue
for each dispatched batch. Adapters run their dispatched work to completion.

Every record keeps its owning RPC context, result reservation and ordinal.
Plan metadata is copied at admission, so reusable prepared plans cannot share
result-accounting changes or caller contexts. Command and backend payloads retain
their existing immutable-input boundary.
Backend results are assigned by dispatched plan identity, because different RPCs can use
the same ordinal. The shared backend context uses the latest caller deadline;
an unbounded caller keeps that context unbounded. Canceling one caller does not interrupt
its peers; when all callers stop waiting, the shared execution is canceled.
Queue capacity is released at dispatch; memory estimates remain observable until execution finishes. A confirmed
write retains its actual outcome after cancellation; a missing acknowledgement
for a dispatched write remains UNKNOWN. Different RPCs gain no additional
transaction or ordering guarantee.

MongoDB collections receive a structural metadata check on
first use, cached per Store according to `backend.mongodb.metadata_cache_entries` (default
64 targets). Zero disables completed caching; concurrent cold requests still share
one check. Failed checks are not cached. The oldest completed target is evicted
when full. Structure must remain stable while the Store is open; changes require
reopening the Store. Actual commands still enforce current database permissions.
Search qualifies each distinct target once per adapter invocation without retaining
completed metadata. It requests aliases, settings and the source/routing mapping
controls, omitting unused field mappings. A single-index alias binds that operation
to its physical index; later invocations observe alias changes. Multiple primary
shards are supported.

The executing portion of the batch set enforces `batching.max_in_flight` when it is positive.
The default is 16; an explicit zero means unlimited. Once the budget is full, compatible waiting
records stay in the bounded queue until an adapter invocation finishes. They form
batches naturally. `batching.max_wait` optionally collects compatible writes for
a bounded duration from the oldest admission; its default is `20ms`, and explicit
`0s` disables collection.
One Store timer wakes the scheduler for the earliest eligible collection deadline.
Reads use already queued compatible reads without waiting for future arrivals.
They neither join mutation batches nor flush their collection window, so a completed
read does not wait for unrelated writes in the same adapter invocation. Both kinds
still share the Store queue and execution budget. Full batches, Native, draining Stores and shorter
caller deadlines bypass collection. Ready groups can pass another group still collecting; once
both are ready, the oldest eligible group runs first. Credit returns at execution completion,
independently of result acknowledgement. Each Store owns its budget. No Store workspace ceiling or backend connection-pool
maximum is imposed. Transport failures do not replay mutations; confirmed conflicts
and aborts can retry within the
participating caller contexts. Waiting work is bounded by queue count and bytes.

The budget includes Search `refresh=wait_for`. It cannot distinguish waiting for
visibility from database application; a small budget can therefore reduce
visible-write throughput. Native publication can borrow a separately bounded
pause reservation, release execution credit while waiting on the consumer, and
fairly regain it through the same scheduler before continuing. Paused batches
remain in the lifecycle set for cancellation and shutdown. The pause count and
retained byte bounds separately reuse the Store queue configuration; charging
includes input, actual response working memory and the current event. A single
oversized stream may occupy an empty pause budget alone. When that budget is
full, output retains its execution slot and backpressure. Abandoning callers
cancels the invocation; retained credits return only when their owners stop.

Metrics observe execution rather than control scheduling.
Queue occupancy and capacity metrics describe waiting operations. Active execution
limits report zero for unlimited operation. Execution and per-record outcome counters
retain actual backend evidence, including unknown writes.

Native execution is a singleton with incremental streaming. Opaque commands and
publication inherit the caller context, without an added backend execution
deadline. Native bypasses typed target qualification and operation allowlists;
backend permissions, cursor/session lifetimes and command responses keep their
native meaning. Possible writes are never replayed automatically. Lua record transforms
participate in compatible request grouping. MongoDB uses a short snapshot
transaction for a bounded group: point read, individual Lua evaluation, bulk
write and commit. No metadata fields are inserted into business documents, and
external writers need no Weir-specific version convention. A database write
error can roll back the entire physical transaction. Only a confirmed abort
allows another read/evaluate/write attempt; an ambiguous commit can retry the
same commit but cannot replay the mutations. Lua keep, reject and evaluation
failures are resolved independently before the write phase.

Query visibility waits are explicit per mutation, including Lua and backend
expressions. Search writes use refresh=wait_for when requested; successful no-op
results use a confirmed refresh barrier. Application evidence survives a later
visibility failure, so APPLIED with Failure is not successful visible completion.

Search uses real-time multi-get and bulk writes with the observed sequence number
and primary term on each item. Confirmed version conflicts reread and reevaluate
only the conflicting items; ambiguous write replies are not replayed. Both
adapters split retained source and generated-write buffers into bounded groups.
Each caller retains its own cancellation, deadline and result ownership. A caller
canceled before its write is omitted; a sent write keeps its actual confirmed or
unknown outcome. Physical MongoDB transaction grouping is not an application
transaction contract and can vary with traffic, bytes and scheduler capacity.

Caller cancellation does not guarantee immediate release of remote transaction
locks. A read can reach MongoDB after `abortTransaction` returns
`NoSuchTransaction`; the bounded, best-effort `killSessions` cleanup is not a
permanent session tombstone. Rollback stays unconfirmed and cannot rebuild the
transaction. A late read may retain locks until MongoDB's transaction lifetime
limit and periodic cleanup apply. A lost commit acknowledgement remains UNKNOWN
and never replays the write.

Shutdown refuses new admission and drains admitted work within a deadline.
Cancellation stops owned work without claiming a write was not applied. Output
stalls may close the affected transport; business writes are never replayed.

## Validation

The versioned Go SDK exposes typed Read, Mutate, single-record helpers and
Native. It depends on weir-protocol, not the Weir server module. Default Go/Python
tests are offline safe. Real databases, process lifecycle, memory pressure and
Kubernetes tests require explicit opt-ins. Standalone weir-tests verifies public
behavior and measures matched direct/Weir workloads from immutable source and
version pins. Compilation, local database execution and Linux capacity evidence
are reported separately. Independent review is repeated after confirmed fixes.
