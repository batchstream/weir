# Store execution architecture

Weir discovers logical Stores and executes database work in one process. Clients
initialize with ResolveStore through any node, then connect directly to the
returned business endpoints. Equal peers periodically synchronize a bounded
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

Scan and Native each send one command with index 1 and half-close input. A scan
page emits business documents then ScanEnd; Native emits optional opaque Document
metadata, body chunks and NativeEnd. Adapters own the native payload formats.
Success requires the terminal event and final gRPC OK. A scan checkpoint additionally
requires the terminal document count to match complete delivery.

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
| Record document / ordinary Read source | Protocol document bound: 2 MiB |
| Read/Mutate stream | No total record limit; configurable publication window (default 32 tickets) |
| Native input | Protocol 8 MiB; backend encoding boundaries also apply |
| Store waiting queue | 1024 operations / 32 MiB, configurable via batching.queue |
| Physical batch input | 32 operations / 8 MiB, configurable |
| Business connections, RPCs and active executions | No Weir concurrency limit |
| Process memory capacity | Automatically detected from host/process/container limits |

The waiting queue counts prepared input and scheduler metadata. Dispatch releases
its capacity immediately; active executions and completed results do not hold queue
credits. Full queues wait for capacity or caller cancellation. A stream retains a
finite publication window, so a stopped reader cannot accumulate unlimited results
inside that stream. Protocol frame boundaries are checked before protobuf construction.

Process and container memory observations drive overload admission. Memory capacity
uses the smallest known physical host, finite process address-space, and visible
container limit. Container pressure is checked at every finite visible ancestor;
each current/limit pair keeps its own accounting. Unknown observations do not
manufacture overload. Container/OS limits remain the hard memory boundary.

Encoded output ownership is tracked until the transport frees the last buffer
reference, including after handler completion. There is no aggregate response
quota derived from a fixed RPC count. Cancellation releases application data;
independent encoded copies remain measured until transport ownership ends.

Lua runs in the main Weir process with caller context and source/value/stack/work
bounds. There is no evaluation semaphore, fixed evaluation deadline, or hard VM
allocation sandbox. Programs must be trusted.

## Scheduling and backend work

Each prepared record enters one Store scheduler directly; there is no timed
collection window or polling. The
scheduler combines compatible queued records by target, actual input bytes and
batching.max_operations and batching.max_bytes. A session/resource key serializes same-resource work
within a stream; distinct resources and streams can share a physical batch.
Scan and Native use the same scheduler with their required singleton lifecycle.
Execution mode and cleanup follow the command type. A Scan adapter returns whether
another bounded fetch is needed; the scheduler retains no duplicate mode flags or
mutable continuation flag on the plan.

Every record keeps its owning RPC context, result reservation and ordinal. Backend
results are assigned by dispatched plan identity, because different RPCs can use
the same ordinal. The shared backend context uses the latest caller deadline;
an unbounded caller keeps that context unbounded. Canceling one caller does not interrupt
its peers; when all callers stop waiting, the shared execution is canceled.
Queue capacity is released at dispatch; memory estimates remain observable until execution finishes. A confirmed
write retains its actual outcome after cancellation; a missing acknowledgement
for a dispatched write remains UNKNOWN. Different RPCs gain no additional
transaction or ordering guarantee.

MongoDB collections and Search indices receive a structural metadata check on
first use, cached per Store for up to 64 targets. Concurrent cold requests share
one check; failed checks are not cached. The oldest completed target is evicted
when full. Structure must remain stable while the Store is open; changes require
reopening the Store. Actual commands still enforce current database permissions.

The active batch set measures execution concurrency without capping it. There is
no Store workspace ceiling or backend connection-pool maximum. Transport failures
do not replay mutations; confirmed conflicts and aborts can retry within the
participating caller contexts. Waiting work is bounded by queue count and bytes.

Metrics observe execution rather than control scheduling. The former
`weir_store_feedback` last-result gauge is removed.
Queue occupancy and capacity metrics describe waiting operations. Active execution
and per-record outcome counters retain actual backend evidence, including unknown writes.

Scan fetches up to `streaming.scan.max_batch_documents` (default 128) per backend
call, bounded by the remaining logical page size. It validates the complete native response and retains an
ordered prefix bounded by `streaming.scan.max_batch_bytes` (default 4 MiB), then
finishes backend execution before publication.
The result remains owned through publication and request completion. A blocked
Scan reader does not stop independent work. MongoDB estimates 48 MiB and Search
64 MiB of working bytes for native buffers and validation; these metrics do not
limit execution or measure process RSS. Only a validated empty response establishes
exhaustion. An unretained tail is fetched again from the last accepted record.

Both backends learn a smaller fetch capacity from the retained prefix size.
They share the Scan event publication and count state; native fetching and
checkpoint cleanup remain backend responsibilities.
Their opaque continuations carry that capacity, separate from the final
request's remaining page size. Search also halves an excessive native response's
fetch size without advancing the checkpoint, using the same PIT and absolute
fetch deadline. Each new page is a new Execute RPC and can select another
instance. MongoDB uses ascending original BSON _id keysets without a retained
cursor or cross-page snapshot. Search carries the latest PIT and search_after
values; the backend snapshot has a 60-second keep-alive. Expiration fails
explicitly. A previously published PIT remains available after a later
failed/exhausted page until expiry.
Checkpoint tokens are bounded, opaque and tied to the target/filter/projection;
the checksum detects corruption and is not authentication.

Native execution is a singleton with bounded streaming. Qualification, native
commands and publication inherit the caller context, without an added backend
execution deadline. Lua record transforms
participate in compatible request grouping. MongoDB uses a short snapshot
transaction for a bounded group: point read, individual Lua evaluation, bulk
write and commit. No metadata fields are inserted into business documents, and
external writers need no Weir-specific version convention. A database write
error can roll back the entire physical transaction. Only a confirmed abort
allows another read/evaluate/write attempt; an ambiguous commit can retry the
same commit but cannot replay the mutations. Lua keep, reject and evaluation
failures are resolved independently before the write phase.

Search uses real-time multi-get and bulk writes with the observed sequence number
and primary term on each item. Confirmed version conflicts reread and reevaluate
only the conflicting items; ambiguous write replies are not replayed. Both
adapters split retained source and generated-write buffers into bounded groups.
Each caller retains its own cancellation, deadline and result ownership. A caller
canceled before its write is omitted; a sent write keeps its actual confirmed or
unknown outcome. Physical MongoDB transaction grouping is not an application
transaction contract and can vary with traffic, bytes and scheduler capacity.

Shutdown refuses new admission and drains admitted work within a deadline.
Cancellation stops owned work without claiming a write was not applied. Output
stalls may close the affected transport; business writes are never replayed.

## Validation

The versioned Go SDK exposes typed Read, Mutate, single-record helpers, Scan and
Native. It depends on weir-protocol, not the Weir server module. Default Go/Python
tests are offline safe. Real databases, process lifecycle, memory pressure and
Kubernetes tests require explicit opt-ins. Standalone weir-tests verifies public
behavior and measures matched direct/Weir workloads from immutable source and
version pins. Compilation, local database execution and Linux capacity evidence
are reported separately. Independent review is repeated after confirmed fixes.
