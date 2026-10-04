# Store execution architecture

Weir discovers logical Stores and executes database work in one process. Clients
initialize with ResolveStore through any node, then connect directly to the
returned business endpoints. Equal peers periodically synchronize a bounded
in-memory directory. Business payloads never travel through another Weir node.
Lua evaluates inside the main process. Public and private peer schemas are
independent; see [protocols](protocols.md) and [discovery design](discovery-design.md).

## Public requests and completion

Execute is bidirectional. One finite stream selects a Store and one operation
kind. ReadBatch and MutationBatch frames carry bounded lists of canonical relative
resources and a consecutive first-item ordinal. The server validates a frame,
prepares one finite window, executes and publishes its indexed results, then
continues receiving. A logical call can contain arbitrarily many frames; it never
owns a complete-call result table on the server.

The SDK Read and Mutate conveniences preflight slice inputs without copying the
whole call into another protobuf batch. ReadStream and MutateStream offer an
incremental producer and consumer. Sending and receiving run concurrently with
bounded frame credits. Convenience methods accumulate output on the caller.

Compatible windows from independent streams share database batches. Mutations to
the same resource execute in stream input order, including after item failures.
Different streams retain backend concurrency semantics. Mutate is not an atomic
transaction; a later invalid frame cannot undo earlier effects. There is no durable
request identity, deduplication or exactly-once guarantee.

Scan and Native each send one command with index 1 and half-close input. A scan
page emits documents then ScanEnd; Native emits metadata, chunks and NativeEnd.
Success requires the terminal event and final gRPC OK. A scan checkpoint additionally
requires the terminal document count to match complete delivery.

Individual business failures are typed results or terminal Events. RPC failures
are gRPC status. NOT_STARTED requires evidence of no backend attempt; NOT_APPLIED
requires definite rejection or evidence of no send; APPLIED requires a validated
acknowledgement; otherwise mutation outcome is UNKNOWN. Confirmed indexed results
survive a later transport failure, which still means the whole RPC failed. Neither
server nor SDK automatically replays possible writes.

## Native gRPC ingress

The server uses grpc.Server.Serve on a bounded listener. Application listeners
serve only the public StoreService; peer listeners serve only SyncDirectory.
Control calls have separate finite admission and short deadlines. InTapHandle
reserves an RPC slot before DATA decoding. Request deadlines, cancellation and
input-stall watchdogs apply to discovery and streaming requests. An expired request
that never reaches a handler still releases its slot.

The SDK reuses round-robin channels with native adaptive HTTP/2 flow control.
Directory and DNS refresh change selection for future RPCs without moving an
active call. Both listeners require deployment-isolated networking; the directory
contains only public ownership/address advertisements, never backend credentials.

## Memory and admission

| Budget | Default or bound |
| --- | --- |
| Read/Mutate request frame | 5 MiB and 1024 items; no whole-call size limit |
| Record document | 2 MiB; ordinary Read additionally obeys max_read_size |
| Ordinary Read source | Default 16 KiB; configurable 1 KiB–2 MiB |
| Native body | MongoDB 4 MiB; Search 8 MiB |
| Execute Event | 2 MiB plus bounded framing/metadata allowance |
| Store pending requests | 32 MiB of prepared input and request metadata |
| Store retained results | 32 MiB of terminal metadata and actual read data |
| Store backend workspace | 384 MiB; configurable with working_memory |
| Physical batch input | 8 MiB / 32 operations, configurable |
| Application admission | 4 business RPCs / 16 accepted connections |
| Encoded native output queue | bounded independently of total stream length |
| Directory controls | 2 concurrent controls per listener; bounded exchanges |
| Process admission threshold | 2 GiB |

There is no whole-call item-count limit. Frame bytes, frame metadata, Store budgets
and physical grouping bounds determine concurrent capacity. A wire pre-scan rejects
frame expansion beyond the metadata budget before protobuf construction.

Each stream retains one bounded input frame and one prepared execution window.
Read windows reserve output credits from their configured maximum read sizes before
execution, so an oversized logical call does not consume an entire Store result
budget. Shared capacity exhaustion waits for admission or cancellation. The server
publishes and releases a window before advancing to the next window or frame.
Window result tables and database workspace are independent of total call length.
Same-resource ordering is maintained across windows. Slow response consumption
stalls publication and stops further input; queues cannot grow without a bound.

Backend working charges cover bounded native replies and decoding scratch.
MongoDB uses its native bounded cursor reply; Search caps multi-get response bytes.
Physical grouping does not multiply the maximum document size by the item count.
`working_memory` is a byte budget, and `max_concurrency` is an independent upper
bound. The smaller capacity applies. At `max_read_size: 2MiB`, a MongoDB read batch
reserves 40.125MiB and a Search batch reserves 96MiB; the default workspace permits
9 and 4 such batches respectively. Size the workspace for the required concurrency
and increase process `memory` to cover it. Configuration rejects an insufficient
process envelope before opening backends.
Encoded response bytes remain charged until the native transport frees its final
buffer reference, including after handler completion. Exhausted output capacity
fails boundedly. A record window's application result charges remain held until
every response's encoded buffer ownership ends, then the window is acknowledged.
Cancellation releases application data; any independent encoded copy remains
charged until the transport drops its final reference.

Configuration validates a conservative sum of connection, RPC, Store and control
envelopes. This is admission accounting, not an allocator or RSS limit: GC slack,
runtime stacks and native allocations remain observable separately. Deployments
must set their OS/container limit and observe overload shedding. Lua is a trusted
program facility with source/value/stack/concurrency/deadline limits, without a
hard VM allocation sandbox.

## Scheduling and backend work

The current prepared window enters the Store scheduler directly; there is no
timed collection window or polling. When an execution permit becomes available,
the scheduler combines compatible queued windows from different streams by
target namespace, actual input bytes and max_batch_operations. One selected
execution holds one working envelope sized for its largest operation. Each
stream advances through sequential bounded windows; multi-namespace work splits
into compatible backend groups. Repeated mutation keys start a new sequential wave.
One adapter implementation handles both record groups and Scan/Native plans.

Every record keeps its owning RPC context, result budget and ordinal. Backend
results are assigned by dispatched plan identity, because different RPCs can use
the same ordinal. The shared backend context uses the latest caller deadline,
capped by the configured backend timeout. Canceling one caller does not interrupt
its peers; when all callers stop waiting, the shared execution is canceled.
Admission and working charges remain held until execution finishes. A confirmed
write retains its actual outcome after cancellation; a missing acknowledgement
for a dispatched write remains UNKNOWN. Different RPCs gain no additional
transaction or ordering guarantee.

MongoDB collections and Search indices receive a structural metadata check on
first use, cached per Store for up to 64 targets. Concurrent cold requests share
one check; failed checks are not cached. The oldest completed target is evicted
when full. Structure must remain stable while the Store is open; changes require
reopening the Store. Actual commands still enforce current database permissions.

Each Store dispatches while configured max_concurrency and backend working bytes
permit. Transport failures do not replay mutations; the bounded Lua conflict and confirmed-abort
retries described below retain the original execution deadline. Pending work
remains bounded by queue bytes and deadlines.

Scan fetches up to 128 documents per backend call, bounded by the remaining
logical page size. It validates the complete native response and retains an
ordered prefix of at most 4 MiB, then releases the working reservation and
execution permit before publication. The result reservation remains held through
publication and request completion. A blocked scan at concurrency one allows
independent record work. MongoDB reserves 48 MiB and Search 64 MiB of working
bytes for their native buffers and validation; this bounds admitted work and is
not a measurement of process RSS. Only a validated empty response establishes
exhaustion. An unretained tail is fetched again from the last accepted record.

Both backends learn a smaller fetch capacity from the retained prefix size.
Their opaque continuations carry that capacity, separate from the final
request's remaining page size. Search also halves an excessive native response's
fetch size without advancing the checkpoint, using the same PIT and absolute
fetch deadline. Each new page is a new Execute RPC and can select another
instance. MongoDB uses ascending original BSON _id keysets without a retained
cursor or cross-page snapshot. Search carries the latest PIT and search_after
values; the backend snapshot has a 60-second keep-alive. Expiration fails
explicitly. A previously published PIT remains available after a later
failed/exhausted page until expiry.
Checkpoint tokens are bounded, opaque and tied to the target/selector/profile;
the checksum detects corruption and is not authentication.

Native execution is a singleton with bounded streaming and backend time. MongoDB
qualification and native command share a deadline; Search counts actual backend
I/O time and pauses that allowance during publication. Lua record transforms
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
