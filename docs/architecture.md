# Store execution architecture

Weir discovers logical Stores and executes database work in one process. Clients
initialize with ResolveStore through any node, then connect directly to the
returned business endpoints. Equal peers periodically synchronize a bounded
in-memory directory. Business payloads never travel through another Weir node.
Lua evaluates inside the main process. Public and private peer schemas are
independent; see [protocols](protocols.md) and [discovery design](discovery-design.md).

## Public requests and completion

Read and Mutate are unary batch RPCs. Each request contains one Store name and an
ordered list of canonical relative resources. Preparation validates every item
before backend work. Results have the same length and position as the inputs.
One batch can select several collections or indices within the same Store.

A shared execution constructor validates the complete public batch once and
borrows its immutable request fields. It decodes relative path segments into
internal records before backend preparation. Adapters consume those prepared
records and validate database-specific targets and documents; they do not copy
request DTOs, build full Store URIs or repeat public protocol validation.

Mutate is not a transaction. Same-resource mutations execute in input order,
including after a failed item; different keys can share a physical batch. Separate
RPCs retain normal backend concurrency semantics. There is no durable request
ID, deduplication or exactly-once guarantee.

Execute accepts exactly one Scan or Native Command and streams typed Events. A scan page emits documents then ScanEnd;
a native operation emits metadata, ordered chunks and NativeEnd. Streaming
success requires a validated terminal and final gRPC OK. A scan checkpoint also
requires its terminal document count to match complete delivery.

Individual business failures are typed results or terminal Events. RPC failures
are gRPC status. NOT_STARTED requires evidence of no backend attempt; NOT_APPLIED
requires definite rejection or evidence of no send; APPLIED requires a validated
acknowledgement; otherwise mutation outcome is UNKNOWN. APPLIED can include a
failure of a later acknowledgement step without losing application evidence.
A failed unary RPC or invalid batch response confirms none of its submitted
mutations. Neither server nor SDK automatically replays possible writes.
NativeEnd evidence can survive a later transport error, but that RPC still failed.

## Native gRPC ingress

The server uses grpc.Server.Serve on a bounded listener. Application listeners
serve only the public StoreService; peer listeners serve only SyncDirectory.
Control calls have separate finite admission and short deadlines. InTapHandle
reserves an RPC slot before DATA decoding. Request deadlines, cancellation and
input-stall watchdogs apply to unary and streaming requests. An expired request
that never reaches a handler still releases its slot.

The SDK reuses round-robin channels with native adaptive HTTP/2 flow control.
Directory and DNS refresh change selection for future RPCs without moving an
active call. Both listeners require deployment-isolated networking; the directory
contains only public ownership/address advertisements, never backend credentials.

## Memory and admission

| Budget | Default or bound |
| --- | --- |
| Complete Read/Mutate protobuf request or response | 32 MiB, including repeated-item envelope bytes |
| Record document | 2 MiB; ordinary Read additionally obeys max_read_size |
| Ordinary Read source | Default 16 KiB; configurable 1 KiB–2 MiB |
| Native body | MongoDB 4 MiB; Search 8 MiB |
| Execute Event | 2 MiB plus bounded framing/metadata allowance |
| Store pending requests | 32 MiB of prepared input and request metadata |
| Store retained results | 32 MiB of terminal metadata and actual read data |
| Store backend workspace | 384 MiB; configurable with working_memory |
| Physical batch input | 8 MiB / 32 operations, configurable |
| Application admission | 4 business RPCs / 16 accepted connections |
| Encoded native output queue | max_sessions × 32 MiB |
| Directory controls | 2 concurrent controls per listener; bounded exchanges |
| Process admission threshold | 2 GiB |

There is no public item-count limit. Complete encoded bytes, real backend limits
and declared memory budgets determine capacity. Runtime counters, session counts
and configured execution concurrency must fit the process memory envelope.

Before protobuf object construction, a wire pre-scan checks repeated-item
metadata against the same 32-MiB terminal-envelope budget. This prevents tiny
encoded items from amplifying into unbounded decoded objects. A failure at this
stage follows the native gRPC decoder status, with an explicit budget diagnostic.
It allocates no item objects and performs no backend work.

A complete client batch owns one prepared request, one admission ticket, one
cancellation watcher and one result table. Record terminal envelopes reserve
small fixed charges. Read data reserves actual copied bytes before retention;
max_read_size is an acceptance limit rather than a per-record reservation.
Insufficient result credit produces an individual ResourceExhausted result instead
of waiting while holding a partially filled response. Mutation acknowledgement
and failure envelopes retain reserved space. Several RPCs can share a backend
execution without sharing admission or response budgets. Duplicate reads may
share immutable data, but every response owner retains its own byte charge until
that RPC releases its results.
There is no separate Store request-count cap: these byte budgets and the configured
ingress RPC limit bound concurrent requests. Every record or command charges its
retained metadata, including a nonzero ticket and terminal envelope.

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
fails boundedly. Application result charges end after native serialization/enqueue.

Configuration validates a conservative sum of connection, RPC, Store and control
envelopes. This is admission accounting, not an allocator or RSS limit: GC slack,
runtime stacks and native allocations remain observable separately. Deployments
must set their OS/container limit and observe overload shedding. Lua is a trusted
program facility with source/value/stack/concurrency/deadline limits, without a
hard VM allocation sandbox.

## Scheduling and backend work

A complete request enters the Store scheduler directly; there is no collection
window or polling. When an execution permit becomes available, the scheduler
combines already queued single-record RPCs and compatible complete small batches
by target namespace, actual input bytes and max_batch_operations. One selected
execution holds one working envelope sized for its largest operation. Large,
multi-namespace and singleton client batches execute independently and split into
sequential bounded groups. Repeated mutation keys start a new sequential wave.
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
permit. Database latency does not train a second concurrency controller. Transport
failures do not replay mutations; the bounded Lua conflict and confirmed-abort
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
