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

Mutate is not a transaction. Same-resource mutations execute in input order,
including after a failed item; different keys can share a physical batch. Separate
RPCs retain normal backend concurrency semantics. There is no durable request
ID, deduplication or exactly-once guarantee.

Execute accepts exactly one version-1 Scan or Native Command and streams typed
version-1 Events. It has no input pump, correlation IDs, byte-fragment assembly
or separate request-complete frame. A scan page emits documents then ScanEnd;
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
active call. No custom HTTP ServeHTTP bridge or static 65-KiB flow-control window
is retained. Both listeners require deployment-isolated networking; the directory
contains only public ownership/address advertisements, never backend credentials.

## Memory and admission

| Budget | Default or bound |
| --- | --- |
| Complete Read/Mutate protobuf request or response | 32 MiB, including repeated-item envelope bytes |
| Record document | 2 MiB; ordinary Read additionally obeys max_read_size |
| Ordinary Read source | Default 16 KiB; configurable 1 KiB–2 MiB |
| Native body | MongoDB 4 MiB; Search 8 MiB |
| Execute Event | 2 MiB plus bounded framing/metadata allowance |
| Store pending requests | 256 entries / 32 MiB of prepared input |
| Store retained results | 128 live entries / 32 MiB |
| Store backend workspace | 384 MiB |
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
and failure envelopes retain reserved space.

Backend working charges cover bounded native replies and decoding scratch.
MongoDB uses its native bounded cursor reply; Search caps multi-get response bytes.
Physical grouping does not multiply the maximum document size by the item count.
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

A complete batch enters the Store scheduler directly; there is no collection
window. Compatible items group by target namespace, action, actual input bytes
and max_batch_operations. Repeated mutation keys start a new sequential wave.
Incompatible operations execute separately. One adapter implementation handles
both record groups and Scan/Native plans.

MongoDB collections and Search indices receive a structural metadata check on
first use, cached per Store for up to 64 targets. Concurrent cold requests share
one check; failed checks are not cached. The oldest completed target is evicted
when full. Structure must remain stable while the Store is open; changes require
reopening the Store. Actual commands still enforce current database permissions.

Each Store dispatches while configured max_concurrency and backend working bytes
permit. Database latency does not train a second concurrency controller. Backend
failures are returned to callers without automatic retries; pending work remains
bounded by queue bytes and deadlines.

Scan fetches one bounded document step at a time and releases the execution permit
before publication. A blocked scan at concurrency one allows independent record
work. Each new page is a new Execute RPC and can select another instance. MongoDB
uses ascending original BSON _id keysets without a retained cursor or cross-page
snapshot. Search carries the latest PIT and search_after values; the backend
snapshot has a 60-second keep-alive. Expiration fails explicitly. A previously
published PIT remains available after a later failed/exhausted page until expiry.
Checkpoint tokens are bounded, opaque and tied to the target/selector/profile;
the checksum detects corruption and is not authentication.

Native execution is a singleton with bounded streaming and backend time. MongoDB
qualification and native command share a deadline; Search counts actual backend
I/O time and pauses that allowance during publication. Lua uses native MongoDB
transactions or Search sequence/primary-term CAS. Transactions are not merged
across requests, and ambiguous failures cannot start a new mutation attempt.

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
