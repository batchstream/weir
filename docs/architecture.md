# Route architecture

Weir is a synchronous finite-batch database router. It has one process and one
public data RPC, `Route(stream Request) returns (stream Response)`. A logical Store
selects either a local adapter/runtime or a static RemoteWeir service. There is no
queue service, persistent task system, worker executable, IPC, compatibility
protocol or migration switch.

## Protocol and completion

`Request{id,destination,payload}` carries one complete Call. The first valid
request fixes the Store, Service and one downstream instance. Every request repeats
the same destination. IDs are positive, strictly increasing unsigned integers;
gaps are legal, reuse and overflow are not. They associate results only: no durable
deduplication or exactly-once guarantee exists.

The payload is protobuf `Call` version 1. Every adapter has one strict preparation
entry, which rejects unknown fields recursively, missing operations and unknown
versions. Adding incompatible semantics requires a new supported version; unknown
versions fail, rather than being guessed from document contents. Configuration
selects MongoDB, Elasticsearch or OpenSearch; the envelope has no backend selector.

The [adapter payload contract](route-payloads.md) defines media profiles, targets
and validation boundaries. A Call contains a read, mutation, scan or bounded native operation. Resource paths
are relative to the Store and canonical, for example `db/collection/s:one` for
MongoDB and `index/s:one` for Search. Full `weir://` wire resources are rejected.
The adapter converts a relative target to its private canonical identity. Same
Store calls can select different collections/indices within a single RPC.

`Response{id,payload,end}` carries arbitrarily split bytes of a length-delimited
sequence of Event version 1 protobuf messages. Individual Events are bounded;
clients can decode one Event at a time. Record Events contain a read result or
mutation outcome. Scans emit document Events followed by ScanEnd with a count and
failure. Native exchanges emit response metadata, byte chunks and NativeEnd with
transport completeness; native errors remain backend data. The execution DTO
Result index mirrors the outer ID for adapter result correlation.

The final response for each ID is a separate empty `end=true` frame. It is valid
only after a complete terminal business Event, with no undecoded bytes. Data or
another end after completion is invalid. A finite RPC succeeds only after input
half-close, completion of every submitted request and final gRPC OK/EOF. There is
no batch Open, batch End or whole-stream result table.

Backend failures are typed business results; route/envelope/transport failures are
gRPC status. A mutation is NOT_STARTED only with evidence that no backend attempt
started, NOT_APPLIED only with definite rejection, APPLIED only with a validated
acknowledgement, otherwise UNKNOWN. A late protocol error or disconnect does not
roll back preceding writes. Uncompleted writes are indeterminate. Weir never
replays a possible mutation. Received acknowledged outcomes remain evidence even
if another request or final transport status fails.

## Routing and transport

One inbound RPC owns one outbound Route RPC. Downstream selection happens once;
health changes cannot move a live batch. Remote service channels and bounded DNS/
socket ownership survive between RPCs. Inbound and outbound pumps operate
concurrently. Each request is received, validated and sent before another is read;
each response is received and sent before another is read. A slow send therefore
stops application reads in that direction.

Upstream input EOF only half-closes downstream sending; receiving drains the
remaining responses. Original cancellation and remaining deadline propagate via
the outgoing context. Peer ingress requires a canonical forwarding budget 0..8;
each forward decrements it. Application ingress supplies the configured initial
budget and rejects client-supplied reserved hop metadata. Metadata is not
authentication: deploy the plaintext application/peer listeners on an isolated
network. Only bounded diagnostics and hop metadata are forwarded.

A finite Route lifetime and progress timeout cover decoding, backend work, sends
and HTTP2 trailers. Handler completion and delivery completion jointly own the
shared ingress slot. Shutdown stops new admission/input while admitted work drains
within the bounded shutdown deadline. Cancellation/error paths interrupt input,
close downstream streams and join pumps; stalled transport can close its bounded
connection, affecting co-resident RPCs whose unfinished writes remain indeterminate.

## Memory and backpressure

| Budget | Bound |
| --- | --- |
| Complete Call | 9 MiB; gRPC Request limit adds 128 bytes envelope allowance |
| Record | 2 MiB opaque document; typed Lua values remain limited to 256 KiB |
| Native body | Mongo command 4 MiB; Search body 8 MiB |
| Response frame | 64 KiB payload plus bounded protobuf overhead |
| Event | 2 MiB plus 8 KiB metadata/framing allowance |
| RPC correlation ledger | 8 unfinished IDs and 16 MiB request wire charges |
| Store pending input | 256 operations / 32 MiB including prepared copies |
| Store retained output | 128 operations / 32 MiB reservations |
| Store backend workspace | 128 MiB; physical batch reserves 24 MiB scratch |
| Physical batch output | 8 MiB worst-case reservations, so at most 3 maximum-size reads |
| Physical batch input | 8 MiB charged bytes and configured operation cap (default 16) |
| Default process ingress | 4 RPCs, 16 accepted connections shared by both listeners |

A request credit is reserved before application Recv. The ledger stores only ID
and byte charges and deletes them when the request end is sent. A single decoded
input waiting for downstream/scheduler admission is included in the RPC envelope;
no historical ID set, whole-batch buffering or unbounded application channel exists.
Event queues contain one borrowed Event. The emitter retains its reservation until
this node finishes sending that Event; Send is not acknowledgement of peer receipt.

`ServeHTTP` gRPC otherwise drains input eagerly: creditedBody explicitly bounds
read-ahead to one maximum request frame. HTTP2 receive windows are 65535 bytes per
stream/connection with 16 KiB frames. Outbound peer channels use static 65535-byte
windows, 16 KiB read/write buffers, bounded sockets, zero retry history and bounded
message/header sizes. The declared per-RPC 64 MiB envelope counts input read-ahead,
protobuf decoding/encoding copies, one response encoding and transport buffers in
addition to the application ledger. Result/backend reservations are separately
bounded per Store. Increasing batch length does not increase these live budgets.

App configuration rejects a process memory threshold smaller than the conservative
sum of RPC, connection, Store and endpoint envelopes. The default threshold is
1 GiB. It is an admission/overload threshold, not an OS allocator/RSS limit: GC
slack, native allocations and runtime overhead remain observable separately.
Lua programs must be trusted: the in-process VM has source/value/stack limits,
four concurrent evaluations and a 500 ms deadline, but no hard VM allocation cap.
It must not be described as an allocation sandbox.

## Execution and batching

One Store scheduler admits every Call with input, output and workspace charges.
It selects compatible adapter batch keys under count/input/result/workspace bounds,
and waits at most 1 ms (maximum configurable collect window 10 ms). A near-deadline
item dispatches without waiting to fill the batch. Ordinary MongoDB requests batch
by namespace; Search requests batch by concrete index. Options and transaction
semantics are validated by the adapter; incompatible work executes singly.

Responses for different IDs can interleave and arrive out of order, including
responses for the same record; fragments within one ID stay in order. Within one RPC,
requests targeting the same record execute in input order, including across
physical batches; active keys are released after execution, without historical
state. Separate RPCs and native requests retain backend concurrency semantics;
request IDs do not serialize external writers or create transactions.

A physical batch uses a shared bounded backend context and each plan retains its
caller context. One expired/canceled item is skipped before its next attempt,
without canceling other interested callers. When all abandon a batch, backend work
is canceled. Record results are published through independently bounded ticket
publishers after the execution permit is released, so a slow RPC does not block
other RPCs in the same physical batch.

Scan is a continuation of the same admitted ticket: one bounded page per scheduler
step, no prefetch, FIFO continuation after publication, with cursor/PIT owned by the
adapter and bounded cancellation cleanup. Sending a page does not retain an
execution permit, so a stalled scan at concurrency one permits short record work.
Native exchange is a singleton that can hold one execution permit during bounded
backend streaming; slow output is canceled by the transport progress budget.

Lua runs in the main Weir process. MongoDB read-modify-write programs preserve
independent transactions and same-transaction commit resolution; Search preserves
native sequence/primary-term CAS. Arbitrary write failures cannot start a new
mutation attempt. Lua transactions are never merged into one cross-request
transaction merely to form a batch. Partial successes and unknown acknowledgements
retain their per-request evidence.

## Clients and validation

`routeclient.Run` accepts an incremental producer and consumer and synchronously
returns after all request ends and final status. It sends and receives concurrently
with eight input slots/16 MiB charges. Complete observes each validated request end;
Consume exposes bounded incremental Events. Callbacks must honor context and release
Events on return. `Record` collects one bounded read/write result; collecting an
entire batch in application callbacks requires memory for that entire batch.
Reuse a connection across finite RPCs; a permanently open stream is unnecessary.
Use static gRPC windows/buffers for the same client transport budget as peers.

Default Go/Python tests are offline safe. Real MongoDB/Search suites use explicit
integration flags and owned loopback fixtures. The coverage map and validation
report distinguish mock/transport proof, actual database proof, current measurements
and historical reports. Independent PR review runs against the current commit and
must be repeated after confirmed fixes. This refactor does not authorize deployment.
