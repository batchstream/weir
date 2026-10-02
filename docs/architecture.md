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
mutation outcome. Scans emit a finite page of document Events followed by ScanEnd with a page count,
failure, and either a next continuation token or explicit exhaustion. Native exchanges emit response metadata, byte chunks and NativeEnd with
transport completeness; native errors remain backend data. The execution DTO
Result index mirrors the outer ID for adapter result correlation.

The final response for each ID is a separate empty `end=true` frame. It is valid
only after a complete terminal business Event, with no undecoded bytes. Data or
another end after completion is invalid. A finite RPC succeeds only after input
half-close, completion of every submitted request and final gRPC OK/EOF. There is
no batch Open, batch End or whole-stream result table.

Backend failures are typed business results; route/envelope/transport failures are
gRPC status. A mutation is NOT_STARTED only with evidence that no backend attempt
started, NOT_APPLIED only with definite rejection or definite evidence that the
request was not sent, APPLIED only with a validated
acknowledgement, otherwise UNKNOWN. A late protocol error or disconnect does not
roll back preceding writes. Uncompleted writes are indeterminate. Weir never
replays a possible mutation. Received acknowledged outcomes remain evidence even
if another request or final transport status fails.
APPLIED can include a typed failure of a later acknowledgement step, such as
replica confirmation; the failure does not erase positive application evidence.

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
and waits at most `local.batch_collect` (default 1 ms, configurable 0–10 ms). A near-deadline
item dispatches without waiting to fill the batch. Ordinary MongoDB requests batch
by namespace; Search requests batch by concrete index. Options and transaction
semantics are validated by the adapter; incompatible work executes singly.

Ordinary Record Read reserves `local.max_read_size` source bytes (default 2 MiB,
configurable 1 KiB–2 MiB). Smaller declarations allow more small reads within the
same bounded batch result budget. An oversized source fails with
`RESOURCE_EXHAUSTED`; this setting does not constrain writes, Scan, Native or
Lua/expression results. Search bounds each multi-get response by these source
reservations. MongoDB qualifies a namespace once within a compatible mixed
physical batch, then performs the read and write phases; the next batch qualifies
again so metadata and permissions are not retained across executions.

Each Store maintains one adaptive execution window, bounded by `max_concurrency`.
Explicit database congestion or an owned backend timeout halves the window and
pauses dispatch for 100–300 ms. Successful record batches and Scan pages also
train at most 64 latency profiles, grouped by target, operation kind, operation
count and average input size. Four healthy samples establish a mean baseline;
ordinary successful completions then update it in both directions with a 1/8
EWMA. Queued slow work does not raise the baseline, so sustained pressure cannot
become normal merely through continued observation. Three
slow saturated samples within one second spanning at least 100 ms, exceeding
twice that baseline plus 2 ms and representing at least 10% of comparable healthy
batch or Scan-page completions in that profile, reduce the window by one when
compatible work is waiting. Healthy completions without queue pressure also
count in that denominator. This is a bounded evidence interval that resets on
confirmation or after one second, rather than a full sliding-second record ratio. Saturation includes
workspace pressure that prevents the next independent batch from fitting.
Successful slow work continues at window one; repeated explicit congestion can
still pause dispatch there. Recovery requires healthy saturated work and at
least 250 ms between increases, with one second free of confirmed latency
pressure before recovery. Fast quota bursts preserve recent slow evidence;
tails below the confirmation thresholds cannot renew the shared recovery hold. Confirmed pressure
on another target still blocks Store recovery.
Native streaming and Lua execution do not train
these latency profiles; complete MongoDB `ok: 1` replies and Search GET 2xx
replies can probe one additional slot per second. Opaque Search POST replies
are neutral. Successful work from an older window contributes comparable
latency statistics but cannot change the new window or renew its recovery hold.
Canceled callers and partial canceled batches cannot train latency, erase
independent pressure evidence or trigger recovery. Slow successful responses are an overload signal, rather than a direct
measurement of database CPU: Weir CPU pressure can also increase adapter latency.

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

Search records can prove a failed bulk write was not applied when the standard
HTTP transport never acquired a connection, or execution stopped before the HTTP
attempt. After connection acquisition, an incomplete acknowledgement remains
UNKNOWN; writes are not replayed. A received valid mutation result remains
evidence even when a later RPC end frame or trailer is lost.

Canceling a blocked response interrupts that HTTP/2 stream through its owned
deadlines. A transport stall watchdog can still close the shared connection to
bound an unresponsive peer. Cancellation alone does not close sibling streams.

Scan admits one finite page per Call: `page_size=0` selects 128 documents, and the
maximum is 256. Each scheduler step fetches at most one document, with no prefetch
and FIFO continuation after publication. Sending a document does not retain an
execution permit, so a stalled scan at concurrency one permits short record work.

The client starts a new finite Route RPC for each page and supplies the previous
`next_continuation_token`. A live Route remains pinned to one downstream instance,
but the next RPC can select any instance serving the same Store and backend. No
Weir session, process registry or shared task storage survives between scan pages.
A page succeeds only after its ScanEnd count, request end frame and final gRPC OK;
only then may the client commit its token. If delivery fails, the previous token
remains the checkpoint. Replaying a page can repeat already consumed documents.

MongoDB performs a bounded `find` with `singleBatch=true`, ascending `_id` sort
and the `_id` index. The token carries the last original BSON `_id`; the next query
uses an inclusive index lower bound and excludes that ID. This preserves BSON
cross-type index ordering without a retained MongoDB session or cursor. Projection
must preserve the original `_id`, and alternate sort orders are rejected. Each
query sees its own database state; concurrent inserts, deletes or filter changes
can change the traversal. This is not a snapshot across pages.

Search's token carries the latest backend PIT ID and `search_after` value. The PIT
preserves the index snapshot and can be used by another Weir instance. Once a PIT
has been passed to the client, later page failures and exhaustion leave it available
until its 60-second keep-alive expires. This lets the client retry its last committed
checkpoint when a terminal response is lost. A newly opened PIT that has never been
passed to the client is cleaned up on failure or immediate exhaustion. An expired
PIT fails rather than silently starting a different snapshot.

Continuation tokens are bounded, versioned client inputs tied to the Store,
resource, selector, representation and backend dialect. They are opaque to clients;
their checksum detects corruption and is not authentication. Backend permissions
remain the access boundary. This replaces the old whole-traversal Scan semantics;
executors, relays and clients must upgrade together because unknown fields fail
strict validation.
Native exchange is a singleton that can hold one execution permit during bounded
backend streaming; slow output is canceled by the transport progress budget.
The backend timeout defaults to 2 seconds. Native MongoDB qualification and command
execution share one deadline; Search accumulates qualification, request and body
read time, pausing its I/O allowance while publishing responses. Backpressure
does not consume that allowance or grant a fresh allowance on the next read.
Scan pages and Lua operations retain the scheduler's backend timeout; their
publication starts after execution, outside that timeout.

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
`Record` can return a validated result together with an RPC error after a lost
end frame or failing final status. Preserve that result as backend evidence and
check the error separately for complete RPC success.
Reuse a connection across finite RPCs; a permanently open stream is unnecessary.
Use static gRPC windows/buffers for the same client transport budget as peers.

Default Go/Python tests are offline safe. Real MongoDB/Search suites use explicit
integration flags and owned loopback fixtures. The coverage map and validation
report distinguish mock/transport proof, actual database proof, current measurements
and historical reports. Independent PR review runs against the current commit and
must be repeated after confirmed fixes. This refactor does not authorize deployment.
