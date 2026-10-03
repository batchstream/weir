# Direct Execute redesign and acceptance

Clients initialize through any Weir application's ResolveStore, then connect directly
to the returned Store group's business addresses. Equal peers synchronize owner
advertisements through periodic PeerDiscoveryService.SyncDirectory. The directory and its cache
are bounded and leased; URI affinity is deferred. Remote Store configuration,
business forwarding and hop budgets are removed. See [discovery design](discovery-design.md).

One finite bidirectional Execute RPC carries strictly increasing positive IDs to one
local Store. The first valid envelope fixes the Store runtime, and the client
transport pins that RPC to one instance. Input half-close stops new requests while
remaining responses drain. Directory or DNS changes affect subsequent RPCs.

Command version 1 is a strict protobuf schema selected by Store configuration. Its
target is relative to the Store. Records, scans and bounded native exchanges share
one admission and scheduler. Backend transaction/CAS and per-item outcome
verification remain adapter responsibilities. Lua evaluates in the main process.

Responses form a length-delimited stream of version 1 Events. Frames carry at most
64 KiB; a separate empty terminal end completes each request. Success requires all
request terminals and final gRPC OK. Transport failure leaves unfinished writes
indeterminate; IDs associate results and no business request is replayed.

Bounds remain: a complete Command at most 9 MiB, native body at most 8 MiB, record at
most 2 MiB, eight unfinished operations and 16 MiB input charges per RPC. Execution
reserves bounded result and decoding workspace before dispatch. One read-ahead
message, codec copies, HTTP/2 buffers and connections have separate accounting.
Credits are released as requests finish; there is no historical ID set.

Acceptance requires default offline tests, vet and race; directory convergence,
lease expiry without resurrection and ownership conflicts; direct initialization,
DNS replacement and client cleanup; preserved ordering, cancellation, half-close,
backpressure, partial outcomes, Lua atomicity and shutdown. Independent process
and backend profiles must verify persisted direct operations and instance changes.
The slow-consumer profile transfers 200 and 800 MiB through a direct executor with
identical record size and active RPC count, measuring object, heap, RSS and queue
recovery. Tagged compilation does not substitute for live execution.

The delivered PR requires an independent Agent review after creation, confirmed
issue fixes and review of the final commit. The user authorized merge after review
and validation; release and deployment are outside this change.
