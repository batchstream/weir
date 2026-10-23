# Public and peer protocols

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.14.0/api/weir/v1/store.proto),
package weir.v1. Clients initialize through any application endpoint and send
business requests directly to the returned Store endpoints. No Kubernetes types
or peer membership appear in this schema.

| Public RPC | Meaning |
| --- | --- |
| StoreService.ResolveStore | Discover IP/DNS endpoints and cache TTL for one named Store |
| StoreService.Execute | Exchange typed requests and indexed results on a finite bidirectional stream |

Execute accepts typed records, open Native forwarding and wait-until-visible
mutations on one finite stream.

ExecuteRequest contains store_name, a consecutive one-based index and Command.
Commands select one ReadRequest, MutateRequest or NativeRequest; a
stream fixes one Store and one kind. Each record is validated before its effects.
The input half-close ends the logical request sequence, which has no total item
or byte limit. Document.content_type identifies the actual backend representation;
Read does not negotiate alternative output encodings. NativeRequest carries
a resource and an opaque request Document; NativeHead carries optional metadata
and a body content type. Shared validation checks envelopes while each adapter
interprets its content type and bytes.

ExecuteResponse contains index and Event. Record results are emitted individually
in ordinal order. Native emits typed events at index 1 and require a valid
terminal Event plus final gRPC OK. Receiving and publication run concurrently
through a bounded ticket queue. The Store scheduler aggregates compatible records
within and across clients. Clients send and receive concurrently to permit
backpressure in both directions.

Request envelopes reject unknown fields, enum values and operation variants,
because ignoring an unrecognized write condition or command option could change
its effects.
Shared validation checks every decoded message, including unknown fields.
Response readers reject unknown fields and undeclared failure codes, and check
result evidence, event kinds and completion rules against the current schema.

Read and Mutate are the core batched operations. Querying, counting and traversal
use Native with the backend's own request and response formats. Weir does not
interpret their pagination, result counts or cursor lifetimes.

Mutate is not an atomic transaction. Same-resource mutations execute in stream
input order; an invalid later request does not undo earlier writes. Confirmed item
results remain evidence after a transport error; missing acknowledgements remain
unknown and are never replayed automatically. MongoDB Lua record groups can share
an internal short transaction with group rollback on database write error; Search
uses per-item version conditions. Neither path adds document metadata.

The independent internal [peer.proto](../internal/api/peer/v1/peer.proto), package
weir.peer.v1, exposes PeerDiscoveryService.SyncDirectory as a finite bidirectional
stream. The initiator sends an empty request, receives one snapshot, sends its own
snapshot and half-closes; final gRPC OK completes the exchange. Each receiver
records a local anchor before the sender forms its snapshot and computes expiry
as that anchor plus remaining lifetime. The receiver requires validated input
half-close before installing the snapshot; the initiator requires exactly one
response and final gRPC OK. It exchanges discovery metadata
without depending on public business DTOs.

| Internal field | Meaning |
| --- | --- |
| announcements | Node state offered to or learned from a peer |
| incarnation_id | Fresh identity for each process lifetime |
| revision | Monotonic heartbeat version advanced only by the origin |
| peer_endpoint | Direct node-to-node synchronization endpoint |
| replica_group | Replicas with equivalent logical Store ownership |
| store_names | Complete locally served Store set |
| store_endpoints | Business endpoints for those Stores |
| lease_remaining_ms | Remaining origin lease; relays cannot extend it |
| withdrawn | Explicit withdrawal of a process incarnation |
| origin_lease_ms | Full origin lease, immutable for an incarnation; bounds replay watermark retention independently of receiver policy |

Same-group endpoints merge; conflicting Store ownership fails ResolveStore.
Replica groups stay internal and do not appear in its public response.
Same or older revisions never extend expiry or watermark retention. A withdrawn
incarnation cannot return to live state. Different origin lease policies are
accepted. Only positive millisecond values representable by `time.Duration` and
remaining lifetime no greater than the full origin lease are accepted.
Application listeners expose only StoreService; peer listeners expose only
PeerDiscoveryService.

[weir-protocol](https://github.com/batchstream/weir-protocol) owns canonical public
schemas, generated Go bindings and shared validation/DNS helpers. Both Weir and
[weir-go](https://github.com/batchstream/weir-go) depend on it; it depends on neither.
The SDK has no dependency on the server, and the server binary does not use the
SDK; only server tests consume it. This repository generates only internal
peer bindings with `scripts/generate.sh`. Clients in other languages can generate
the public schema and implement discovery, endpoint refresh, load balancing and
typed completion handling. Weir uses protocol v0.14.0 and Go SDK v0.16.0.
