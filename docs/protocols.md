# Public and peer protocols

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.6.0/api/weir/v1/store.proto),
package weir.v1. Clients initialize through any application endpoint and send
business requests directly to the returned Store endpoints. No Kubernetes types
or peer membership appear in this schema.

| Public RPC | Meaning |
| --- | --- |
| StoreService.ResolveStore | Discover IP/DNS endpoints and cache TTL for one named Store |
| StoreService.Execute | Exchange bounded typed requests and indexed results on a finite bidirectional stream |

ExecuteRequest contains store_name, a consecutive one-based index and Command.
Commands select one ReadRequest, MutateRequest, ScanRequest or NativeRequest; a
stream fixes one Store and one kind. Each record is validated before its effects.
The input half-close ends the logical request sequence, which has no total item
or byte limit. Document.content_type identifies the actual backend representation;
Read and Scan do not negotiate alternative output encodings.

ExecuteResponse contains index and Event. Record results are emitted individually
in ordinal order. Scan and Native emit typed events at index 1 and require a valid
terminal Event plus final gRPC OK. Receiving and publication run concurrently
through a bounded ticket queue. The Store scheduler aggregates compatible records
within and across clients. Clients send and receive concurrently to permit
backpressure in both directions.

Mutate is not an atomic transaction. Same-resource mutations execute in stream
input order; an invalid later request does not undo earlier writes. Confirmed item
results remain evidence after a transport error; missing acknowledgements remain
unknown and are never replayed automatically. MongoDB Lua record groups can share
an internal short transaction with group rollback on database write error; Search
uses per-item version conditions. Neither path adds document metadata.

The independent internal [peer.proto](../internal/api/peer/v1/peer.proto), package
weir.peer.v1, exposes PeerDiscoveryService.SyncDirectory. It exchanges bounded
snapshots without depending on public business DTOs.

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

Same-group endpoints merge; conflicting Store ownership fails ResolveStore.
Replica groups stay internal and do not appear in its public response.
Application listeners expose only StoreService; peer listeners expose only
PeerDiscoveryService.

[weir-protocol](https://github.com/batchstream/weir-protocol) owns canonical public
schemas, generated Go bindings and shared validation/DNS helpers. Both Weir and
[weir-go](https://github.com/batchstream/weir-go) depend on it; it depends on neither.
The SDK has no dependency on the server. Server acceptance tests can consume the
SDK without introducing a cycle. This repository generates only internal peer
bindings with scripts/generate.sh. Future language SDKs can independently generate
the public schema and implement discovery, endpoint refresh, load balancing and
safe typed completion. The current dependencies are protocol v0.6.0 and Go SDK v0.8.0.

Internal execution plans retain scheduling budgets and backend state. They borrow
the public Command and publish the public Event directly; no second request or
result DTO hierarchy duplicates the protocol.
