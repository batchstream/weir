# Public and peer protocols

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.8.0/api/weir/v1/store.proto),
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
Read and Scan do not negotiate alternative output encodings. NativeRequest carries
a resource and an opaque request Document; NativeHead carries optional metadata
and a body content type. Shared validation bounds envelopes while each adapter
interprets its content type and bytes. No database-specific public DTO is required
to add another Store.

ExecuteResponse contains index and Event. Record results are emitted individually
in ordinal order. Scan and Native emit typed events at index 1 and require a valid
terminal Event plus final gRPC OK. Receiving and publication run concurrently
through a bounded ticket queue. The Store scheduler aggregates compatible records
within and across clients. Clients send and receive concurrently to permit
backpressure in both directions.

Request envelopes reject unknown fields, enum values and operation variants;
ignoring a future write condition or command option could change its effects.
The wire validator checks known field framing before allocation, and shared
validation checks the decoded values. Response readers accept additive unknown
fields and retain unknown positive failure codes as failures, while still checking
known result evidence, event kinds and completion rules. New execution behavior
must have an explicit supported contract boundary. After this initial baseline,
published field numbers and types are fixed; removed fields must be reserved.

Scan is read-only and returns documents without a separate record identity. A
continuation fixes the Store, resource, exact filter representation, projection
and versioned backend traversal profile; the next page may change page_size.
Its envelope and fingerprint use explicit version 1 formats. MongoDB profile
`mongodb:v1` retains a native BSON _id checkpoint in ascending BSON order. Search
profiles `search:elasticsearch:v1` and `search:opensearch:v1` retain PIT and sort
checkpoints. Adaptive batch capacities and process-local scheduling state are not
part of a token.

Any equivalent node serving the same Store can resume a token; the backing target
and Store configuration must still describe the same logical data. MongoDB uses
keyset reads without a cross-page snapshot or protocol-imposed expiry; intervening
inserts, deletes and updates can change later pages. Search uses a backend PIT with
60s keep_alive renewed by successful PIT search requests. Expired or lost
PITs fail continuation without automatically restarting traversal. Tokens are
opaque bounded checkpoints and do not grant additional access to backend data.
Rolling upgrades must retain readers for issued envelope/fingerprint/profile
formats while their tokens can remain valid. Internal tuning can change without
invalidating a token; changing traversal semantics requires a new profile and an
explicit token retirement policy, especially for unexpired MongoDB tokens.

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
safe typed completion. The current dependencies are protocol v0.8.0 and Go SDK v0.10.0.

Internal execution plans retain scheduling budgets and backend state. They borrow
the public Command and publish the public Event directly; no second request or
result DTO hierarchy duplicates the protocol.
