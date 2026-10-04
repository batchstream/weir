# Public and peer protocols

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.3.0/api/weir/v1/store.proto),
package weir.v1. Clients initialize through any application endpoint and send
business requests directly to the returned Store endpoints. No Kubernetes types
or peer membership appear in this schema.

| Public RPC | Meaning |
| --- | --- |
| StoreService.ResolveStore | Discover IP/DNS endpoints and cache TTL for one named Store |
| StoreService.Read | Read a complete ordered batch in one unary RPC |
| StoreService.Mutate | Apply a complete ordered mutation batch in one unary RPC |
| StoreService.Execute | Stream Events for one Scan or Native Command |

ReadBatchRequest and MutateBatchRequest contain store_name and requests.
Their responses contain results in the same positions as inputs. All input is
validated before effects; individual business failures remain positional results.
Full encoded requests and responses are bounded to 32 MiB. Mutate is not an atomic
transaction; same-resource mutations execute in input order.
The server can combine compatible queued RPCs into one backend execution.
Each RPC keeps independent positional results, cancellation and response budgets;
this aggregation gives callers no application transaction guarantee. MongoDB Lua
items can share an internal short transaction, with whole-group rollback on a
database write error. Search Lua items use individual version conditions within
bulk writes. Neither path adds metadata fields to business documents, and an
ambiguous write or commit cannot start a new mutation attempt.

ExecuteRequest contains store_name and one Command. ExecuteResponse contains one
Event. Unknown protobuf fields and missing variants are rejected. Scan and Native require their terminal Event and final
gRPC OK. A failed unary RPC confirms no batch result; mutations are never
automatically replayed. See [payload contracts](payloads.md).

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
safe typed completion. The current dependencies are protocol v0.3.0 and Go SDK v0.5.0.

Internal execution plans and positional result associations are ordinary Go values
owned by the server. They are not public protobuf messages and do not appear on
the wire. Public schema messages describe only client-visible RPC data.
