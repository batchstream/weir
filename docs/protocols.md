# Public and peer protocols

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.2.0/api/weir/v1/store.proto),
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

ExecuteRequest contains store_name and one Command. ExecuteResponse contains one
Event. Commands and Events use version 1; unknown fields/versions are rejected.
There are no request IDs, encoded command blobs, Event fragments or separate
request-complete messages. Scan and Native require their terminal Event and final
gRPC OK. A failed unary RPC confirms no batch result; mutations are never
automatically replayed. See [payload contracts](route-payloads.md).

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
PeerDiscoveryService. Removed RPC paths have no aliases.

[weir-protocol](https://github.com/batchstream/weir-protocol) owns canonical public
schemas, generated Go bindings and shared validation/DNS helpers. Both Weir and
[weir-go](https://github.com/batchstream/weir-go) depend on it; it depends on neither.
The SDK has no dependency on the server. Server acceptance tests can consume the
SDK without introducing a cycle. This repository generates only internal peer
bindings with scripts/generate.sh. Future language SDKs can independently generate
the public schema and implement discovery, endpoint refresh, load balancing and
safe typed completion. This change requires protocol v0.2.0 and Go SDK v0.4.0.
