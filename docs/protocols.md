# Public and peer protocols

The public contract is [store.proto](../api/weir/v1/store.proto), package
`weir.v1`. It contains only client discovery and business execution, with no
dependency on the peer schema. A client initializes through any application
endpoint, then sends business requests directly to the returned Store endpoints.

| Public API | Meaning |
| --- | --- |
| `StoreService.ResolveStore` | Find reachable endpoints for one named Store |
| `ResolveStoreRequest.store_name` | The logical Store to discover |
| `ResolveStoreResponse.store_name` | The resolved Store, matching the request |
| `ResolveStoreResponse.endpoints` | IP or DNS host:port values; DNS may identify several replicas |
| `ResolveStoreResponse.cache_ttl_ms` | Remaining time the mapping may be used |
| `StoreService.Execute` | Execute a finite stream of Calls in one local Store |
| `ExecuteRequest.request_id` | Positive, strictly increasing correlation ID within this RPC |
| `ExecuteRequest.store_name` | Store fixed by the first valid request of the stream |
| `ExecuteRequest.call_payload` | Exactly one protobuf-encoded, versioned `Call` |
| `ExecuteResponse.request_id` | Request whose output this frame carries |
| `ExecuteResponse.event_fragment` | Fragment of a length-delimited protobuf `Event` sequence |
| `ExecuteResponse.request_complete` | Separate empty frame ending that request's Events |

An Event can span several response frames. Clients must parse varint-length
delimited Events across arbitrary fragment boundaries. Completing a request also
requires its valid business terminal. Completing the entire RPC requires client
half-close, all request completions and final gRPC OK. Connection failures never
authorize replaying a mutation.

The independent internal contract is
[peer.proto](../internal/api/peer/v1/peer.proto), package `weir.peer.v1`.
`PeerDiscoveryService.SyncDirectory` exchanges bounded snapshots of node
announcements. It has no dependency on public business DTOs.

| Internal field | Meaning |
| --- | --- |
| `announcements` | Node state offered to, or learned from, a peer |
| `incarnation_id` | Fresh identity for each process lifetime |
| `revision` | Monotonic heartbeat version advanced only by the origin |
| `peer_endpoint` | Direct endpoint for node-to-node synchronization |
| `replica_group` | Replicas that provide the same logical Store ownership |
| `store_names` | Complete set of locally served Stores |
| `store_endpoints` | Business endpoints serving those Stores |
| `lease_remaining_ms` | Remaining origin lease; relays cannot extend it |
| `withdrawn` | Explicit withdrawal of this process incarnation |

The server merges same-group endpoints and rejects conflicting Store ownership
before answering ResolveStore. Replica groups and peer membership therefore stay
internal and are absent from the public response.

Application listeners expose only StoreService. Peer listeners expose only
PeerDiscoveryService. Old RPC paths and aliases are removed. Shared endpoint
validation belongs to the public protocol helpers; the production Go client does
not depend on the peer protocol or directory implementation.

The [Go SDK](https://github.com/batchstream/weir-go) is an independent module,
`github.com/batchstream/weir-go` (package `weir`). This server repository owns the
canonical public schemas and generated Go bindings. Public Go helpers in
`api/protocol` and `api/netlimit` share protocol validation and bounded DNS
transport with the SDK; they depend only on public bindings and standard runtime
libraries. The SDK imports no server internals or peer protocol.

The repository currently generates Go bindings for both schemas through
`scripts/generate.sh`. Other language bindings and SDKs are deferred. Future SDKs
can generate the public schema independently, then implement initialization,
endpoint/DNS refresh, load balancing, Event framing and safe completion handling
using this contract. No Kubernetes types or APIs appear in either schema.
