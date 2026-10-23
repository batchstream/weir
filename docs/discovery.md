# Store discovery and direct business traffic

The public StoreService exposes ResolveStore and bidirectional Execute for
individual Read/Mutate requests or one Native command. PeerDiscoveryService
exchanges Store ownership announcements with SyncDirectory. All run in one Weir
process.

The public and peer schemas are independent; see [protocol boundaries and
names](protocols.md).

## Topology

Clients can initialize through any Weir application address. A normal
load-balanced seed service can select any group: every peer eventually learns
the same Store ownership directory. ResolveStore returns the provider group's
reachable host:port business endpoints. The client connects directly to those
endpoints. Business RPCs accept a locally configured Store.

The protocol uses Store names, replica group identities, instance advertisements
and addresses. It does not use Kubernetes objects or APIs. On VMs/containers,
instances can publish individual IP/hostname targets or shared multi-address
DNS. On Kubernetes, groups can publish their headless Service DNS; the all-group
S1 service is an ordinary initialization and peer-bootstrap Service. S1 need not
expose all Pod IPs to clients. Each peer publishes a directly reachable peer
address so bootstrap can return concrete peers for later synchronization.

## Ownership advertisements

Every process gets a fresh random node ID and monotonically increasing heartbeat
sequence. An advertisement contains its peer address, replica group, complete
local Store names, business endpoints, revision, remaining lease, immutable
origin lease duration and withdrawal state. Backend URIs, authentication and
backend configuration are never synchronized.

Same-group live advertisements contribute the group's Store targets. Identical
shared DNS targets deduplicate; individual addresses union across replicas. If a
Store is advertised by different groups, ResolveStore returns an explicit
ownership conflict. Synchronization does not resolve conflicting ownership by
arrival order.

An owner periodically advances its sequence and renews its own configured lease.
Relays send only remaining lifetime. Each receiver anchors expiry to its own
monotonic time recorded before the sender formed that snapshot, so network delay
can only shorten a relayed lease; no synchronized wall clock is required.
Receiving the same or an older sequence never extends expiry. Withdrawal is
terminal for that incarnation; a restarted process uses a new node ID.

## Directory synchronization

A background loop periodically exchanges snapshots with known direct peers and
bootstrap seeds. Bootstrap seed connections are recreated to avoid forever
selecting the same Pod behind an ordinary service. Learning peer addresses joins
initially separate membership islands.

Synchronization uses short deadlines and lease-based expiry:

| Setting | Default | Meaning |
| --- | --- | --- |
| `discovery.sync_interval` | `1s` | Lease renewal and one round-robin bootstrap seed visit per round |
| `discovery.sync_timeout` | `2s` | Whole peer exchange, including DNS, connection and handshake |
| `discovery.lease` | `60s` | This node's origin lease, in whole milliseconds |

All three durations must be positive, and the lease must exceed the sync
interval plus two exchange timeouts. Different nodes can choose different lease
and timeout policies; the receiving peer also applies its own exchange deadline.

Each SyncDirectory call is one finite bidirectional exchange in which both sides
send one current snapshot. The receiver installs the initiator's snapshot after
validating it and the input half-close; the initiator installs the receiver's
snapshot only after final gRPC OK. Malformed snapshots are never partially
installed, and already expired entries are ignored. Both nodes learn membership
through the same connection, even when only the initiator has a bootstrap seed.
Peer exchanges never carry business payloads.

Shutdown cancels synchronization and makes a best-effort withdrawal bounded by
the caller's drain deadline.

This is eventual consistency. A new or isolated node can temporarily lack a
mapping. ResolveStore returns a transient discovery error for an unavailable
mapping, not a claim that a globally unknown Store permanently does not exist.
Startup and client initialization have caller-visible bounded deadlines.

## Clients

The [weir-go](https://github.com/batchstream/weir-go) SDK resolves Stores
through a seed, caches mappings for the owner lease-derived TTL, refreshes DNS
and connects directly to business endpoints; see its documentation for client
lifecycle and connection handling. Endpoint changes never migrate or replay an
active business RPC: a write interrupted by a failed connection remains
indeterminate.
