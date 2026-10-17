# Store discovery and direct business traffic

The public StoreService exposes ResolveStore and bidirectional Execute for
individual Read/Mutate requests or one Native command. PeerDiscoveryService exchanges Store
ownership announcements with SyncDirectory. All run in one Weir process.

The public and peer schemas are independent; see [protocol boundaries and names](protocols.md).

## Topology

Clients can initialize through any Weir application address. A normal load-balanced
seed service can select any group: every peer eventually learns the same Store
ownership directory. ResolveStore returns the provider group's reachable host:port
business endpoints. The client connects directly
to those endpoints. Business RPCs accept a locally configured Store.

The protocol uses Store names, replica group identities, instance advertisements
and addresses. It does not use Kubernetes objects or APIs. On VMs/containers,
instances can publish individual IP/hostname targets or shared multi-address DNS.
On Kubernetes, groups can publish their headless Service DNS; the all-group S1
service is an ordinary initialization and peer-bootstrap Service. S1 need not
expose all Pod IPs to clients. Each peer publishes a directly reachable peer
address so bootstrap can return concrete peers for later synchronization.

## Ownership advertisements

Every process gets a fresh random node ID and monotonically increasing heartbeat
sequence. An advertisement contains its peer address, replica group, complete local Store
names, business endpoints, revision, remaining lease, immutable origin lease
duration and withdrawal state. Backend
URIs, authentication and backend configuration are never synchronized.

Same-group live advertisements contribute the group's Store targets. Identical
shared DNS targets deduplicate; individual addresses union across replicas. If a
Store is advertised by different groups, ResolveStore returns an explicit ownership
conflict. Synchronization does not resolve conflicting ownership by arrival order.

An owner periodically advances its sequence and renews its own configured lease.
Relays send only remaining lifetime. Each receiver anchors expiry to its own
monotonic time recorded before the sender formed that snapshot. The complete
exchange delay is deducted; no synchronized wall clock or fixed transit allowance
is required. Receiving the same or an older sequence cannot extend either its
expiry or its replay watermark. Withdrawal is terminal for that incarnation,
including when a later live revision is offered. New processes use new node IDs.

Each accepted remote version retains its high-water mark until the local receipt
time plus that origin's full lease, independently of this receiver's lease policy.
The origin's lease duration is immutable across an incarnation's revisions; an
exchange changing a known origin's duration is rejected before any mutation.
Every older version was generated before the accepted version, so even its full
origin lease expires by that retention deadline. Relays cannot extend it, and an
in-flight snapshot arriving after collection has an already expired anchored
deadline and is ignored. Replaying a revision never moves the collection time.

## Directory synchronization

A background loop periodically exchanges snapshots with known direct peers and bootstrap
seeds. Bootstrap seed connections are recreated to avoid forever selecting the
same Pod behind an ordinary service. Learning peer addresses joins initially
separate membership islands. The algorithm is periodic anti-entropy for this
ownership directory; it does not implement a new general-purpose SWIM stack.

Synchronization uses short deadlines and lease-based expiry. There are no
independent heartbeat or seed timers: `discovery.sync_interval` (default `1s`)
controls renewal and a round-robin bootstrap seed visit during each round.
`discovery.sync_timeout` (default `2s`) bounds the entire peer exchange, including
DNS, connection establishment and the complete handshake. The receiving peer
also applies its own configured exchange deadline, including when the caller
provides none. `discovery.lease` (default `60s`) is this node's origin lease;
different nodes can choose different lease and timeout policies. These defaults
are not maximum values. All three durations must be positive; only the lease
requires whole milliseconds for its wire encoding. The lease must exceed the
sync interval plus two exchange timeouts, covering one round's peer and seed
visits. Addition is validated without duration overflow. The wire accepts any
positive origin lease representable as Go `time.Duration` milliseconds, with
remaining lifetime no greater than that origin lease.

SyncDirectory is one finite bidirectional gRPC stream, with exactly this sequence:

1. The initiator records its local anchor and sends an empty opening request.
2. The receiver records its own local anchor and sends one current snapshot.
3. The initiator validates the response envelope, sends its own current snapshot,
   and half-closes input. The receiver validates that this is the only remaining
   request and atomically merges it before returning final gRPC OK.
4. The initiator requires exactly one response and final gRPC OK before atomically
   merging the receiver's snapshot. In both directions, expiry is the local anchor
   plus advertised remaining lifetime. Already expired entries are ignored.

Each anchor precedes formation of the corresponding remote snapshot; arbitrary
network delay can therefore only shorten a relayed lease. Extra messages and
malformed snapshots do not partially install that received snapshot. The receiver
requires validated input half-close; the initiator additionally requires final
gRPC OK. A lost final status can leave the receiver's completed merge installed,
but the initiator installs nothing from a failed exchange.
Both directions learn membership through the same connection, including when
only the initiating node has a bootstrap seed. Shutdown attempts withdrawals until its caller's drain deadline, without a fixed
peer-count cutoff. There are no Weir
message, node, Store or endpoint count quotas. A malformed exchange is rejected
before partial state mutation. No
peer control contains business payloads. Shutdown cancels synchronization, joins
owned workers and makes a bounded best-effort withdrawal.

This is eventual consistency. A new or isolated node can temporarily lack a
mapping. ResolveStore returns a transient discovery error for an unavailable mapping,
not a claim that a globally unknown Store permanently does not exist. Startup
and client initialization have caller-visible bounded deadlines.

## Client lifecycle

Open resolves every requested Store and waits for at least one usable direct
business connection per Store before returning a client. The initialization seed
is used for control requests only and is closed after all Store metadata is
resolved, before opening business channels. Business methods reject uninitialized
Stores.

The client periodically refreshes directory mappings and actively resolves DNS.
Refresh borrows a ready business connection for ResolveStore: every application node
is a directory entry point. Transient failures can use one temporary seed
connection shared by the round. Borrowed connections are never closed by refresh.
Four workers can resolve DNS while at most two ResolveStore calls run concurrently.
Bounded rounds try least-recently-attempted Stores first; failures also advance
that order, and a canceled request waiting for control admission does not. This
prevents slow or unavailable Stores from permanently starving other mappings.
New RPCs round-robin across ready physical addresses. Store mappings have the
owner lease-derived cache TTL; transient seed errors can retain only an unexpired
last-good mapping. Ownership conflicts invalidate the cached mapping. Returned
targets and DNS answer sets are validated before installation, without count quotas.

DNS updates add/remove physical channels without migrating or replaying an active
business RPC. A drained/failed active connection leaves incomplete writes indeterminate.
The same rules cover finite native exchanges. Explicit client Close
cancels background work, closes owned channels and joins refresh/DNS work.

## Acceptance

Default offline tests must cover arbitrary seed ResolveStore, direct-only business RPCs,
periodic peer convergence, same-group replicas, ownership conflicts, origin lease
expiry without resurrection, terminal withdrawal, mixed origin lease policies,
delayed responses after watermark collection, complete finite-handshake validation,
server-side exchange deadlines without a caller deadline, malformed atomic exchange rejection,
seed self-selection, lease expiry and lifecycle cleanup. Client tests must prove
business never reaches the seed, multiple targets receive independent RPCs, DNS
changes are observed while old connections remain healthy, invalid/expired
mappings fail, and writes are not replayed after an ambiguous transport failure.

Actual process/backend verification must initialize through a node that does not
own the requested Store, execute and independently verify persisted operations at
the owner, and exercise process replacement/replica changes. Tagged compilation,
local fixture execution and real Kubernetes deployment are distinct evidence.
