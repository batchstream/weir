# Store discovery and direct business traffic

This breaking refactor separates Store discovery from business execution. The
public StoreService exposes ResolveStore and finite Execute. The private
PeerDiscoveryService exchanges owner announcements with SyncDirectory. All run in the single Weir
process. URI affinity and automatic business retries are intentionally absent.

The public and peer schemas are independent; see [protocol boundaries and names](protocols.md).

## Topology

Clients can initialize through any Weir application address. A normal load-balanced
seed service can select any group: every peer eventually learns the same Store
ownership directory. ResolveStore returns the provider group's reachable host:port
business targets, rather than a forwarding next hop. The client connects directly
to those targets. Execute only accepts a locally configured Store; there is no
remote execution destination or fallback forwarding path.

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
names, business endpoints, revision, remaining lease and withdrawal state. Backend
URIs, authentication and backend configuration are never synchronized.

Same-group live advertisements contribute the group's Store targets. Identical
shared DNS targets deduplicate; individual addresses union across replicas. If a
Store is advertised by different groups, ResolveStore returns an explicit ownership
conflict. Synchronization does not resolve conflicting ownership by arrival order.

An owner periodically advances its sequence and renews its own lease. Relays send
only remaining lifetime, with a fixed bounded transit allowance removed before
transmission. Receiving the same sequence cannot extend its deadline. Expired and
withdrawn versions retain bounded high-water marks long enough to reject delayed
state. Older gossip cannot renew a dead origin. New processes use new node IDs,
so a restart does not reset another incarnation's sequence.

## Bounded anti-entropy

A worker periodically push/pulls snapshots with known direct peers and bootstrap
seeds. Bootstrap seed connections are recreated to avoid forever selecting the
same Pod behind an ordinary service. Learning peer addresses joins initially
separate membership islands. The algorithm is periodic anti-entropy for this
small bounded directory; it does not implement a new general-purpose SWIM stack.

Controls use short deadlines, finite message/node/Store/target limits and separate
admission credits. Tables, high-water marks and outgoing work stay bounded. A
malformed or oversized exchange is rejected before partial state mutation. No
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
targets and DNS answer sets are validated and bounded before installation.

DNS updates add/remove physical channels without migrating or replaying an active
Execute. A drained/failed active connection leaves incomplete writes indeterminate.
The same rules cover finite native exchanges and scan pages. Explicit client Close
cancels background work, closes owned channels and joins refresh/DNS work.

## Acceptance

Default offline tests must cover arbitrary seed ResolveStore, direct-only Execute,
periodic peer convergence, same-group replicas, ownership conflicts, origin lease
expiry without resurrection, withdrawal, malformed atomic exchange rejection,
seed self-selection, bounded state and lifecycle cleanup. Client tests must prove
business never reaches the seed, multiple targets receive independent RPCs, DNS
changes are observed while old connections remain healthy, invalid/expired
mappings fail, and writes are not replayed after an ambiguous transport failure.

Actual process/backend verification must initialize through a node that does not
own the requested Store, execute and independently verify persisted operations at
the owner, and exercise process replacement/replica changes. Tagged compilation,
local fixture execution and real Kubernetes deployment are distinct evidence.
