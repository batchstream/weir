# Weir

Weir discovers logical Stores and combines concurrent client requests into finite
database batches. Clients initialize
through any Weir node with `ResolveStore`, then connect directly to the returned business
targets. Nodes synchronize their Store directory through bounded periodic peer
exchanges. Business requests execute only local Stores.

`Read` and `Mutate` carry a complete batch in one unary RPC. `Execute` carries one
Scan or Native command and streams its typed Events. All share the bounded Store
scheduler. Each client request completes its full preparation before admission.
Queued single-record requests and compatible small client batches share backend
executions while keeping their own ordered results and memory budgets. Same-URI
mutations within one request execute in input order. Lua runs inside the
single Weir process. Unconfirmed writes remain indeterminate after transport
failure and are never automatically replayed.

This is a breaking routing/configuration refactor. Configuration declares local
`stores` and peer discovery; there are no remote services, static remote route
mappings, forwarding budgets or business relays. URI affinity is not implemented.

See [architecture](docs/architecture.md), [discovery design](docs/discovery-design.md)
and [payload contracts](docs/route-payloads.md). See [current verification](docs/discovery-validation.md)
for direct discovery evidence. Historical load reports describe
the protocol and commit tested at that time.

## Build and run

Requires Go **1.27.1**. Backend collections/indices must already exist; MongoDB
program transforms require a replica set with transactions.

```sh
go build -o bin/weir ./cmd/weir
bin/weir check --config config/weir.yaml --routes config/routes.yaml
bin/weir serve --config config/weir.yaml --routes config/routes.yaml
```

The basic process configuration and optional local Store document are separate.
Both CLI paths resolve from the working directory. `--config` defaults to
`./weir.yaml`; an omitted `--routes` assembles no backend. Nodes without local
Stores can participate in discovery and answer ResolveStore for learned Stores.

A process file:

```yaml
listeners:
  application: "127.0.0.1:7447"
  peer: "127.0.0.1:7448"
discovery:
  group: "catalog"
  peer_address: "127.0.0.1:7448"
  seeds: ["127.0.0.1:7548"]
  advertise: ["127.0.0.1:7447"]
diagnostics:
  address: "127.0.0.1:7449"
memory: "2GiB"
transport:
  max_connections: 16
  max_sessions: 4
  timeouts:
    route: "15m"
    stall: "30s"
```

The local Store file:

```yaml
stores:
  - name: "mongo"
    mongodb:
      uri: "mongodb://127.0.0.1:27028/?directConnection=true"
    max_concurrency: 2
    max_batch_operations: 32
    max_read_size: "16KiB"
    backend_timeout: "2s"
```

`discovery.group` identifies the replica group providing those Stores.
`peer_address` is the node's reachable peer address; seeds are bootstrap peer
addresses, not remote Store mappings. `advertise` contains reachable business
IP/DNS addresses with explicit ports. Replicas may publish one shared multi-address
DNS name or their individual business addresses. Wildcard listener addresses need
explicit reachable advertisements. Concrete standalone listeners can advertise
their bound addresses automatically.
For dynamic instance addresses, `peer_address_env` can name one environment variable
containing the complete peer host:port; it is mutually exclusive with `peer_address`.
Only this explicit address source is expanded during configuration loading.

The [process reference](config/weir.yaml) and [Store reference](config/routes.yaml)
list the field bounds and defaults. Both files use strict single-document YAML:
unknown/duplicate fields, anchors, aliases, explicit tags and documents over
128 KiB are rejected. `weir check` validates configuration without connecting to
peers or backends. Configuration changes take effect after a restart.

Store defaults are two concurrent backend executions, 32 operations per batch,
a `16KiB` ordinary-read limit and a `384MiB` backend working budget. There is no
collection delay; queued RPCs combine by namespace, actual input bytes and
`max_batch_operations`. Large client batches split into sequential bounded groups.
Lua transforms also combine compatible queued requests: MongoDB batches reads and
writes in a short snapshot transaction, and Search batches real-time reads and
version-conditional writes. Weir adds no revision fields to business documents.
Adapters issue their native read and write
commands for each group. Read results reserve actual retained bytes, rather
than the configured maximum size multiplied by the number of records. Tune against completed
throughput, backend CPU and tail latency. Memory is admission accounting; use an OS
or container limit for a hard memory boundary. Execution concurrency and backend
working budgets remain independent of peer discovery.

A MongoDB Lua transaction can contain records from several RPCs. A database write
failure rolls back that transaction; a confirmed abort can be retried within the
original deadline. An uncertain commit cannot start another write attempt. Lua
keep, reject and evaluation failures have individual results and do not enqueue
writes. Search retries only items with a confirmed version conflict. Physical
groups can split further to bound retained read sources and generated writes;
clients cannot treat Mutate or several RPCs as one application transaction.

`backend_timeout` is a positive duration and defaults to `2s` when omitted.
Record execution gets one absolute deadline from dispatch through qualification,
reads and writes. A shared group uses the latest participating caller deadline,
capped by `backend_timeout`; an earlier caller stops waiting independently, and
canceling every caller stops the backend work. Queueing consumes the original RPC
lifetime. Set an explicit longer budget, such as `10s`, for that service's SLO.
Connected Search requests inherit this deadline for both headers and body;
connection setup has a separate `2s` bound. A sent write whose acknowledgement
times out remains `UNKNOWN`, with `DEADLINE_EXCEEDED` and a sanitized cause; it is
never replayed. Scan pages use the same backend budget. Search Native retains its
cumulative backend I/O budget, paused while publishing to the caller, so this
setting is not the entire streaming RPC's wall-clock lifetime.

MongoDB and Search authentication can use explicit `username`/`password` fields
or `username_file`/`password_file`; each credential has exactly one source. Search
credentials belong under `search.connection`. Files resolve relative to the Store
document, contain UTF-8 text and permit one final LF/CRLF. Usernames are limited to
128 bytes and passwords to 256 bytes. URI userinfo is rejected. Authentication and
backend configuration stay local and are never distributed in the Store directory.

Application and peer listeners use plaintext gRPC on an isolated network. Peer
metadata is not authentication. Lua programs must be trusted: a fresh restricted
VM, execution/source/value/stack limits and bounded concurrency do not impose a
hard allocation limit on arbitrary Lua objects.

The [public and peer protocols](docs/protocols.md) are independent schemas and
services. The public schema is suitable for future language-specific bindings.
The public contract and shared helpers live in
[weir-protocol](https://github.com/batchstream/weir-protocol), an independent
module consumed by both Weir and its SDK. The SDK does not depend on this server
module; server acceptance tests can depend on the SDK without a dependency cycle.

## Client initialization and business calls

The Go SDK lives in the independent [weir-go](https://github.com/batchstream/weir-go)
repository and module `github.com/batchstream/weir-go` (package `weir`). It resolves
every requested Store before exposing business methods. It reuses round-robin
channels, refreshes directory mappings and DNS,
and drains retired connections without moving an active RPC to another instance.
Install the versioned SDK with `go get github.com/batchstream/weir-go@v0.4.2`.
Initialization accepts up to 16 Stores; each Store expands to at most 64 physical
addresses. Refresh runs at the earlier of the configured interval and one third of
the remaining ResolveStore TTL.

```go
// import weir "github.com/batchstream/weir-go"
options := weir.OpenOptions{
    Seed: "127.0.0.1:7447",
    Stores: []string{"mongo"},
}
client, err := weir.Open(ctx, options)
if err != nil {
    return err
}
defer client.Close()
first := &weir.ReadRequest{Resource: "database/collection/s:first"}
second := &weir.ReadRequest{Resource: "database/collection/s:second"}
readOptions := weir.ReadOptions{StoreName: "mongo", Requests: []*weir.ReadRequest{first, second}}
results, err := client.Read(ctx, readOptions)
if err != nil {
    return err
}
for _, result := range results {
    if result.Failure != nil {
        return fmt.Errorf("read failed: %s", result.Failure.Message)
    }
    fmt.Println("missing:", result.Missing)
}
```

The SDK [basic](https://github.com/batchstream/weir-go/tree/v0.4.2/examples/basic),
[native](https://github.com/batchstream/weir-go/tree/v0.4.2/examples/native) and
[scan](https://github.com/batchstream/weir-go/tree/v0.4.2/examples/scan) examples
initialize through a seed. Read and Mutate accept batches for one Store, each with
one unary RPC and results in input order. Resources are canonical relative paths.
There is no item-count limit in the public API; the complete protobuf request and
response must each fit 32 MiB. The SDK also provides ReadOne, Create, Put, Replace,
Delete, AtomicTransform, Scan and Native methods. It pins public protocol v0.2.1.
Advanced fixed-owner callers can use Dial and package-level business helpers.

Batch requests are fully validated before backend work. Individual business
failures remain in their corresponding result. Mutate is not a transaction:
earlier successes remain applied if a later item fails. A transport or invalid
response error returns no confirmed batch results; all submitted mutations may
have applied and must not be retried automatically. An APPLIED outcome with a
later acknowledgement failure preserves application evidence without counting
as a successful operation.

Scan and Native each use one server-streaming Execute RPC. Success requires a
valid terminal Event and final gRPC OK. A scan checkpoint additionally requires
the matching delivered document count. A later page can use another instance.
MongoDB scans paginate by ascending `_id` without a retained cursor; Search
continuations carry the backend PIT snapshot. Consumers must tolerate repeated
documents when restarting an interrupted page.

## Deployment

The same discovery and client protocols support local processes, VMs, containers
and Kubernetes. Every instance publishes its own peer address and Store targets.
A seed can be an individual node, ordinary DNS name or load-balanced service.

In Kubernetes, each Store group can use its own Deployment, HPA and headless
business Service. A normal ClusterIP Service S1 selecting all Weir groups exposes
application ResolveStore and peer bootstrap ports. A client connects to S1, resolves
its Store to that group's headless DNS, then load-balances directly across the
group's Ready Pods. S1 need not be headless. Scaling changes the DNS instance set;
Store reassignment changes the directory mapping.

See [Kubernetes scaling](deploy/kubernetes/scaling.md) for aggregate backend
connection budgets. Adding Weir replicas does not increase the database's capacity.

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
python3 -m unittest discover -s scripts -p '*_test.py'
```

Default tests use owned offline/loopback fixtures. Live backend and process tests
are explicit opt-ins through `scripts/test-integration.sh`. Compile tagged helpers
and run real backend profiles separately; tagged compilation is not live coverage.
When several integration packages share one MongoDB fixture, run them sequentially
or pass `go test -p=1`: fault tests change the instance's global `failCommand`
failpoint, so concurrent packages can overwrite each other's faults and cleanup.
Matched direct/Weir benchmarks and current blackbox integration live in the
independent [weir-tests](https://github.com/batchstream/weir-tests) repository.
The locked first-reference `scripts/test-capacity.py` and historical Kubernetes
calibration require their original source/artifact receipts; they do not qualify
this refactor. Build reproducible archives from a clean commit with
`python3 scripts/package.py --output dist/local-build`.

## CLI

| Command | Purpose |
| --- | --- |
| `weir serve --config config/weir.yaml --routes config/routes.yaml` | Start discovery and configured local Stores. |
| `weir check --config config/weir.yaml --routes config/routes.yaml` | Validate process settings, Stores and credential files. |
| `weir version` | Print build identity. |
| `weir probe live` / `weir probe ready` | Check loopback diagnostics. |

`serve` and `check` accept `-c` for `--config`. `probe --address` selects a loopback
diagnostic address. The peer examples use their explicit local Store files:

```sh
weir serve --config examples/peer-a.yaml --routes examples/peer-a.routes.yaml
weir serve --config examples/peer-b.yaml --routes examples/peer-b.routes.yaml
```
