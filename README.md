# Weir

Weir discovers logical Stores and executes finite database batches. Clients initialize
through any Weir node with `ResolveStore`, then connect directly to the returned business
targets. Nodes synchronize their Store directory through bounded periodic peer
exchanges. Business `Execute` streams execute only local Stores.

Each finite bidirectional Execute RPC selects one Store and one instance. Record
reads/mutations, scans, native exchanges and atomic Lua transforms share the
bounded Store scheduler. Compatible operations batch across RPCs. Lua runs inside
the single Weir process. Uncompleted writes remain indeterminate after transport
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
memory: "1GiB"
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
    batch_collect: "5ms"
    max_read_size: "16KiB"
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
a `5ms` collection window and a `16KiB` ordinary-read limit. Tune against completed
throughput, backend CPU and tail latency. Memory is admission accounting; use an OS
or container limit for a hard memory boundary. The adaptive scheduler and backend
connection budgets remain independent of peer discovery.

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

## Client initialization and business calls

The high-level client resolves every requested Store before exposing business
methods. It reuses round-robin channels, refreshes directory mappings and DNS,
and drains retired connections without moving an active Execute to another instance.
Initialization accepts up to 16 Stores; each Store expands to at most 64 physical
addresses. Refresh runs at the earlier of the configured interval and one third of
the remaining ResolveStore TTL.

```go
options := weirclient.OpenOptions{
    Seed: "127.0.0.1:7447",
    Stores: []string{"mongo"},
}
client, err := weirclient.Open(ctx, options)
if err != nil {
    return err
}
defer client.Close()
// client.Record, client.Execute and client.ScanPage connect to the resolved Store.
```

The [basic](examples/basic/main.go), [native](examples/native/main.go) and
[scan](examples/scan/main.go) examples initialize through a seed. Low-level
`Dial` and finite `Execute`/`Record`/`ScanPage` helpers remain available for callers
that already hold a direct generated gRPC client.

An Execute succeeds only after every request's business terminal and end frame,
input half-close and final gRPC OK. A finite scan page returns a continuation
checkpoint or exhaustion; commit its checkpoint only after complete delivery.
A later page can use another instance. MongoDB scans paginate by ascending `_id`
without a retained cursor; Search continuations carry the backend PIT snapshot.
Consumers must tolerate repeated documents when restarting an interrupted page.

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
Build reproducible archives from a clean commit with
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
