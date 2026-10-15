# Weir

Weir discovers logical Stores and combines concurrent client requests into finite
database batches. Clients initialize
through any Weir node with `ResolveStore`, then connect directly to the returned business
targets. Nodes synchronize their Store directory through bounded periodic peer
exchanges. Business requests execute only local Stores.

`Execute` is a finite bidirectional stream of typed requests and results. SDK
Read and Mutate calls send one record per request for one Store. The server
receives through a bounded ticket queue, combines compatible queued records into
database batches and publishes results in input order. Waiting queue capacity and
slow consumption apply backpressure. Same-resource mutations within a stream
execute in input order.
Native forwards MongoDB commands and Search HTTP requests without operation
allowlists. A mutation can request `MutationOptions.visibility=QUERY_VISIBLE`;
successful completion then confirms visibility to subsequent normal queries,
including Lua transforms. Each adapter implements that completion guarantee.
Lua runs inside the single Weir process. Unconfirmed writes remain indeterminate
after transport failure and are never automatically replayed.

See [architecture](docs/architecture.md), [discovery design](docs/discovery-design.md)
and [payload contracts](docs/payloads.md) for the current contracts.

The [value proof](docs/value-proof.md) links the executed Linux MongoDB/Search
benchmark: 94 paired comparisons, all rounds and retained raw evidence.
High-concurrency MongoDB requests benefit from aggregation; low concurrency,
Search pure reads and efficient native batches favor direct access. The report
includes throughput, tail latency, resource costs and grouping ablation.
The [single-record crawler follow-up](docs/value-proof.md#single-record-crawler-follow-up-2026-10-15)
retains rejected optimizations and does not establish a stable database CPU benefit
for that workload. Adoption should compare completed throughput and actual database
load using the application's submission and visibility requirements.

## Build and run

Requires Go **1.27.1**. Typed Read, Mutate and Scan require existing backend
collections/indices; Native can provision them. MongoDB program transforms
require a replica set with transactions.

```sh
go build -o bin/weir ./cmd/weir
cp config/weir.reference.yaml weir.yaml
cp config/routes.reference.yaml routes.yaml
```

Edit the copied files for your listener addresses, backend endpoints, credentials
and CA paths, then validate and start the node:

```sh
bin/weir check --config weir.yaml --routes routes.yaml
bin/weir check --config weir.yaml --routes routes.yaml --effective
bin/weir serve --config weir.yaml --routes routes.yaml
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
transport:
  timeouts:
    stall: "30s"
```

The local Store file:

```yaml
stores:
  - name: "mongo"
    backend:
      mongodb:
        uri: "mongodb://127.0.0.1:27028/?directConnection=true"
    batching:
      max_operations: 32
      max_in_flight: 0
      max_wait: "0s"
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

The [process reference](config/weir.reference.yaml) and [Store reference](config/routes.reference.yaml)
document each field's purpose, defaults, bounds and related settings. They are
templates to copy and edit, and are also included under `config/` in release archives.
Reference files are not automatically loaded. Both files use strict single-document YAML:
unknown/duplicate fields, anchors, aliases and explicit tags are rejected.
`weir check` validates configuration without connecting to
peers or backends. Configuration changes take effect after a restart.
`check --effective` shows resolved defaults and automatic sources with
authentication and trust paths redacted. The [configuration review](docs/configuration.md)
explains removed fields, retained controls and fixed protocol boundaries.

Store defaults are 32 operations and 8MiB of input per batch. `batching.max_wait`
defaults to `0s` for immediate dispatch. A positive duration collects compatible
RPCs until the oldest admission reaches that age; full batches, Scan/Native
commands and draining Stores dispatch immediately. A caller deadline before the
collection deadline also dispatches immediately. Collection uses the existing
bounded queue and adds no per-request timer. Queued RPCs combine by namespace, actual input bytes and
`batching.max_operations`. Large client batches split into sequential bounded groups.
Lua transforms also combine compatible queued requests: MongoDB batches reads and
writes in a short snapshot transaction, and Search batches real-time reads and
version-conditional writes. Weir adds no revision fields to business documents.
Adapters issue their native read and write
commands for each group. `batching.max_bytes` sets the aggregation window;
one larger input executes alone.
`queue.max_operations` (default 1024) and `queue.max_bytes` (default
32MiB) bound waiting work, with capacity returned at dispatch. Full queues apply
backpressure until capacity is available or the caller cancels. A larger input
can occupy an empty queue alone. `batching.max_in_flight` optionally bounds simultaneous
adapter invocations per Store; its default `0` leaves them unlimited. A positive
budget keeps excess requests in the bounded queue, allowing natural batching while
the backend is busy. Execution completion returns this credit before consumer Ack;
Scan publication releases it between pages. Search visibility waits and Native
streaming still occupy credit, so a small budget can reduce throughput when waiting
for refresh or a slow Native consumer. Select a budget with the actual workload;
separate Stores have independent budgets. Backend pools, RPCs and connections have
no separate concurrency cap. Backend protocols determine document sizes.

Store settings are grouped by responsibility: `backend` holds connection establishment
and MongoDB target caching, native exchange chunk sizes, and the shared
`authentication`/`tls` layout. MongoDB connection workers are under
`backend.mongodb.max_connecting`. MongoDB target caching belongs to
`backend.mongodb.metadata_cache_entries`. Store `queue`, `batching` and `scan` hold waiting
work, request aggregation and Scan publication batches. Process `transport` owns
Execute flow control and independent keepalive/progress settings; process `lua`
sets VM execution policy for every Store. Business work
has no default instruction budget or server-selection deadline; native exchanges
split large batches rather than rejecting their combined input.

Memory capacity is detected from physical host memory, a finite process
address-space limit, and visible container memory limits. Actual process and
container memory pressure controls overload admission; no configured budget or
startup working-set reservation can reject otherwise valid configuration.
Use container/OS memory limits for the hard boundary.

A MongoDB Lua transaction can contain records from several RPCs. A database write
failure rolls back that transaction; a confirmed abort can be retried within the
original deadline. An uncertain commit cannot start another write attempt. Lua
keep, reject and evaluation failures have individual results and do not enqueue
writes. Search retries only items with a confirmed version conflict. Physical
groups split generated write commands at the configured exchange or native BSON
boundary, after a confirmed transaction abort; the IO setting is not a Lua workspace cap;
clients cannot treat Mutate or several RPCs as one application transaction.

Business RPCs and backend execution inherit caller cancellation and deadlines.
A shared batch uses the latest caller deadline; an unbounded caller keeps that
batch unbounded, while individual plans retain their own contexts. Canceling
all callers stops shared backend work. Scan and Native use the same caller
lifetime. Connection setup, stream stalls and cleanup retain their lifecycle
timeouts. A sent write whose acknowledgement is lost remains `UNKNOWN` and is
never replayed automatically.

MongoDB and Search authentication can use explicit `username`/`password` fields
or `username_file`/`password_file`; each credential has exactly one source. Search
credentials belong under `backend.authentication`. Files resolve relative to the Store
document, contain UTF-8 text and permit one final LF/CRLF. URI userinfo is rejected. Authentication and
backend configuration stay local and are never distributed in the Store directory.

Application and peer listeners use plaintext gRPC on an isolated network. Peer
metadata is not authentication. Lua programs must be trusted: a fresh restricted
VM and optional stack/instruction limits do not impose a
hard allocation limit on arbitrary Lua objects.
Transforms use `function(current, incoming)` with ordinary Lua tables, precise
64-bit integers and a fixed operation timestamp; see the [Lua guide](docs/lua.md)
and [product example](examples/lua/product.lua).

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
Install the versioned SDK with `go get github.com/batchstream/weir-go@v0.15.0`.
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

The SDK [basic](https://github.com/batchstream/weir-go/tree/v0.15.0/examples/basic),
[native](https://github.com/batchstream/weir-go/tree/v0.15.0/examples/native) and
[scan](https://github.com/batchstream/weir-go/tree/v0.15.0/examples/scan) examples
initialize through a seed. Read and Mutate accept batches for one Store and use
one bidirectional Execute RPC, with indexed results in input order. There is no
whole-call byte or item-count limit; each wire request carries one record.
Resources are canonical relative paths. ReadStream and MutateStream accept an
incremental producer and consumer so callers can avoid retaining the full input
and output. The slice convenience methods accumulate results in client memory.
The SDK also provides ReadOne, Create, Put, Replace, Delete, AtomicTransform, Scan
and Native methods. It pins public protocol v0.13.0. Advanced fixed-owner callers
can use Dial and package-level business helpers.

The SDK validates slice inputs before sending them. The server validates each
request before executing it; an invalid later request does not undo earlier effects.
Mutate is not a transaction. Confirmed indexed results survive a later stream
failure; unconfirmed mutations may have applied and must not be retried
automatically. An APPLIED outcome that includes a Failure preserves application
evidence while reporting a business failure. A later stream error does not revoke
an already confirmed successful item.

Scan and Native each send one command on a bidirectional Execute RPC and half-close
its input before consuming the typed result events. Success requires a
valid terminal Event and final gRPC OK. A scan checkpoint additionally requires
the matching delivered document count. A later page can use another instance.
MongoDB scans paginate by ascending `_id` without a retained cursor; Search
continuations carry the backend PIT snapshot. Consumers must tolerate repeated
documents when restarting an interrupted page. Scan fetches up to 128 documents
per backend call by default and uses a 4 MiB publication window. A larger
individual document is published alone. It releases the
backend execution workspace before sending that batch, so a slow consumer does not
block other database work. Both backends learn a smaller fetch capacity from
large documents within each page.
Scan accepts a native filter and explicit INCLUDE/EXCLUDE Projection. Internal
MongoDB `_id` and Search PIT/sort metadata need not appear in the result. Search
publishes business `_source` documents, consistent with Read. See the
[payload contracts](docs/payloads.md) for projection, Lua and Native semantics.

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
connection demand. Adding Weir replicas does not increase the database's capacity.

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

The local Search fixture defaults to small write queues for overload tests.
Positive application and SDK batch tests require normal backend capacity: start
it with `scripts/search-local.sh start elasticsearch normal` (or `opensearch`).
The OpenSearch fixture uses 3.3 for native PIT `_shard_doc` traversal across shards.
Run backend congestion tests separately with the default `overload` profile;
stop the owned fixture before changing its capacity profile.

Monitor active executions, queue occupancy and per-record backend outcomes.
The queue exposes count and byte capacity metrics. `weir_store_active_executions_limit`
reports `batching.max_in_flight`, with `0` meaning unlimited. Active executions and
average operations per batch show whether a budget retains enough work to aggregate.
`weir_store_batch_max_wait_seconds` reports the configured collection wait.

Search exposes `weir_search_http_requests_total` and
`weir_search_http_request_duration_seconds` by fixed operation, outcome and
visibility labels. They cover Record/Scan exchanges and startup qualification;
Native relay is excluded. Bulk elapsed time includes `refresh=wait_for`, so it
does not isolate refresh waiting. `weir_search_http_request_records` reports
actual framed records per attempted mget/bulk request, including failures.
`weir_search_lua_evaluation_duration_seconds` measures in-process Lua evaluation
including its compile/conversion work, excluding external JSON decoding/encoding.
HTTP success does not imply every bulk item succeeded. Concurrent durations
overlap and must not be summed as wall time. Labels contain no target or document
identities.

Build reproducible archives from a clean commit with
`python3 scripts/package.py --output dist/local-build`.

## Releases

Publish stable versions through **Verified Weir images** on `main`, supplying a
canonical `vMAJOR.MINOR.PATCH` release version. The workflow runs default and race
tests, reproduces the Linux amd64/arm64 image and four platform archives, publishes
the verified image to GHCR, and confirms an anonymous pull before creating the
version image tag and GitHub release. Existing version tags and assets cannot be
replaced with different content.

Server archives support Linux and macOS on amd64 and arm64. Windows server builds
are unavailable because the current Lua dependency uses Unix process primitives.

Publishing a GitHub release directly also triggers this workflow automatically.
Its tag must point to a commit already merged into `main`; the workflow builds
that exact source and fills in verified release assets and the version image.
Use `ghcr.io/batchstream/weir:vMAJOR.MINOR.PATCH` or pin the image digest from
`delivery.json`. A release created by another workflow using `GITHUB_TOKEN`
must invoke this workflow explicitly, because GitHub suppresses subsequent
release events from that token. The supported manual workflow builds the image
before it creates the release.

## CLI

| Command | Purpose |
| --- | --- |
| `weir serve --config weir.yaml --routes routes.yaml` | Start discovery and configured local Stores from edited reference files. |
| `weir check --config weir.yaml --routes routes.yaml` | Validate process settings, Stores and credential files. |
| `weir check --config weir.yaml --routes routes.yaml --effective` | Inspect resolved policy and automatic sources with sensitive values redacted. |
| `weir version` | Print build identity. |
| `weir probe live` / `weir probe ready` | Check loopback diagnostics. |

`serve` and `check` accept `-c` for `--config`. `probe --address` selects a loopback
diagnostic address; `probe --timeout` controls its complete request deadline
(default `750ms`). The peer examples use their explicit local Store files:

```sh
weir serve --config examples/peer-a.yaml --routes examples/peer-a.routes.yaml
weir serve --config examples/peer-b.yaml --routes examples/peer-b.routes.yaml
```
