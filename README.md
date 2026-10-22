# Weir

Weir combines concurrent database requests into bounded batches. It supports
MongoDB, Elasticsearch and OpenSearch, with typed reads and mutations, Lua
transforms and native backend commands.

Clients resolve logical Stores through any Weir node, then send business requests
directly to the owning nodes. Peers exchange an in-memory Store directory. Each
node runs one process, including Lua evaluation.

## Build and run

Requires Go **1.27.1**. Server builds support Linux and macOS on amd64 and arm64.
Typed operations require existing collections or indices. MongoDB requires an
ordinary replica set and the native client `bulkWrite` command; Lua transforms
also require transactions. Native commands can provision backend resources.

```sh
go build -o bin/weir ./cmd/weir
cp config/weir.reference.yaml weir.yaml
cp config/routes.reference.yaml routes.yaml
```

Edit the copied files for your addresses, backends and authentication, then run:

```sh
bin/weir check --config weir.yaml --routes routes.yaml
bin/weir check --config weir.yaml --routes routes.yaml --effective
bin/weir serve --config weir.yaml --routes routes.yaml
```

`--config` defaults to `./weir.yaml`. Omit `--routes` to run a directory node
without local Stores. Paths resolve from the working directory. Configuration
changes require a restart. The effective report shows defaults and automatic
sources with authentication and trust paths redacted.

A minimal local setup uses two files:

```yaml
# weir.yaml
listeners:
  application: 127.0.0.1:7447
diagnostics:
  address: 127.0.0.1:7449
```

```yaml
# routes.yaml
stores:
  - name: mongo
    backend:
      mongodb:
        uri: mongodb://127.0.0.1:27028/?directConnection=true
```

The [process reference](config/weir.reference.yaml) and
[Store reference](config/routes.reference.yaml) describe every supported field.
[Configuration](docs/configuration.md) explains file loading, addresses and policy.

The example intentionally uses isolated MongoDB **direct mode**, without failover.
For automatic primary discovery in an ordinary replica set, configure one to seven
explicit seeds and its set name instead, for example
`mongodb://mongo-a:27017,mongo-b:27017,mongo-c:27017/?replicaSet=orders`.
Replica-set clients use a one-second topology polling interval; direct mode retains
the driver default. Discovery restores new primary operations; it does not replay
uncertain writes or transparently migrate Native cursors, sessions or transactions.
See the
[MongoDB connection profiles](docs/configuration.md#store-settings) for strict URI,
authentication and per-member TLS requirements.

## Go client

The SDK is an independent module, [weir-go](https://github.com/batchstream/weir-go).

```sh
go get github.com/batchstream/weir-go@v0.16.0
```

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

request := &weir.ReadRequest{Resource: "database/collection/s:first"}
readOptions := weir.ReadOptions{
    StoreName: "mongo",
    Requests: []*weir.ReadRequest{request},
}
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

Read and Mutate publish indexed results in input order. Stream variants accept
incremental producers and consumers. Same-resource mutations within a stream
execute in input order; Mutate is not an application transaction. Unconfirmed
writes after transport failure remain indeterminate and are never automatically
replayed. `QUERY_VISIBLE` requests completion visible to subsequent queries.
See [payload contracts](docs/payloads.md) for resources, mutation outcomes and
Native request formats, and the [Lua guide](docs/lua.md) for transforms.

## Batching and operation

Each Store defaults to a waiting queue of 1024 operations / 32 MiB, batches of
128 operations / 4 MiB, a 20 ms write collection window and 16 active executions.
Reads combine already queued compatible reads without waiting for new arrivals.
Full queues apply backpressure until dispatch or caller cancellation.

The Node shares one execution-memory account across ingress, queued work, backend
processing, Lua and transport output. Actual process/container pressure controls
overload admission. Container/OS limits provide the hard memory boundary.

Application and peer listeners use plaintext gRPC on an isolated network. Backend
authentication stays local. Each instance advertises reachable peer and business
addresses; Kubernetes examples use a headless business Service per Store group
and a shared bootstrap Service.

| Guide | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Ownership, execution, memory and shutdown |
| [Discovery](docs/discovery-design.md) | Store directory, leases and direct client connections |
| [Batching](docs/batching.md) | Queue, batch, wait and concurrency tuning |
| [Operations](docs/operations.md) | Health, metrics, memory pressure and failure handling |
| [Protocols](docs/protocols.md) | Public and peer contracts and module boundaries |
| [Kubernetes scaling](deploy/kubernetes/scaling.md) | Replica and backend connection demand |

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags=integration ./...
python3 scripts/check_dependencies.py
python3 -m unittest discover -s scripts -p '*_test.py'
```

Default tests are offline. Prepare cached modules before using
`GOWORK=off GOPROXY=off` for offline checks. `scripts/bootstrap-tools.sh` installs
the pinned macOS arm64 toolchain and MongoDB into the ignored `.tools/`; CI
installs its Linux toolchain and modules with `scripts/ci-tools.py`.

### Integration tests

Integration tests use the `integration` build tag and local fixtures:

- `scripts/mongo-local.sh start` runs a single-member MongoDB 8.0.32 replica set
  on `127.0.0.1:27028` from `.tools/`; `WEIR_MONGOD` selects another `mongod`
  binary and `WEIR_MONGO_BIND` adds comma-separated literal, non-wildcard IP
  bind addresses. A running fixture is reused only when its bind matches.
- `scripts/search-local.sh start elasticsearch|opensearch normal|overload` runs
  Elasticsearch 8.19.22 on `127.0.0.1:19200` or OpenSearch 3.3.0 on
  `127.0.0.1:19202` in Docker. Use `normal` capacity for ordinary runs; the
  default `overload` profile shrinks the write thread pool for the Search
  capacity-rejection tests. Stop the fixture before changing profiles.

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch normal
WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -skip '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$'
```

`scripts/test-integration.sh` sets `WEIR_INTEGRATION=1` and serializes packages
because MongoDB fault injection uses a server-global failpoint. Search tests that
need no MongoDB run on their own, as in CI:

```sh
scripts/search-local.sh start opensearch normal
export WEIR_SEARCH_INTEGRATION=opensearch
go test -p=1 -tags=integration -skip '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./internal/backend/search
go test -tags=integration -run '^TestProcessSearchCRUD$' ./internal/app
scripts/search-local.sh stop opensearch
scripts/search-local.sh start opensearch overload
go test -tags=integration -run '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./internal/backend/search
```

### Replica-set tests

Replica-set discovery, failover, reply-loss and read-retirement tests start three
MongoDB members over plain and TLS connections from an explicit `mongod` 8.0.32
binary. CI downloads the pinned Linux binary with `scripts/ci-tools.py test --mongo`.

```sh
export WEIR_MONGO_REPLICA_INTEGRATION=1 WEIR_MONGO_REPLICA_BINARY=/absolute/path/to/mongod
go test -race -p=1 -parallel=1 -tags=integration -count=1 -timeout=10m \
  -run '^TestMongoReplica(DiscoveryOwnership|ProxyDiscoveryRecovery|CLIRecovery)$' \
  ./internal/backend/mongodb ./internal/app
go test -race -p=1 -parallel=1 -tags=integration -count=1 -timeout=20m \
  -run '^TestMongoReplicaCLIReplyLoss$' ./internal/app
```

### Load, fault and memory tests

These tests cross-compile the Linux CLI and run it in a CPU- and memory-limited
`busybox:1.37.0` container, reaching the MongoDB and Search fixtures above
through `host.docker.internal`. On Linux Docker Engine, also bind mongod to the
bridge gateway, as CI does:
`WEIR_MONGO_BIND=$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')`.
Each needs `WEIR_INTEGRATION=1`, `WEIR_SEARCH_INTEGRATION` when it uses Search,
and its own opt-in:

| Opt-in | Test |
| --- | --- |
| `WEIR_LOAD_INTEGRATION=1` | `TestProcessLoad`: fixed 200 RPC/s SDK traffic for a 30s warmup plus 600s (`WEIR_LOAD_MEASUREMENT` shortens it), with slow consumers and a burst; p99, resource-bound and recovery gates and an independent oracle |
| `WEIR_FAULT_INTEGRATION=1` | `TestProcessFaults`: lost acknowledgements, cancellation, slow IO, unavailability and SIGTERM through owned proxies, with no replay |
| `WEIR_MEMORY_RECOVERY_INTEGRATION=1` | `TestCLIBusinessHeapRecovery` |
| `WEIR_NATIVE_BURST_INTEGRATION=1` | `TestCLINativeSmallBurstPreservesAdmission` (Elasticsearch) |
| `WEIR_NATIVE_OUTPUT_BUDGET_INTEGRATION=1` | `TestCLILargeNativeOutputBudgetPreservesHealthyQueries` |
| `WEIR_TYPED_MEMORY_BUDGET_INTEGRATION=1` | `TestCLITypedMemoryBudget` (Elasticsearch) |
| `WEIR_INGRESS_MEMORY_INTEGRATION=1`, `WEIR_INGRESS_MEMORY_PARTIAL_INTEGRATION=1` | Ingress Put and partial-upload budgets |
| `WEIR_LUA_MEMORY_INTEGRATION=1`, `WEIR_LUA_MEMORY_PATHS_INTEGRATION=1` | Lua memory budget and allocation paths (paths use Elasticsearch) |

```sh
docker pull busybox:1.37.0
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch WEIR_LOAD_INTEGRATION=1 \
  go test -tags=integration -count=1 -timeout=20m -run '^TestProcessLoad$' -v ./internal/app
```

Run the load and fault tests separately so injected faults do not distort load
measurements. `WEIR_MEMORY_INTEGRATION=1 scripts/test-memory-linux.py` runs the
Linux memory guard tests in owned containers. TLS fixtures, packaged artifacts and
shared-memory pressure use the `WEIR_*` variables checked at the start of each
test.

The [weir-tests](https://github.com/batchstream/weir-tests) repository contains
matched direct/Weir workloads and blackbox integration. Keep generated logs,
profiles and experiment output under ignored `.testdata/`.

The local gRPC and Lua patches are required for allocation ownership. Their
upstream identities, patch diffs and focused regression tests live under
[third_party](third_party/README.md). Peer bindings are generated with
`scripts/generate.sh`; the public schema belongs to
[weir-protocol](https://github.com/batchstream/weir-protocol).

## Releases

Build reproducible Linux and macOS amd64/arm64 archives and their `SHA256SUMS`
from a clean commit (`--oci` also builds the Linux image locally). Packaged
binaries report that commit as `revision` in `weir version`:

```sh
python3 scripts/package.py --output dist/local-build
```

Run **Weir images** on `main` with a stable `vMAJOR.MINOR.PATCH` release
version. It tests the source, builds the archives and the Linux amd64/arm64
image, pushes the image to GHCR tagged with the source SHA, runs `weir version`
from the pulled image, then tags the version and creates the GitHub release with
the archives. Existing tags are never replaced with different content.
Publishing a GitHub release also runs the workflow; its tag must identify
source already merged into `main`.

Use `ghcr.io/batchstream/weir:vMAJOR.MINOR.PATCH`, or pin the image digest listed
in the release notes. A release created by another workflow with `GITHUB_TOKEN`
must invoke the image workflow explicitly. Windows server builds are unavailable
because the Lua dependency uses Unix process primitives.
