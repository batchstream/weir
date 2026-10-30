# Weir

Weir combines concurrent database requests into bounded batches. It supports
MongoDB, Elasticsearch and OpenSearch, with typed reads and mutations, Lua
transforms and native backend commands.

Clients connect to one Weir address and name the Store in each request. Every
server behind the address hosts the same Stores, so servers share nothing and
scale by adding replicas. Each server runs one process, including Lua evaluation.

## Why Weir

Fleets of workers, such as distributed crawlers, often write each result as its
own request, and every worker process keeps its own connection pool. Routed
through Weir, those writers share a small set of backend connections per Store,
and their concurrent single-record writes become native MongoDB `bulkWrite` and
Search `_bulk` calls.

On EKS, 16 client processes sent single 1 KiB upserts to a 2-vCPU database,
directly or through one Weir server on its own node (medians of three rounds,
MongoDB with `w:majority`):

| Concurrent writers | MongoDB ops/s, direct → Weir | Elasticsearch ops/s, direct → Weir |
| --: | --- | --- |
| 64 | 7.7k → 9.6k (1.2×) | 9.3k → 9.1k (0.97×) |
| 256 | 6.0k → 14.0k (2.3×) | 22.6k → 24.6k (1.1×) |
| 1,024 | 5.0k → 11.5k (2.3×) | 22.3k → 62.6k (2.8×) |
| 2,048 | 4.7k → 12.4k (2.7×) | 20.6k → 86.1k (4.2×) |

At 2,048 writers, direct writes held about 2,100 database connections against
about 20 for one Weir Store, and median latency was 3.7–5.2× lower. Database CPU
per write fell to 21–44% of the direct cost. Counting Weir's own CPU (about half
a core for MongoDB, three and a half at 86k Elasticsearch writes/s), total CPU
per write fell by about half for MongoDB; for Elasticsearch it was higher at 64
writers, even at 256 and 27–37% lower from 1,024. With few writers, throughput
stays close to direct writes and median latency can rise by up to 3 ms; the gain
grows with the number of concurrent writers. The workload tools live in
[weir-tests](https://github.com/batchstream/weir-tests).

## Build and run

Requires Go **1.27.1**. Server builds support Linux and macOS on amd64 and arm64.
Typed operations require existing collections or indices. MongoDB requires an
ordinary replica set and the native client `bulkWrite` command; Lua transforms
also require transactions. Native commands can provision backend resources.

```sh
go build -o bin/weir ./cmd/weir
cp config/weir.reference.yaml weir.yaml
```

Edit the copied file for your addresses, backends and authentication, then run:

```sh
bin/weir check --config weir.yaml
bin/weir serve --config weir.yaml
```

`--config` defaults to `./weir.yaml`. Configuration changes require a restart.
A minimal local setup:

```yaml
listeners:
  application: 127.0.0.1:7447
  diagnostics: 127.0.0.1:7449
stores:
  - name: mongo
    mongodb:
      uri: mongodb://127.0.0.1:27028/?directConnection=true
```

The [reference](config/weir.reference.yaml) describes every supported field, and
[Configuration](docs/configuration.md) explains loading, addresses and defaults.

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
go get github.com/batchstream/weir-go@v0.18.0
```

```go
// import weir "github.com/batchstream/weir-go"
client, err := weir.New("127.0.0.1:7447")
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

Read and Mutate publish each indexed result as its record completes; the
collecting calls return results in input order. Stream variants accept
incremental producers and consumers. Single-record Client calls share a bounded
set of long-lived streams, so concurrent callers in one process pay for few
gRPC streams. Same-resource mutations within a stream execute in input order;
Mutate is not an application transaction. Unconfirmed
writes after transport failure remain indeterminate and are never automatically
replayed. `QUERY_VISIBLE` requests completion visible to subsequent queries.
See [payload contracts](docs/payloads.md) for resources, mutation outcomes and
Native request formats, and the [Lua guide](docs/lua.md) for transforms.

## Batching and operation

Each Store defaults to a waiting queue of 1024 operations / 32 MiB, batches of
128 operations / 4 MiB, a 1 ms write collection window and 16 active executions.
Reads combine already queued compatible reads without waiting for new arrivals.
Full queues apply backpressure until dispatch or caller cancellation.

Static queue, batch, stream and 64 MiB message limits bound memory use. Native
commands bypass the Store queue and run in their own pool of 64 per Store, so
slow Native consumers never hold the queue space or slots of typed reads and
mutations. New work is refused while live memory is above 80% of the Go memory
limit, which the CLI sets to 90% of the container's cgroup v2 memory limit
unless `GOMEMLIMIT` is set. Container/OS limits provide the hard memory boundary.

The application listener uses plaintext gRPC on an isolated network. Backend
authentication stays local. In Kubernetes, expose the replicas through a headless
Service so the SDK spreads calls across every Pod; the
[weir-charts](https://github.com/batchstream/weir-charts) Helm chart is the
deployment reference.

| Guide | Contents |
| --- | --- |
| [Architecture](docs/architecture.md) | Ownership, execution, memory and shutdown |
| [Batching](docs/batching.md) | Queue, batch, wait and concurrency tuning |
| [Operations](docs/operations.md) | Health, metrics, memory pressure and failure handling |
| [Protocols](docs/protocols.md) | Public contract and module boundaries |

## Development

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags=integration ./...
```

Default tests need no backends.

### Integration tests

Integration tests use the `integration` build tag, Docker and these local
fixtures:

- `scripts/mongod.sh` downloads MongoDB 8.0.32 for Ubuntu 24.04 amd64 or macOS
  arm64 into the ignored `.tools/`, checks its SHA-256 and prints its path.
- `scripts/mongo-local.sh start` runs a single-member replica set from that
  `mongod` on `127.0.0.1:27028`. `WEIR_MONGOD` selects another binary and
  `WEIR_MONGO_BIND` adds comma-separated literal, non-wildcard IP bind
  addresses. A running fixture is reused only when its bind matches.
- `scripts/search-local.sh start elasticsearch|opensearch normal|overload` runs
  Elasticsearch 8.19.22 on `127.0.0.1:19200` or OpenSearch 3.3.0 on
  `127.0.0.1:19202` in Docker. Use `normal` capacity for ordinary runs; the
  default `overload` profile shrinks the write thread pool for the Search
  capacity-rejection tests. `start` waits until the cluster is green and the
  write thread pool has stayed idle for 5 seconds, so startup system-index
  writes do not compete with tests. Each fixture container runs a 1 GiB heap
  and needs about 2 GiB of Docker memory. Stop the fixture before changing
  profiles.

The TLS and replica-set tests start their own `mongod` processes from the same
binary. The HTTPS Search tests start their own 1.5 GiB container from the image
`scripts/search-local.sh` pulled, so the whole suite needs about 3.5 GiB of
Docker memory. `WEIR_SEARCH_PRODUCT` selects `elasticsearch` (the default) or
`opensearch`.
Run packages one at a time, because MongoDB fault injection uses a server-global
failpoint:

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch normal
go test -p=1 -tags=integration -timeout=15m -skip '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./...
scripts/search-local.sh stop elasticsearch
scripts/search-local.sh start elasticsearch overload
go test -tags=integration -run '^(TestSearchRealBackendCongestion|TestSearchExpressionRealCapacity)$' ./internal/backend/search
```

### Load and fault tests

These tests cross-compile the Linux CLI and run it in a CPU- and memory-limited
`busybox:1.37.0` container, reaching the MongoDB and Search fixtures above
through `host.docker.internal`. On Linux Docker Engine, also bind mongod to the
bridge gateway, as CI does:
`WEIR_MONGO_BIND=$(docker network inspect bridge --format '{{(index .IPAM.Config 0).Gateway}}')`.
Each has its own opt-in:

| Opt-in | Test |
| --- | --- |
| `WEIR_LOAD_INTEGRATION=1` | `TestProcessLoad`: fixed 200 RPC/s SDK traffic for a 30s warmup plus 600s (`WEIR_LOAD_MEASUREMENT` shortens it), with slow consumers and a burst; p99, resource-bound and recovery gates and an independent oracle |
| `WEIR_FAULT_INTEGRATION=1` | `TestProcessFaults`: lost acknowledgements, cancellation, slow IO, unavailability and SIGTERM through owned proxies, with no replay |

```sh
docker pull busybox:1.37.0
WEIR_LOAD_INTEGRATION=1 go test -tags=integration -count=1 -timeout=20m -run '^TestProcessLoad$' -v ./internal/app
```

Run the load and fault tests separately so injected faults do not distort load
measurements.

The [weir-tests](https://github.com/batchstream/weir-tests) repository contains
matched direct/Weir workloads and blackbox integration. Keep generated logs and
profiles under ignored `.testdata/`.

The public schema belongs to
[weir-protocol](https://github.com/batchstream/weir-protocol).

## Releases

Every push to `main` publishes the image `ghcr.io/batchstream/weir:<commit SHA>`
for Linux amd64 and arm64. Publishing a GitHub release `vMAJOR.MINOR.PATCH` also
publishes `ghcr.io/batchstream/weir:vMAJOR.MINOR.PATCH` and attaches Linux and
macOS amd64/arm64 archives with their `SHA256SUMS`. `weir version` reports the
source commit as `revision`. Build the image locally with:

```sh
docker buildx build --build-arg REVISION=$(git rev-parse HEAD) --load -t weir .
```
