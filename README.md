# Weir

Weir combines concurrent database requests into bounded batches. It supports
MongoDB, Elasticsearch and OpenSearch, with typed reads and mutations, Lua
transforms and native backend commands.

Clients resolve logical Stores through any Weir node, then send business requests
directly to the owning nodes. Peers exchange an in-memory Store directory. Each
node runs one process, including Lua evaluation.

## Build and run

Requires Go **1.27.1**. Server builds support Linux and macOS on amd64 and arm64.
Typed operations require existing collections or indices. MongoDB requires direct
replica-set access and the native client `bulkWrite` command; Lua transforms also
require transactions. Native commands can provision backend resources.

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

Default tests use offline or owned loopback fixtures. Prepare cached modules
before using `GOWORK=off GOPROXY=off` for offline checks. CI installs the pinned
toolchain and modules with `scripts/ci-tools.py`.

Live backend and process tests require explicit opt-in:

```sh
scripts/mongo-local.sh start
scripts/search-local.sh start elasticsearch normal
WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh
```

The integration runner serializes packages because MongoDB fault injection uses
a server-global failpoint. Use a separate Search `overload` fixture profile for
congestion tests; stop the owned fixture before changing profiles. Process and
memory qualification helpers retain their own explicit environment opt-ins.
Tagged compilation does not establish backend or process coverage.

The [weir-tests](https://github.com/batchstream/weir-tests) repository contains
matched direct/Weir workloads and blackbox integration. Keep generated logs,
profiles and experiment output under ignored `.testdata/`; documentation describes
current behavior. Historical experiments remain available in Git history.

The local gRPC and Lua patches are required for allocation ownership. Their
upstream identities, patch diffs and focused regression tests live under
[third_party](third_party/README.md). Peer bindings are generated with
`scripts/generate.sh`; the public schema belongs to
[weir-protocol](https://github.com/batchstream/weir-protocol).

## Releases

Build reproducible archives from a clean commit:

```sh
python3 scripts/package.py --output dist/local-build
```

Run **Verified Weir images** on `main` with a stable `vMAJOR.MINOR.PATCH` release
version. It tests the source, reproduces Linux amd64/arm64 images and four platform
archives, publishes verified content to GHCR and checks an anonymous pull before
creating version tags and release assets. Existing versions cannot be replaced
with different content. Publishing a GitHub release also triggers verification;
its tag must identify source already merged into `main`.

Use `ghcr.io/batchstream/weir:vMAJOR.MINOR.PATCH` or pin the image digest from
`delivery.json`. A release created by another workflow with `GITHUB_TOKEN` must
invoke the image workflow explicitly. Windows server builds are unavailable
because the Lua dependency uses Unix process primitives.
