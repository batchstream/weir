# Weir

Weir is a synchronous gRPC data plane for MongoDB, Elasticsearch and OpenSearch.
It provides record reads and mutations, duplex bulk operations, native backend
requests, scans and static peer forwarding. Each Store owns bounded admission,
micro-batching and adaptive database concurrency.

See the [architecture](https://github.com/batchstream/weir/blob/main/docs/architecture.md)
for protocol semantics and guarantees.
Use the [Go SDK](https://github.com/batchstream/weir-go) for clients and the
[Helm chart](https://github.com/batchstream/weir-charts) for Kubernetes deployments.

## Build and run

Requires Go **1.27.1** and a configured backend with pre-created collections or
indices. MongoDB program transforms require transactions on a replica set.

```sh
go build -o bin/weir ./cmd/weir
go build -o bin/weir-lua-worker ./cmd/weir-lua-worker
bin/weir -version
bin/weir -check-config deploy/docker/node.example.json
```

Copy the [configuration template](https://github.com/batchstream/weir/blob/main/deploy/docker/node.example.json), set the
backend address and resource names, and point `lua_worker` to the worker executable
if using program transforms. Start with `bin/weir -config /path/to/node.json`.
The application listener uses plaintext gRPC; deploy it on an isolated network.
Lua programs must be trusted: workers have bounded input, output and execution
time, but no hard per-process memory limit.

## Development

```sh
go test ./...
go vet ./...
python3 -m unittest discover -s scripts -p '*_test.py'
```

Default tests run without live backends. Live integration tests are opt-in through
`scripts/test-integration.sh` and require locally prepared fixtures.
Build reproducible release archives from a clean commit with
`python3 scripts/package.py --output dist/local-build`.

## Layout

- `api/`: protocol definitions and generated Go types.
- `cmd/`: server and Lua worker executables.
- `internal/`: implementation and test helpers.
- `examples/`: basic and native clients, plus peer configurations.
- `deploy/`: Docker assets, third-party licenses and Kubernetes manifests.
- `scripts/`: development, release and qualification tools.
- `docs/architecture.md`: architecture and protocol contract.
