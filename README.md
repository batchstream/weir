# Weir

Weir is a synchronous gRPC data plane for MongoDB, Elasticsearch and OpenSearch.
It provides record reads and mutations, duplex bulk operations, native backend
requests, scans and static peer forwarding. Each Store owns bounded admission,
micro-batching and adaptive database concurrency.

All operations use the same Store scheduler. Every record read and mutation can
join a bounded batch across RPCs; backend adapters merge compatible reads and
writes while preserving individual results. Native exchanges and Scan fetches
retain their own bounded stream state under that scheduler. Lua evaluation runs
inside the Weir process; MongoDB program commits remain independent transactions.

See the [architecture](https://github.com/batchstream/weir/blob/main/docs/architecture.md)
for protocol semantics and guarantees.
Use the [Go SDK](https://github.com/batchstream/weir-go) for clients and the
[Helm chart](https://github.com/batchstream/weir-charts) for Kubernetes deployments.

## Build and run

Requires Go **1.27.1** and a configured backend with pre-created collections or
indices. MongoDB program transforms require transactions on a replica set.

```sh
go build -o bin/weir ./cmd/weir
bin/weir version
bin/weir check --config deploy/docker/node.example.json
```

Copy the [basic configuration template](https://github.com/batchstream/weir/blob/main/deploy/docker/node.example.json)
and its [routing configuration](https://github.com/batchstream/weir/blob/main/deploy/docker/routes.example.json)
into the same directory. Set the backend address and resource names in the routing
file. Start with `bin/weir serve --config /path/to/node.json`;
`bin/weir serve` loads `weir.json` in the current directory.
Lua program transforms run inside the Weir process; no additional executable or
runtime path configuration is required.
The application listener uses plaintext gRPC; deploy it on an isolated network.
Lua programs must be trusted: each evaluation has a fresh restricted Lua state,
a 500ms execution deadline (including admission), bounded source and typed values,
and stack limits. At most four evaluations run concurrently in the process.
Lua allocation has no hard per-evaluation memory limit.

## Configuration

Configuration follows the separation between startup settings and routing used by
[Traefik](https://doc.traefik.io/traefik/getting-started/configuration-overview/).
The basic file groups application and peer addresses under `listeners`, diagnostic
HTTP settings under `diagnostics`, connection/session limits and readable timeouts
under `transport`, and the hop budget under `forwarding`. Its required
`routing.file` selects a separate file containing `services` and `routes`.

For example, `weir.json`:

```json
{
  "listeners": {"application": "127.0.0.1:7447"},
  "diagnostics": {"address": "127.0.0.1:7449"},
  "memory": "512MiB",
  "transport": {
    "max_connections": 16,
    "max_sessions": 16,
    "timeouts": {
      "unary": "30s",
      "bulk": "15m",
      "scan": "5m",
      "native": "5m",
      "stall": "30s"
    }
  },
  "forwarding": {"hop_limit": 4},
  "routing": {"file": "routes.json"}
}
```

And `routes.json`:

```json
{
  "services": [{
    "name": "database",
    "local": {
      "max_concurrency": 4,
      "max_batch_operations": 16,
      "mongodb": {
        "uri": "mongodb://127.0.0.1:27028/?directConnection=true",
        "database": "weir_m1",
        "collection": "records"
      }
    }
  }],
  "routes": [{"store": "mongo", "service": "database"}]
}
```

Relative routing paths resolve from the basic file's directory; absolute paths
are also supported. Configure at least one application or peer listener. Both
files are strict JSON objects: unknown fields, duplicate keys, trailing data and
files over 128 KiB are rejected. Weir validates both files and the complete route
graph before opening listeners or backend connections. Changes to either file
take effect after a restart.

Memory is a string containing an integer and `B`, `KiB`, `MiB` or `GiB`, such as
`"512MiB"` or `"1GiB"`; the allowed process budget is 64 MiB through 64 GiB.
Timeouts use duration strings such as `"500ms"`, `"30s"` and `"5m"`. Omitted
memory, transport and forwarding settings use the defaults shown above.
Diagnostics are enabled by setting an address. Memory is an admission budget;
use the deployment's memory limit to enforce an OS boundary.

The CLI uses Cobra commands:

| Command | Purpose |
| --- | --- |
| `weir serve --config weir.json` | Load both files and start the server. |
| `weir check --config weir.json` | Validate both files without backend or listener access. |
| `weir version` | Print build identity. |
| `weir probe live` / `weir probe ready` | Check loopback diagnostics. |
| `weir --help` | Show commands; each command also accepts `--help`. |

`serve` and `check` accept `-c` as the short form of `--config`; `probe` accepts
`--address` for a custom loopback diagnostic address. Backend, batching, listener
and memory settings belong in the configuration files. Every compared binary in
the load-comparison tool uses this same configuration layout.

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
- `cmd/weir/`: the single Weir server executable.
- `internal/`: implementation and test helpers.
- `examples/`: basic and native clients, plus peer configurations.
- `deploy/`: Docker assets, third-party licenses and Kubernetes manifests.
- `scripts/`: development, CI, release and opt-in local qualification tools.
- `docs/architecture.md`: architecture and protocol contract.
