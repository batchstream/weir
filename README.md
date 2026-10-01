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
bin/weir check --config config/weir.yaml
```

Keep the [basic configuration](https://github.com/batchstream/weir/blob/main/config/weir.yaml)
and [routing configuration](https://github.com/batchstream/weir/blob/main/config/routes.yaml)
together in the `config/` directory. Set backend addresses, resource names,
credentials and CA paths in the routing file, and remove services/routes you do
not need.
Start with `bin/weir serve --config /path/to/config/weir.yaml`;
`bin/weir serve` and `bin/weir check` default to `./weir.yaml` in the current
directory. Pass `--config config/weir.yaml` to use the reference directory.
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

The [config/weir.yaml reference](https://github.com/batchstream/weir/blob/main/config/weir.yaml)
documents every basic field. The separate
[config/routes.yaml reference](https://github.com/batchstream/weir/blob/main/config/routes.yaml)
documents every routing field, including MongoDB, Search, HTTPS authentication
and peer forwarding. Both files contain actual YAML settings with comments for
each field's purpose, required status, default, allowed values and relationships.
All declared services and routes are active configuration.

For example, `config/weir.yaml`:

```yaml
listeners:
  application: "127.0.0.1:7447"
diagnostics:
  address: "127.0.0.1:7449"
memory: "512MiB"
transport:
  max_connections: 16
  max_sessions: 16
  timeouts:
    unary: "30s"
    bulk: "15m"
    scan: "5m"
    native: "5m"
    stall: "30s"
forwarding:
  hop_limit: 4
routing:
  file: "routes.yaml"
```

And `config/routes.yaml`:

```yaml
services:
  - name: "database"
    local:
      max_concurrency: 4
      max_batch_operations: 16
      mongodb:
        uri: "mongodb://127.0.0.1:27028/?directConnection=true"
        database: "weir_m1"
        collection: "records"
routes:
  - store: "mongo"
    service: "database"
```

Relative routing paths resolve from the basic file's directory; absolute paths
are also supported. A service can contain inline `local` or `remote` settings,
or a nonempty `file` path to load those settings from a separate YAML file.
For example, a routing file can refer to a file mounted into the container:

```yaml
services:
  - name: "database"
    file: "/etc/weir/services/database.yaml"
routes:
  - store: "mongo"
    service: "database"
```

The referenced `database.yaml` contains the service body:

```yaml
local:
  max_concurrency: 4
  max_batch_operations: 16
  mongodb:
    uri: "mongodb://127.0.0.1:27017/?directConnection=true"
    database: "example"
    collection: "records"
```

Service file paths resolve from the routing file's directory, or can be absolute.
Each file contains exactly one `local` or `remote` block, using the same fields
and defaults documented in `config/routes.yaml`. The service name and its routes
stay in the routing file. A nonempty `file` cannot be combined with inline
`local` or `remote`; referenced files cannot contain `name` or another `file`.
Weir reads ordinary files and follows symbolic links, so files mounted from
[Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)
can use this mechanism. Configuration is read at startup; restart Weir after
changing a service file.

Configure at least one application or peer listener. All configuration files
are strict single-document YAML mappings with exact lowercase field names.
Unknown fields, duplicate keys, anchors, aliases, merge keys, explicit tags,
trailing documents and files over 128 KiB are rejected. Weir validates all files
and the complete route graph before opening listeners or backend connections.
Changes take effect after a restart.

Memory is a string containing an integer and `B`, `KiB`, `MiB` or `GiB`, such as
`"512MiB"` or `"1GiB"`; the allowed process budget is 64 MiB through 64 GiB.
Timeouts use duration strings such as `"500ms"`, `"30s"` and `"5m"`. Omitted
memory, transport and forwarding settings use the defaults shown above.
Diagnostics are enabled by setting an address. Memory is an admission budget;
use the deployment's memory limit to enforce an OS boundary.

The CLI uses Cobra commands:

| Command | Purpose |
| --- | --- |
| `weir serve --config config/weir.yaml` | Load the configuration and start the server. |
| `weir check --config config/weir.yaml` | Validate all referenced files without backend or listener access. |
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
- `config/`: complete basic and routing configuration references.
- `examples/`: basic and native clients, plus peer configurations.
- `deploy/`: Docker assets, third-party licenses and Kubernetes manifests.
- `scripts/`: development, CI, release and opt-in local qualification tools.
- `docs/architecture.md`: architecture and protocol contract.
