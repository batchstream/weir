# Weir

Weir routes finite database batches through one bidirectional gRPC RPC, `Route`.
Each RPC fixes one logical Store. Forwarding nodes relay bounded opaque envelopes
to one downstream instance; the final node validates a versioned protobuf Call,
admits it to one bounded scheduler and safely batches compatible database work
across RPCs. Record reads/mutations, scans, native exchanges and atomic Lua
transforms use that same execution boundary. Lua runs in the Weir process.

This is a breaking protocol change. The former Read, Mutate, Bulk, Native and Scan
RPCs and their stream control frames are removed. The repository's
[`routeclient`](routeclient/client.go) runs synchronous finite batches with bounded
parallel sending and consumption; the [basic](examples/basic/main.go) and
[native](examples/native/main.go) examples show incremental use. Connections are
reused between batches. Client libraries and deployment configuration must use
the new protocol and `transport.timeouts.route` setting.

See [architecture](docs/architecture.md) for semantics and resource budgets,
[test coverage](docs/route-test-coverage.md) for acceptance mapping and
[Route verification](docs/route-validation.md) for current measured evidence.
Historical load reports describe the protocol and commit tested at that time.

## Build and run

Requires Go **1.27.1**. Local backend routes require pre-created collections or
indices. MongoDB program transforms require transactions on a replica set.

```sh
go build -o bin/weir ./cmd/weir
bin/weir version
bin/weir check --config config/weir.yaml --routes config/routes.yaml
```

Keep the [basic configuration](https://github.com/batchstream/weir/blob/main/config/weir.yaml)
and [routing configuration](https://github.com/batchstream/weir/blob/main/config/routes.yaml)
in the `config/` directory. Set backend addresses, resource names,
credentials and CA paths in the routing file, and remove services/routes you do
not need.
Start with `bin/weir serve --config config/weir.yaml --routes config/routes.yaml`;
`bin/weir serve` and `bin/weir check` default to `./weir.yaml` in the current
directory. `--routes` defaults to empty: no routing file is loaded and no backend
is assembled. Pass `--routes` to enable your routes. Both CLI paths resolve from
the working directory; neither file is automatically discovered elsewhere.
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
under `transport`, and the hop budget under `forwarding`. The optional `--routes`
argument selects a separate file containing `services` and `routes`.

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
memory: "1GiB"
transport:
  max_connections: 16
  max_sessions: 4
  timeouts:
    route: "15m"
    stall: "30s"
forwarding:
  hop_limit: 4
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
routes:
  - store: "mongo"
    service: "database"
```

Both `--config` and `--routes` resolve relative paths from the working directory;
absolute paths are also supported. An omitted `--routes`, an empty routing mapping
(`{}`), or two empty `services`/`routes` lists produces an empty graph. Both lists
can be omitted or null when empty. A partially populated graph is rejected.
Services contain `local` or `remote` settings in the routing file. MongoDB and
Search credentials can be configured as `username`/`password`
values or read from `username_file`/`password_file` paths. Each credential must
use only one source; inline and file sources can be mixed across the pair.
Each local service connects to one backend server. The Call resource selects the
MongoDB database and collection (`example/records/s:one`) or Search
index (`records/s:one`); these targets are not configuration fields.
Weir identifies Elasticsearch or OpenSearch during startup, without a configured
product profile or version allowlist. MongoDB connections also have no version
allowlist. Required server and resource capabilities are checked before use.
For example, a MongoDB service can use an inline username and a mounted password:

```yaml
services:
  - name: "database"
    local:
      mongodb:
        uri: "mongodb://mongo.example.invalid:27017/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true"
        username: "weir"
        password_file: "/run/secrets/mongo-password"
routes:
  - store: "mongo"
    service: "database"
```

Search supports the same credential sources inside `connection`:

```yaml
connection:
  username_file: "/run/secrets/search-username"
  password_file: "/run/secrets/search-password"
```

Credential files contain plain UTF-8 text. One final LF or CRLF is removed;
spaces are preserved. Usernames are limited to 128 bytes and passwords to 256
bytes after that removal; empty values and control characters are rejected.
File paths resolve from the routing file's directory, or can be absolute.
Weir reads ordinary files and follows symbolic links, so credentials mounted from
[Kubernetes Secrets](https://kubernetes.io/docs/concepts/configuration/secret/#using-secrets-as-files-from-a-pod)
can use this mechanism. Credentials are read by `check` and at startup; restart
Weir after changing a credential file. MongoDB URIs contain the endpoint and
connection options; configure credentials in the separate fields rather than URI
userinfo. Credentials are passed directly to the database client.

Configure at least one application or peer listener. Both YAML configuration files
are strict single-document YAML mappings with exact lowercase field names.
Unknown fields, duplicate keys, anchors, aliases, merge keys, explicit tags,
trailing documents and YAML files over 128 KiB are rejected. Weir validates both
YAML files, credential sources and the complete route graph before opening
listeners or backend connections.
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
| `weir serve --config config/weir.yaml --routes config/routes.yaml` | Load process settings and routes, then start the server. |
| `weir check --config config/weir.yaml --routes config/routes.yaml` | Validate settings, routes and credential files without backend or listener access. |
| `weir version` | Print build identity. |
| `weir probe live` / `weir probe ready` | Check loopback diagnostics. |
| `weir --help` | Show commands; each command also accepts `--help`. |

`serve` and `check` accept `-c` as the short form of `--config` and optional
`--routes` (default empty); `probe` accepts `--address` for a custom loopback
diagnostic address. Backend, batching, listener and memory settings belong in
the configuration files. Every compared binary in
the load-comparison tool uses this same configuration layout.

The peer examples require their routing files explicitly:

```sh
weir serve --config examples/peer-a.yaml --routes examples/peer-a.routes.yaml
weir serve --config examples/peer-b.yaml --routes examples/peer-b.routes.yaml
```

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
