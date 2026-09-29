# Weir — bounded record data plane

A **synchronous MongoDB and Search data plane with static intranet peer forwarding**,
not complete V1 or a production release.
One Go module: `github.com/batchstream/weir`.

Implemented: gRPC Read / Mutate / duplex Bulk / Native / server-streaming Scan, independent local MongoDB and Search Stores,
opaque BSON / JSON, Put / Create / Replace / Delete, bounded admission/results/connections,
micro-batching, stream-local ordering, explicit AIMD and bounded shutdown. A static
RemoteWeir Service selects from 1–8 stable IP/DNS endpoints and forwards the same five RPCs over plaintext HTTP/2 without replay or failover;
forwarding-only nodes and two-hop chains are supported. Optional loopback health checks
and bounded Prometheus metrics expose lifecycle, reservations and execution evidence.

**AtomicTransform supports only explicitly qualified backend-expression profiles:**
`application/vnd.weir.mongodb-update.v1+bson` (`$set`/`$unset`/`$inc`) and
`application/vnd.weir.search-update.v1+json` (exactly one `doc` object), existing
records only, through unary Mutate and Bulk. Expressions are bounded to 16 KiB;
Search Update requires full stored source and no default/final pipeline.
See [milestone 7](docs/milestone-7.md) for exact bounds, native numeric semantics,
no-op outcomes, fixed versions and evidence.

**ProgramTransform remains UNSUPPORTED; the user approved deferring it from V1.** General programs remain a future architecture requirement; other production gates are unchanged. The GopherLua candidate failed isolation
qualification; its probes are test-only. MongoDB transaction RMW is a real, tested
internal foundation using a finite counter transform, not a public general runtime.
Dynamic configuration and queues remain out of scope. The companion
[Go SDK](https://github.com/batchstream/weir-go),
[Helm chart](https://github.com/batchstream/weir-charts) and versioned release
workflow provide the deployment path. See the
[production deployment guide](docs/production-deployment.md) for configuration,
network isolation, monitoring, upgrades and the required qualification evidence.
Publishing artifacts does not close the outstanding production qualification gates.

Qualification correction: the original `37d1454` implementation did not cover unary
response sending with its server deadline. The repair and new transport regression
evidence are recorded in `docs/unary-response-deadline.md`; the earlier overall
acceptance conclusion must not be used as evidence for that property.

## Local reproducible artifacts

With the pinned Go **1.27.1** and its verified module cache available, run from a
clean commit:

```sh
python3 scripts/package.py --output dist/local-build
```

This single entry point builds Linux/macOS/Windows × amd64/arm64 twice, using
separate clean exports and compilation caches. It compares each binary and archive,
records the actual source SHA and build settings, and emits SHA256SUMS plus separate
module-graph and linked-binary inventories. It neither downloads Go modules nor
contacts databases. `weir -version` prints identity before configuration or network
initialization; ordinary developer builds remain visibly `dev`.

Add `--oci --builder NAME` to use a locally prepared BuildKit builder and the pinned
public distroless base. OCI export stays local; no push, registry, signing or QEMU is
used. The packaging entry uses its own empty Docker client configuration; a container
builder's owned metadata can be selected with `BUILDX_CONFIG`. Exact tested builder,
commands, artifact source, digests, native scope and cleanup are in
[M15](docs/milestone-15.md). See [artifact instructions](packaging/README.md) and the
[configuration template](packaging/node.example.json). [M16](docs/milestone-16.md) adds
Go 1.27.1 / gRPC 1.83.2 artifacts, standard CycloneDX SBOMs, frozen-database scans
and explicit triage. See the [local supply-chain procedure](docs/supply-chain.md).
Remaining module matches concern packages absent from the CLI. M16 received limited
independent acceptance. [M17](docs/milestone-17.md) updates the exact Search profiles
and records new backend/image checks separately; OpenSearch bundled-component
security candidates still block its production qualification. These are unsigned
local artifacts, and the production gates remain open.

## Kubernetes

Use the [Helm chart](https://github.com/batchstream/weir-charts) for managed installs
and upgrades. Before startup, `weir -check-config /path/to/node.json` validates
configuration without opening listeners, reading CA files or connecting to backends.
For Prometheus inside an isolated cluster, explicit `diagnostics_allow_intranet: true`
allows a non-loopback diagnostic IP; restrict port 7449 to the monitoring workload
using enforced NetworkPolicy. Loopback remains the default.

The [minimal deployment](deploy/kubernetes/README.md) uses the same binary with
loopback exec startup/readiness/liveness probes, a ClusterIP Service, read-only
configuration and explicit resource/termination budgets. Readiness describes the
serving lifecycle, not backend health. [M20](docs/milestone-20.md) records the
limited single-replica qualification; three replicas, two workers and soak remain
separate gates.

## Local Run

The qualified test platform is macOS arm64, Go **1.27.1**, MongoDB **8.0.32**,
mongosh **2.6.0**. The application listener defaults to `127.0.0.1:7447`.
Explicit intranet IPs and wildcard IPs are accepted; access isolation belongs to
the deployment. Weir provides no authentication, authorization, or TLS/mTLS.
This profile is not suitable for an unisolated public network.
Use `PATH="$PWD/.tools/go1.27.1/bin:$PATH" GOENV=off GOTOOLCHAIN=local` after bootstrap.
The bootstrap downloads pinned public tools into ignored `.tools/`; it does not
change Homebrew or read environment/credential files.

MongoDB accepts one explicit `mongodb://host:port` endpoint, directly connected
to a non-sharded replica-set member. Both the credential-free intranet profile
and explicit SCRAM-SHA-256 with verified TLS use the same production adapter.
For the local unauthenticated fixture:

```text
mongodb://127.0.0.1:27028/?directConnection=true&serverMonitoringMode=poll
```

The authenticated profile requires a username, non-empty password,
`authMechanism=SCRAM-SHA-256`, `authSource`, and `tls=true`; optional `tlsCAFile`
supplies an exclusive CA root bundle (regular file, at most 256 KiB). Otherwise
Go uses system roots. Percent-encode URI credentials and CA paths; keep actual
configuration private. Configuration/CA changes take effect on restart.
The same `local.mongo.uri` JSON field accepts this shape (placeholder credentials):

```text
mongodb://APP_USER:APP_PASSWORD@mongo.internal:27017/?authMechanism=SCRAM-SHA-256&authSource=admin&tls=true&tlsCAFile=%2Fetc%2Fweir%2Fmongo-ca.pem
```

URI validation precedes DNS and CA access: 4096 bytes, one host with explicit
port, no database path, duplicate/unknown options, SRV, client certificates,
implicit mechanisms, insecure certificate options, or retry overrides.
`directConnection=true` and `serverMonitoringMode=poll` are the only optional
transport settings. TCP, verified TLS/OCSP, then bounded decrypted Mongo wire
inspection occur in that order, within a 2-second connect budget. Explicit
SCRAM never reauthenticates/replays a command in pinned driver v2.9.1.

OCSP retains the fixed driver's signed-response verification. This finite profile
adds limits: at most eight presented certificates, 64 KiB each and per staple;
at most one HTTP responder URL (2048 bytes), no redirects/proxies, 16 KiB HTTP
headers and 64 KiB body. Responder I/O errors are hard connection failures;
a complete, bounded inconclusive response retains the driver's soft-failure
semantics. A fresh one-leaf cache exists only during each handshake. See
[M10 remediation](docs/milestone-10-remediation.md) for exact evidence and limits;
this is not multi-node failover or overall production qualification.

```sh
scripts/bootstrap-tools.sh
scripts/mongo-local.sh start
go run ./cmd/weir
```

In another terminal:

```sh
go run ./cmd/weir-example
```

The example writes `_id: "example"` in the isolated `weir_m1.records` collection,
then verifies a three-operation Bulk and a bounded Scan filtered to that example.
The Scan consumer checks End, document count, Failure, and final gRPC OK. Search
Scan returns native JSON hits (including metadata), while Read returns `_source`;
a just-written Search document may not yet be visible before native refresh.
Use Ctrl-C to drain Weir, then `scripts/mongo-local.sh stop` to stop only the owned
test replica set. Data/logs remain in ignored `.testdata/mongo-m1`; start can reuse
that marked directory. Never enable test failpoints on a real database.

Optional flags: `-batch=false` makes physical batches singleton **without bypassing
admission**, `-database`, `-collection`, `-mongo-uri`, `-listen`, `-memory-mib`.
Default Store is `mongo`; default resource shape:
`weir://mongo/weir_m1/records/s:example`. Data must be BSON with explicit first `_id`
matching the URI. Supported keys: `s:`, `oid:` (24 lowercase hex), canonical `i:`.
No automatic creation of collections/indexes or hidden document fields.

## Local concurrency and shared databases

In each JSON Service's `local` object, set `"concurrency": 2` alongside `mongo`
or `search`. Omitted/zero means 4; integers 1–32 are accepted; negative and >32
values fail whole-graph validation before CA, DNS, database or listener I/O.
This is a static per-Local ceiling, changed by controlled restart. Mongo's business
pool and Search's ordinary pool both use that same value. For example:

```json
{
  "name": "database",
  "local": {
    "concurrency": 2,
    "mongo": {
      "uri": "mongodb://127.0.0.1:27028/?directConnection=true",
      "database": "weir_m1",
      "collection": "records"
    }
  }
}
```

Read, Mutate, Bulk batches, Native and Scan fetch share one Store scheduler.
The AIMD window starts at 1 and cannot grow past this ceiling. Native retains a
permit for its entire exchange, which also has the default **2-second physical
execution budget**; the 5-minute RPC lifetime does not extend it. Scan has one
separate 2-second cleanup reservation and reuses backend pools.

Sum **every Local adapter on every still-live executor**, including starting,
draining and terminating/replacement overlap. Two Local Stores targeting one DB
own two pools; forwarding-only nodes own no DB pool. Total accounting also includes
Mongo monitoring, Search Native, DNS and remote work after lost replies.
Mongo direct/poll owns at most **C+1 local dial/raw/closing slots**, from lookup
until raw Close completes (business pool C plus one monitor). Driver pool removal
does not release a slot. DNS finishes before serial TCP candidates; each resolving
slot has at most two DNS sockets. OCSP owns separate bounded responder I/O.
Search ordinary and Native connections share its existing C+1 owner.
For both backends, proxy/DB accepted sockets and work continuing after cancellation
have a separate remote tail: closing local TCP cannot force an immediate rollback
or remote close. Budget these under measured network/backend recovery assumptions.
The owner metrics and one-time close summary cover startup through termination.
See [M12R](docs/milestone-12-remediation.md) for layered formulas, actual process
measurements and the retained [M12 failure history](docs/milestone-12.md). This is
local ownership qualification, not a cluster quota, Kubernetes qualification or
throughput guarantee. Never replay an UNKNOWN write.

## Local Dual-Store Run

Keep the MongoDB fixture running. Search containers are pinned, loopback-only and
labeled as disposable test services; no host kernel settings are changed.
See `docs/milestone-2.md` for exact profiles and limitations (not a production setup).

```sh
scripts/search-local.sh start elasticsearch
# Operator setup on this isolated test node; Weir itself never creates indexes.
curl --fail -X PUT http://127.0.0.1:19200/weir_m2_example \
  -H 'Content-Type: application/json' \
  -d '{"settings":{"number_of_shards":1,"number_of_replicas":0}}'
go run ./cmd/weir -search-url http://127.0.0.1:19200 -search-index weir_m2_example
# Another terminal, against the same listener:
go run ./cmd/weir-example
go run ./cmd/weir-example -store search
```

## Search backend connections

Elasticsearch **8.19.22** and OpenSearch **2.19.6** accept one static HTTP or HTTPS
base URL with an explicit DNS hostname/IP and port. HTTP is credential-free.
HTTPS uses standard Go chain, SAN and expiry verification, with system roots or
an explicit CA bundle. Basic credentials are optional and allowed only on HTTPS.
Use the strict JSON configuration for this profile; for example, the `local.search`
object below contains placeholders, not working credentials:

```json
{
  "url": "https://search.internal:9200",
  "index": "records",
  "profile": "elasticsearch-8.19.22",
  "connection": {
    "username": "APP_USER",
    "password": "APP_PASSWORD",
    "ca_file": "/etc/weir/search-ca.pem"
  }
}
```

Omit `connection` for HTTP or HTTPS with system roots and no credentials. A CA-only
block is supported. Credentials must be paired (128/256-byte username/password
limits); CA files must be immutable regular files at most 256 KiB. Configuration
changes require restart. URL userinfo, paths, query, fragments, multiple endpoints,
proxy, redirects, insecure TLS overrides and caller-supplied authorization are
rejected or disabled. No credentials are loaded from environment or local files.
The entire app graph uses the adapter's pure validation before CA/DNS/network I/O.

DNS uses bounded standard Go A/AAAA I/O and picks the first returned address for
one connection attempt. A new connection resolves again; existing connections and
streams remain fixed. There is no discovery, sniffing, failover or business retry.
The ordinary pool follows `local.concurrency` (default 4); Native has a separate single fresh HTTP/1 connection.
For ordinary pool size P, the shared owner caps backend TCP sockets and concurrent
dial/handshake slots at P+1 (default 5). DNS adds at most two temporary sockets per
resolving slot, with a combined ceiling of 2(P+1) (default 10) while resolving.
All are canceled and joined on Close.

The existing single concrete index/primary, source and ingest restrictions remain.
Weir does not create accounts, certificates or indexes. The operator grants backend
qualification reads and permitted data actions; exact tested permissions, outcomes,
historical connection evidence is in [M11](docs/milestone-11.md); current versions,
commands and remaining security gates are in [M17](docs/milestone-17.md). Only
`elasticsearch-8.19.22` and `opensearch-2.19.6` are accepted. Old profiles and
unverified patches fail startup; there are no aliases or fallback. The disposable
Search fixtures use pinned linux/arm64 images and fresh data. Semantic regression
success does not establish overall backend or deployment safety.

Native HTTPS fixtures are isolated from the old HTTP containers and create their
own temporary CA, credentials and restricted application account:

```sh
GOPROXY=off GOSUMDB=off WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch \
  go test -race -tags integration -p 1 -count=1 -timeout=5m \
  ./internal/backend/search ./internal/app \
  -run 'TestSecureSearchQualification|TestSearchTLSApplicationAssemblyAllOperations' -v
# Repeat separately with WEIR_SEARCH_SECURE_INTEGRATION=opensearch.
```

Default tests never start databases. These fixtures require the pinned Docker
images already present, remove only their owned containers/materials/data, and
retain redacted logs under `.testdata/weir-m11-*`.

## Bounded Native

Native is an adapter-specific escape hatch with response-completeness evidence.
The initial matrix is MongoDB `count` / document `findAndModify`, and Search exact
GET / bounded streaming index-create-delete `_bulk`. Unknown operations are rejected.
Native and Scan share one live session per Store; Native holds its execution permit
through upload, backend I/O and response delivery. See `docs/milestone-4.md` for
exact descriptors, limits, no-replay audit and separate real-backend qualification.

```sh
go run ./cmd/weir-native-example                  # ordered BSON count
go run ./cmd/weir-native-example -store search    # native GET of example
```

The example sends and receives concurrently and checks Head, End and final gRPC OK.
A complete native database error is still RESPONSE_COMPLETE; incomplete responses
say nothing conclusive about whether a write applied. No automatic Native retry.

## Static Peers

Use `go run ./cmd/weir -config /absolute/path/node.json` for an immutable graph of
LocalStore and RemoteWeir Services. Both application and peer listeners use plaintext
HTTP/2 gRPC. The deployment must restrict peer ingress to intended Weir nodes:
hop metadata has no cryptographic identity guarantee.

Application ingress rejects any client-supplied `weir-remaining-forwards`; it creates
the configured initial budget (default 4, range 0–8). Peer ingress requires exactly
one canonical `0`–`8`, including on local requests. Every remote dispatch decrements
it; zero permits only local execution. Two listeners share process admission and
connection limits. Store/URI/operation checks, deadlines, resource budgets and
UNKNOWN/no-replay rules apply on every path.

Current two-node examples need no certificate tools or identity files:

```sh
scripts/mongo-local.sh start
# Terminal B:
go run ./cmd/weir -config "$PWD/examples/peer-b.json"
# Terminal A:
go run ./cmd/weir -config "$PWD/examples/peer-a.json"
# Client terminal:
go run ./cmd/weir-example -address 127.0.0.1:7447
go run ./cmd/weir-native-example -address 127.0.0.1:7447
```

Stop each Weir with Ctrl-C, then stop the owned MongoDB fixture. To add a second
hop, configure B with a RemoteWeir to C's peer address and move the LocalStore to C.
Each RemoteWeir configures `"endpoints": ["peer-a.example:7448", "peer-b.example:7448"]`
and **1–16 total relay slots shared by the Service**. IP literals and ordinary DNS
names require explicit ports. The old `endpoint` field is strictly rejected.
Canonical host:port is the stable identity; DNS returns at most eight A/AAAA
addresses per identity. Read/Mutate use the same URI rendezvous affinity;
Native/Scan use resource, Bulk uses request ID and stays pinned for the entire stream.
Select one READY endpoint before dispatch, or wait at most 2 s on one cold endpoint
within the original deadline. No application retry or endpoint failover loop.

DNS refreshes every 30 s (connection hints are rate-limited); each lookup is capped
at 2 s, with two concurrent lookups per Service. NXDOMAIN, timeout, empty or oversized
answers withdraw old addresses for new calls. Backoff is capped at 30 s. Existing
streams stay on their old connection until completion/deadline; there is no stream
migration. Each endpoint owns one ClientConn and at most two TCP sockets including
draining connections. See [the exact DNS and hash contract](docs/architecture.md#143-endpoint-selection-and-affinity)
and [M9 qualification](docs/milestone-9.md).
All nodes must agree on Store and backend profile. Forwarding
preserves opaque bytes and bounded request ID/traceparent, replacing outgoing metadata
instead of accumulating baggage.

Application/peer binds require explicit IP:port, including `0.0.0.0` or `[::]` when
needed in a deployment-isolated container. Examples and tests use only loopback.
Port 0 requests independent ephemeral listeners; fixed duplicate/overlapping binds
are rejected. Diagnostics remain optional and loopback-only. The static graph allows
1–16 Services/routes and rejects unknown/duplicate fields. Removed `identity`, `allow`
and `remote.server_name` fields fail configuration decoding; there is no disabled
mode or compatibility shim. `-config` remains exclusive with local flags.

See [milestone 8](docs/milestone-8.md) for plaintext transport qualification and
[milestone 9](docs/milestone-9.md) for static endpoint/DNS qualification and bounds.
[Milestone 5](docs/milestone-5.md) preserves historical mTLS qualification;
its certificate-based setup is superseded. Backend TLS and credentials remain
backend concerns; their validation and no-replay protections are unchanged.

## Optional Diagnostics

Diagnostics are disabled by default. Add `-diagnostics 127.0.0.1:7449` to the local
run, or `"diagnostics": "127.0.0.1:7449"` to the static JSON configuration.
Only explicit loopback IPs are accepted. Each Node has its own Prometheus registry.

```sh
curl --fail http://127.0.0.1:7449/livez
curl --fail http://127.0.0.1:7449/readyz
curl --fail http://127.0.0.1:7449/metrics
```

Liveness checks only the responding process; readiness means validated data listeners
have entered service and the node is neither draining nor failed. Queue saturation,
memory overload and a disconnected remote do not change readiness. No health request
contacts a database. Diagnostics remain available during bounded data drain, then close.
They have independent limits: four HTTP/1 connections, two handlers, one-second read
and write deadlines, bounded headers, no keep-alive, compression, body or query parameters.

[Milestone 6](docs/milestone-6.md) lists exact metric meanings, labels, the **historical 1932 series**
maximum, and tests. With permissions removed and backend ownership included, the current maximum is
**2043 series** (`41 + 41*listeners + 120*mongo_local + 118*search_local + 34*remote`).
Backend `connections_owned/peak/limit/acquired/released` measure local slot ownership;
Mongo additionally reports `dialing/closing`. The final `backend_connections_closed`
summary covers shutdown after diagnostics disappear. These are not remote DB
connection/work counts. Ledger bytes are reservations; the Go memory fallback is not RSS.
Local APPLIED and Native/Scan completion evidence never promise client delivery.

## Validate

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags integration ./...
python3 -m unittest discover -s scripts -p package_test.py -v

# Explicit isolated backend and real response-loss/failpoint qualification.
# Start scripts/mongo-local.sh first. No configurable production URI is accepted.
scripts/test-integration.sh -count=1 -v
scripts/test-integration.sh -race -count=1 -v
# Search tests run only for an explicitly selected local profile.
WEIR_SEARCH_INTEGRATION=elasticsearch scripts/test-integration.sh -race -count=1 -v
scripts/search-local.sh start opensearch
WEIR_SEARCH_INTEGRATION=opensearch scripts/test-integration.sh -race -count=1 -v
```

Default tests never contact MongoDB or Search backends. The local TCP connection-limit unit test does
not contact external services. The integration runner uses `-p 1` because MongoDB
failpoints are server-global, and each test creates/drops only its own unique
`weir_test_<pid>_<counter>` database. Opted-in tests fail, rather than silently skip,
if the isolated replica set is unavailable.

Generated bindings are checked in; no protoc is needed for ordinary builds.
To regenerate with the pinned compiler and plugins:

```sh
scripts/bootstrap-tools.sh
scripts/generate.sh
```

## Contracts And Evidence

- [Current code guide](docs/code-organization.md): actual package ownership, imports,
  request/remote/shutdown paths and explicit fixture usage.
- [Code organization review](docs/code-organization-review.md): migration mapping,
  behavior-preservation checks and real regression evidence. Historical reports
  retain their original source paths; the guide maps them to current locations.
- `docs/architecture.md` / `docs/architecture.zh-CN.md`: parallel target architecture
  and protocol contracts, not implementation status or qualification results.
- `docs/milestone-1.md`: exact versions/limits, executed tests, evidence, known
  limitations and reproduction details.
- `docs/milestone-2.md`: exact Elasticsearch/OpenSearch profiles, dual-Store examples,
  resource isolation and real-backend fault evidence.
- `docs/milestone-3.md`: Scan selectors, page/session budgets, integrity and cleanup,
  separate MongoDB/Elasticsearch/OpenSearch qualification and remaining limits.
- `docs/milestone-4.md`: bounded Native profiles, ownership, early response and no-replay evidence.
- `docs/milestone-5.md`: historical static peer mTLS qualification, superseded by M8.
- `docs/milestone-6.md`: optional bounded diagnostics, health semantics, exact metrics and process evidence.
- [Milestone 7](docs/milestone-7.md): native expression profiles and atomic-update evidence.
- [Milestone 8](docs/milestone-8.md): intranet plaintext peers, removed Weir identity/TLS and retained transport bounds.
- [Milestone 9](docs/milestone-9.md): bounded static endpoint sets, ordinary DNS, affinity, pinned streams and no replay.
- [Milestone 10](docs/milestone-10.md): historical failed connection qualification.
- [Milestone 10 remediation](docs/milestone-10-remediation.md): verified Mongo TLS before decrypted wire bounds, explicit SCRAM no replay, and finite lifecycle evidence.
- [Production readiness checklist](docs/production-readiness.md): platform/runtime/deployment/load gates and unverified blockers for the trusted-intranet scope.
- `api/weir/v1/weir.proto`: wire contract and Go client bindings.
- `internal/store`: single ledger, scheduler, result credits and AIMD.
- `internal/app` / `internal/overload`: process ownership, static assembly and shared overload guard.
- `internal/backend/mongodb`: concrete driver ownership, TLS/wire bounds, CRUD and codec;
  the counter transaction conformance harness is integration-test-only.
- `internal/backend/search`: qualified Search CRUD, native OCC, bounded HTTP/TLS/DNS and bulk evidence.
- `internal/server`: shared application/peer gRPC transport, bounded forwarding and completion framing.
- `internal/testutil`: owned Mongo/Search/DNS fixtures, metric assertions and repository asset lookup.
- `experiments/luaprobe`: test-only runtime feasibility evidence; ProgramTransform remains unsupported.

Missing mutation replies are **UNKNOWN**, not proof of non-application. Never blindly
replay them. Ordinary Delete of an absent record is APPLIED after acknowledgement.
Bulk preserves only same-key order in the same live stream; after UNKNOWN even a
successor cannot assume backend completion ordering. Unary response deadlines now
run at the HTTP/2 stream write layer, through DATA/trailers, not only the handler.
Unary input stalls are bounded too; progress may refresh only the stall budget,
never the original lifetime, and decoding ends input-stall accounting.
Bulk input/send watchdogs and connection-level write stalls can still close a
connection; other RPCs on that connection can also be truncated.

The pinned Go 1.27 HTTP/2 + gRPC ServeHTTP profile uses at most one maximum-frame
request-body read-ahead credit per admitted RPC. This is required because the
ServeHTTP adapter otherwise drains request bodies ahead of application Recv.
