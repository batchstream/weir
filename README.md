# Weir — bounded record data plane

A **synchronous MongoDB and Search data plane with static intranet peer forwarding**,
not complete V1 or a production release.
One Go module: `github.com/batchstream/weir`.

Implemented: gRPC Read / Mutate / duplex Bulk / Native / server-streaming Scan, independent local MongoDB and Search Stores,
opaque BSON / JSON, Put / Create / Replace / Delete, bounded admission/results/connections,
micro-batching, stream-local ordering, explicit AIMD and bounded shutdown. A fixed
RemoteWeir Service forwards the same five RPCs over plaintext HTTP/2 without replay or failover;
forwarding-only nodes and two-hop chains are supported. Optional loopback health checks
and bounded Prometheus metrics expose lifecycle, reservations and execution evidence.

**AtomicTransform supports only explicitly qualified backend-expression profiles:**
`application/vnd.weir.mongodb-update.v1+bson` (`$set`/`$unset`/`$inc`) and
`application/vnd.weir.search-update.v1+json` (exactly one `doc` object), existing
records only, through unary Mutate and Bulk. Expressions are bounded to 16 KiB;
Search Update requires full stored source and no default/final pipeline.
See [milestone 7](docs/milestone-7.md) for exact bounds, native numeric semantics,
no-op outcomes, fixed versions and evidence.

**ProgramTransform remains UNSUPPORTED.** The GopherLua candidate failed isolation
qualification; its probes are test-only. MongoDB transaction RMW is a real, tested
internal foundation using a finite counter transform, not a public general runtime.
Dynamic configuration, SDKs, queues,
production deployment and releases are out of scope.

Qualification correction: the original `37d1454` implementation did not cover unary
response sending with its server deadline. The repair and new transport regression
evidence are recorded in `docs/unary-response-deadline.md`; the earlier overall
acceptance conclusion must not be used as evidence for that property.

## Local Run

The qualified test platform is macOS arm64, Go **1.27.0**, MongoDB **8.0.32**,
mongosh **2.6.0**. The application listener defaults to `127.0.0.1:7447`.
Explicit intranet IPs and wildcard IPs are accepted; access isolation belongs to
the deployment. Weir provides no authentication, authorization, or TLS/mTLS.
This profile is not suitable for an unisolated public network.
The bootstrap downloads pinned public tools into ignored `.tools/`; it does not
change Homebrew or read environment/credential files.

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
Each RemoteWeir has one fixed IP:port and 1–16 relay slots; no DNS/multiple-endpoint
selection or failover. All nodes must agree on Store and backend profile. Forwarding
preserves opaque bytes and bounded request ID/traceparent, replacing outgoing metadata
instead of accumulating baggage.

Application/peer binds require explicit IP:port, including `0.0.0.0` or `[::]` when
needed in a deployment-isolated container. Examples and tests use only loopback.
Port 0 requests independent ephemeral listeners; fixed duplicate/overlapping binds
are rejected. Diagnostics remain optional and loopback-only. The static graph allows
1–16 Services/routes and rejects unknown/duplicate fields. Removed `identity`, `allow`
and `remote.server_name` fields fail configuration decoding; there is no disabled
mode or compatibility shim. `-config` remains exclusive with local flags.

See [milestone 8](docs/milestone-8.md) for the current plaintext qualification and
bounds. [Milestone 5](docs/milestone-5.md) preserves historical mTLS qualification;
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
maximum, and tests. Removing the permission bucket makes the current maximum
**1931 series** (`41 + 41*listeners + 113*local + 34*remote`). Ledger bytes are reservations; the Go memory fallback is not RSS.
Local APPLIED and Native/Scan completion evidence never promise client delivery.

## Validate

```sh
go test ./...
go test -race ./...
go vet ./...
go vet -tags integration ./...

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
- [Production readiness checklist](docs/production-readiness.md): platform/runtime/deployment/load gates and unverified blockers for the trusted-intranet scope.
- `api/weir/v1/weir.proto`: wire contract and Go client bindings.
- `internal/store`: single ledger, scheduler, result credits and AIMD.
- `internal/app` / `internal/overload`: process ownership, static assembly and shared overload guard.
- `internal/mongostore`: concrete driver ownership, CRUD, codec and transaction state machine.
- `internal/searchstore`: qualified Search CRUD, native OCC, bounded HTTP and bulk evidence.
- `internal/server`: shared application/peer gRPC transport, bounded forwarding and completion framing.

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
