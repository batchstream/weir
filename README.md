# Weir — local record data plane

A **local-only, synchronous MongoDB and Search data plane**, not complete V1 or a production release.
One Go module: `github.com/batchstream/weir`.

Implemented: gRPC Read / Mutate / duplex Bulk / server-streaming Scan, independent local MongoDB and Search Stores,
opaque BSON / JSON, Put / Create / Replace / Delete, bounded admission/results/connections,
micro-batching, stream-local ordering, explicit AIMD and bounded shutdown.

**AtomicTransform returns UNSUPPORTED.** The GopherLua candidate failed isolation
qualification; its probes are test-only. MongoDB transaction RMW is a real, tested
internal foundation using a finite counter transform, not a public general runtime.
Native, peers, TLS/auth, dynamic configuration, SDKs, queues,
production deployment and releases are out of scope.

Qualification correction: the original `37d1454` implementation did not cover unary
response sending with its server deadline. The repair and new transport regression
evidence are recorded in `docs/unary-response-deadline.md`; the earlier overall
acceptance conclusion must not be used as evidence for that property.

## Local Run

The qualified test platform is macOS arm64, Go **1.27.0**, MongoDB **8.0.32**,
mongosh **2.6.0**. The listener accepts only explicit loopback IPs.
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

## Validate

```sh
go test ./...
go test -race ./...
go vet ./...

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
- `api/weir/v1/weir.proto`: wire contract and Go client bindings.
- `internal/store`: single ledger, scheduler, result credits, AIMD and overload guard.
- `internal/mongostore`: concrete driver ownership, CRUD, codec and transaction state machine.
- `internal/searchstore`: qualified Search CRUD, native OCC, bounded HTTP and bulk evidence.
- `internal/server`: bounded loopback gRPC transport and Bulk completion framing.

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
