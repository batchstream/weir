# Weir — first verifiable milestone

A **local-only, synchronous MongoDB data plane**, not complete V1 or a production release.
One Go module: `github.com/batchstream/weir`.

Implemented: gRPC Read / Mutate / duplex Bulk, one Store / pre-created collection,
raw BSON, Put / Create / Replace / Delete, bounded admission/results/connections,
micro-batching, stream-local ordering, explicit AIMD and bounded shutdown.

**AtomicTransform returns UNSUPPORTED.** The GopherLua candidate failed isolation
qualification; its probes are test-only. MongoDB transaction RMW is a real, tested
internal foundation using a finite counter transform, not a public general runtime.
Native, Scan, search backends, peers, TLS/auth, dynamic configuration, SDKs, queues,
production deployment and releases are out of scope.

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
then sends and receives a three-operation Bulk concurrently and verifies End/counts.
Use Ctrl-C to drain Weir, then `scripts/mongo-local.sh stop` to stop only the owned
test replica set. Data/logs remain in ignored `.testdata/mongo-m1`; start can reuse
that marked directory. Never enable test failpoints on a real database.

Optional flags: `-batch=false` makes physical batches singleton **without bypassing
admission**, `-database`, `-collection`, `-mongo-uri`, `-listen`, `-memory-mib`.
Default Store is `mongo`; default resource shape:
`weir://mongo/weir_m1/records/s:example`. Data must be BSON with explicit first `_id`
matching the URI. Supported keys: `s:`, `oid:` (24 lowercase hex), canonical `i:`.
No automatic creation of collections/indexes or hidden document fields.

## Validate

```sh
go test ./...
go test -race ./...
go vet ./...

# Explicit isolated backend and real response-loss/failpoint qualification.
# Start scripts/mongo-local.sh first. No configurable production URI is accepted.
scripts/test-integration.sh -count=1 -v
scripts/test-integration.sh -race -count=1 -v
```

Default tests never contact MongoDB. The local TCP connection-limit unit test does
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

- `docs/architecture.md` / `docs/architecture.zh-CN.md`: parallel design and the
  explicitly narrowed milestone profile. Future V1 sections are **not implemented**.
- `docs/milestone-1.md`: exact versions/limits, executed tests, evidence, known
  limitations and reproduction details.
- `api/weir/v1/weir.proto`: wire contract and Go client bindings.
- `internal/store`: single ledger, scheduler, result credits, AIMD and overload guard.
- `internal/mongostore`: concrete driver ownership, CRUD, codec and transaction state machine.
- `internal/server`: bounded loopback gRPC transport and Bulk completion framing.

Missing mutation replies are **UNKNOWN**, not proof of non-application. Never blindly
replay them. Ordinary Delete of an absent record is APPLIED after acknowledgement.
Bulk preserves only same-key order in the same live stream; after UNKNOWN even a
successor cannot assume backend completion ordering. A send/input stall closes the
single offending connection; other RPCs on that connection can also be truncated.
