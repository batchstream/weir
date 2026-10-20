# Weir

Weir combines concurrent database requests into bounded batches. It supports
MongoDB, Elasticsearch and OpenSearch, with typed reads and mutations, Lua
transforms and native backend commands.

Clients resolve logical Stores through any Weir node, then send business requests
directly to the owning nodes. Peers exchange an in-memory Store directory. Each
node runs one process, including Lua evaluation.

## Build and run

Requires Go **1.27.1**. Server builds support Linux and macOS on amd64 and arm64.
Typed operations require existing collections or indices. MongoDB requires an
ordinary replica set and the native client `bulkWrite` command; Lua transforms
also require transactions. Native commands can provision backend resources.

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

Focused replica-set discovery/recovery tests instead own three new foreground
MongoDB processes, without the fixed-address helpers above. Supply an explicit
checksum-pinned native MongoDB 8.0.32 binary, with no inherited legacy fixture
opt-ins or Docker environment; the test verifies its identity before starting:

```sh
WEIR_MONGO_REPLICA_INTEGRATION=1 WEIR_MONGO_REPLICA_BINARY=/absolute/path/to/mongod \
  go test -p=1 -parallel=1 -tags=integration -count=1 -timeout=10m \
  -run '^TestMongoReplica(DiscoveryOwnership|ProxyDiscoveryRecovery|CLIRecovery)$' \
  ./internal/backend/mongodb ./internal/app
```

These serialized plain/TLS cases check discovery, new-operation recovery and
lifecycle ownership, not sustained load or formal qualification. The dedicated Linux CI job prepares its verified native binary explicitly
with `scripts/ci-tools.py test --mongo`; tests never download or fall back to
another backend. Generated logs remain ignored and temporary data/secrets are
removed only after all owned children have been reaped.

`TestMongoReplicaProxyDiscoveryRecovery` additionally checks a test-only observer:
exactly three owned frontends rewrite only hello/isMaster member address fields;
the production adapter's complete discovered/selected address set and per-member
monitor/data observations must agree. Real member configuration and inter-member
traffic remain unchanged. Both TLS legs verify freshly generated member identities.
The observer supports pinned polling/uncompressed OP_QUERY/OP_REPLY and OP_MSG
(including client bulkWrite ops/nsInfo sequences); checksums, exhaust, compression,
unknown wire forms/flags and unmapped addresses fail closed. It preserves unknown
hello fields and ordinary business replies, with bounded metadata-only observations.
This TLS-terminated fixture does not replace direct-member TLS coverage.

Offline wire, bypass-oracle and actual loopback TLS rejection controls need no
MongoDB, CLI or fixture opt-in and are explicitly selected in normal/race CI:

```sh
CGO_ENABLED=0 go test -p=1 -parallel=1 -count=1 -run '^TestReplicaProxy' ./internal/testutil/testmongo
CGO_ENABLED=1 go test -race -p=1 -parallel=1 -count=1 -run '^TestReplicaProxy' ./internal/testutil/testmongo
```

The backend observer cases retain ordinary typed/Native/Lua success across primary
transitions and add stable held-reply checks using the real driver's discovered
and selected frontend plus checked-out session ownership before adapter close.
Native does not acquire an implicit session. Separate ordinary-CLI cases cover
all 12 typed/Native × stable/stepdown/one-primary-loss × plain/TLS combinations:

```sh
WEIR_MONGO_REPLICA_INTEGRATION=1 WEIR_MONGO_REPLICA_BINARY=/absolute/path/to/mongod \
  go test -race -p=1 -parallel=1 -tags=integration -count=1 -timeout=10m \
  -run '^TestMongoReplicaCLIReplyLoss$/^plain$' -v ./internal/app
WEIR_MONGO_REPLICA_INTEGRATION=1 WEIR_MONGO_REPLICA_BINARY=/absolute/path/to/mongod \
  go test -race -p=1 -parallel=1 -tags=integration -count=1 -timeout=10m \
  -run '^TestMongoReplicaCLIReplyLoss$/^tls$' -v ./internal/app
```

Four serial fixture/CLI/SDK lifetimes each retain the same PID and client across
all three cases. A real successful, explicit-majority increment reply is held;
an independent primary-majority read proves the effect before deliberate loss.
The drop is performed promptly after the action, before waiting for external
majority usability. Actual uncancelled callers receive typed `UNKNOWN` or Native
`RESPONSE_INCOMPLETE`; only immutable health reads are retried, and new-ID work
must complete within five seconds of independently observed majority usability.
Up to eight persistent namespace/record watches cover every frontend through
later work and actual CLI exit, counting both matched requests and completely
forwarded mutation items. Old effects and both counts must remain exactly one.
Inline and document-sequence encodings are parsed with namespace-index resolution;
ambiguous/malformed requests and partial forwarding fail closed. Observations
retain bounded facts, presence flags and digests, not raw command/auth bodies.

Native complete command and item errors remain `RESPONSE_COMPLETE` without a
forwarding failure; consumer bytes must hash to the actual observed BSON reply,
and independent state must stay unchanged. CLI evidence is observable wire
closure/forwarding, TLS legs, effects, receipts, gauges, same PID and actual Wait,
not private driver topology or session counts. Real relay/client joins and
marker-checked fixture cleanup follow exit code zero within the existing eight
second SIGTERM gate. Linux CI retains all three serial command logs on failure
and measures the combined envelope without increasing its 15-minute job limit.

The reply-loss cases alone do **not** qualify cursor/Ack ownership, Lua
ambiguous commit or topology/cancellation-specific rollback, cgroups, sustained
load, SLOs or production. Direct TLS
coverage remains separate. A proxy's pre-forward barrier is already after Weir
dispatch; `ok:1` alone is not majority/business evidence, and Native callers must
request majority explicitly.

The same three serialized native commands also cover eight live sent/unreceived
read lifecycle cases, without adding a fixture or CLI start. Backend
`TestMongoReplicaProxyDiscoveryRecovery/{plain,tls}/sent_read_retirement` uses
an unchanged real adapter/driver in `store.Runtime` with `store.DefaultLimits`:
ordinary cancellation must spend the caller-independent two-second cleanup
allowance and actually return its execution/session/raw sockets before adapter
close. Final retirement holds a real ready read's unacknowledged ticket and
publisher alongside a second active read, preserves the full five-second drain
and two-second retirement, and checks actual execution/publisher/adapter joins.
Two charged record tickets remain after close; real `Ticket.Ack` reduces ownership
from two to one and the owning `Session.Close` releases the final ticket/credit.

`TestMongoReplicaCLIReplyLoss/{plain,tls}/typed/sent_read_retirement` cancels a
real sent read and checks caller completion and product-credit recovery within
three seconds while both replies remain held, then independently establishes
primary-majority usability and same-PID/SDK immutable health plus new-ID business
success within five seconds. After an idle-zero recheck, a fresh original
12-second caller is proved active, pending and uncancelled immediately before
actual SIGTERM. The CLI must drain/retire and actually Wait with exit zero within
eight seconds, without releasing either reply. Native CLI lifetimes retain their
original idle-exit behavior. All twelve mutation cases, four complete Native
business-error controls and persistent old effect/no-replay checks remain through
actual CLI exit and whole relay join.

The existing backend and typed-CLI plain/TLS lifetimes also execute four stable
Lua cases (eight executions), before read retirement and with no extra fixture or
CLI start. A singleton snapshot reply is held only after an actual correlated
successful read of `(n=0, external=0, candidate=0)`. An independent majority writer
sets `(20,1,0)`, then the unchanged snapshot is delivered. The observer requires
an attempted replacement `(1,0,1)` and the real server's complete WriteConflict
112, preserving its actual command/item envelope and label facts. Both cases
hold the real abort reply: successful `ok:1` without write-concern error, or real
NoSuchTransaction 251 only after the write has completely settled.

While that reply is held there is exactly one find/write/abort and no commit or
fresh attempt, and the independent durable ledger remains `(20,1,0)`. Fully
forwarding the actual abort reply permits one fresh transaction on the original
session, a replacement `(21,1,1)`, one commit and `APPLIED` without failure.
Dropping the actual abort reply permits no rebuild or commit and must produce
`NOT_APPLIED` with `UNAVAILABLE`/rollback-unconfirmed. The latter no-effect claim
requires both **no commit ever sent** and independent durable candidate zero;
lost acknowledgement alone does not establish it. Namespace/ID binding precedes
session-digest/transaction correlation across all three fixed frontends. Actual
full writes, correlated reply receipts, full forwarding and deliberate drops are
counted separately from release requests. Fixed registration/attempt/event caps
and a test-only 20-second watchdog fail closed; they never synthesize replies.

The backend shares one real Runtime/delegate with the original read retirement.
An unchanged real Ticket memory delegate observes nonzero working reservations
at both held snapshots: the retry must reuse, not accumulate, the attempt window.
Mutable global, library and input sentinels in both attempted replacements prove
fresh state, with the same original observation time; source construction uses a
fresh VM/budget/input conversion, not a measured VM-pointer claim. Input/result
credit stays owned until real Ack/session close, then returns to zero without
forced GC or ledger changes. Each original 12-second caller contains all stable
Lua phases; independently established majority usability starts the unchanged
five-second same-Runtime or same-PID/SDK new-ID recovery gate. Old mutation and
new Lua count/effect oracles persist through actual Runtime.Close/CLI.Wait and
relay joins. The same four lifetimes additionally execute four stable original-commit cases
(eight executions), with no new fixture or CLI start. One real snapshot/find and
singleton replacement `(1,0,1)` receive a complete successful bulkWrite reply.
The first complete successful majority commit reply is held while an independent
Primary+majority read strictly proves durable `(1,0,1)`, **before** the actual
reply socket drop. Losing that reply once permits only original-session/transaction
commit confirmation: the genuine successful confirmation must be fully forwarded
before `APPLIED`/no failure. Losing every successful confirmation produces
`UNKNOWN`/`UNAVAILABLE` while the original 12-second caller is still live, with
exactly one durable effect. Neither case permits Lua rebuild, replacement replay,
a new transaction or abort. Physical commits are bounded from two through twice
the default adapter retry count (currently five); driver-internal commit retry and
stronger retry writeConcern are not mistaken for replacement replay.

The commit cases use the same actual Runtime/Ticket mirror, observe nonzero
working/input ownership at the held commit, join execution/publisher/session,
and retain positive result/input credit until real Ack. Zero is asserted after
Ack **before** Session.Close, then again afterward. Ordinary typed CLI/SDK cases
join the actual call and observe idle ownership before explicit watch completion.
Same-Runtime or same-PID/SDK new-ID recovery retains the five-second budget after
independent majority usability. Exact old wire identities, request/full-write/
reply/full-forward/socket-drop counts and independent durable ledgers persist
through recovery, actual Runtime.Close/CLI.Wait/owner zero and relay joins. There
is no fictitious Native Lua axis. These stable cases do **not** cover in-flight
Lua stepdown/member loss or Lua-specific cancellation/shutdown uncertainty;
overall production qualification remains false.

The separate bounded read observer watches only fixture `records` IDs across the
three frontends and captures immutable first-find session identity. Cleanup
matches the original session in the `killSessions` **target array**, not the
cleanup command's potentially different implicit session. Each phase requires
one matched request, one complete upstream write and one real correlated
successful reply received but zero forwarded; replay, owned-session getMore or
killCursors, malformed/ambiguous boundaries and partial writes fail closed. A real
find reply may have cursor zero: withholding the whole reply proves cursor state
is **unknown to Weir**, not that a nonzero remote cursor was allocated. The
40-second read-observer watchdog is test-only, records failure on expiry and
closes without forwarding. It does not extend the mutation observer's 20 seconds
or any product budget. Targeted observer tails are closed/joined only after
independent product-owner proof; backend cancellation additionally checks the
exact raw sockets and driver session baseline, while CLI uses actual process
Wait and the final raw-owner-zero marker, not private session-pool claims.
`^TestReplicaProxy` includes deterministic `TestReplicaProxyRead` identity,
forwarding, replay, correlation, watchdog-failure and targeted-tail-join controls
in both existing normal and race CI commands.

The native CI job retains its 15-minute job and 10-minute Go limits, pinned
tools and race/serial settings. These focused lifecycle tests do not establish
sustained-load, cgroup, SLO, EKS or immutable-release qualification.

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
