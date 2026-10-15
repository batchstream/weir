# What justifies Weir?

Weir has measured value for independent concurrent small requests that can be
aggregated across callers. The benefit depends on the workload and actual database
load; native bulk operations and low-concurrency direct access remain stronger
alternatives in the tested cases. The performance objective is lower actual
database load at comparable completed throughput, or higher completed throughput
at comparable actual database load, using a representative workload.

## Single-record crawler follow-up, 2026-10-15

The [cross-index batching experiment](https://github.com/batchstream/weir-tests/blob/bad363af4f36e366c1a7e7a55b8a36a4b9f713c8/results/search-batching-2026-10-15/README.md)
and [metadata-only experiment](https://github.com/batchstream/weir-tests/blob/bad363af4f36e366c1a7e7a55b8a36a4b9f713c8/results/search-metadata-2026-10-15/README.md)
retain all 54 timed stages, immutable sources, raw hashes and independent audits.
Four client processes own 128 workers. Each native `_update` and Weir `Mutate`
call contains one record; the native caller receives no preassembled batch.
Business completion requires query-visible reconciliation, product mutation and
the independent parallel seller mutations. The offered rates are 16, 48 and 96
products/s, each with three paired rounds and the same database resources.

The cross-index candidate `b13318f` was rejected. Completed throughput regressed
by 3.6–12.3% at 48 offered products/s and 21.2–31.5% at 96. Larger batches did not
compensate for the observed latency and drain regressions. That implementation
was removed before merge.

The metadata-only candidate `7455013` retains the original index-specific batching
and fresh target checks. At 16 and 48, both Weir versions completed all arrivals
and passed all arrival gates, with similar throughput. CPU per completed product
reversed direction in the third round, so these measurements establish no stable
database CPU saving against baseline or native. At 96, every path was overloaded;
those rows do not establish sustainable capacity. Separate real-backend protocol
tests with 128 mapped fields reduced each metadata response from about 4.2 KB to
0.3–0.45 KB while preserving safety controls; that response-byte reduction is not
a throughput claim.

The separate [database-pressure follow-up](https://github.com/batchstream/weir-tests/blob/96a2e8ffc336ae11aea2e367741eb43c20e2275c/results/crawler-pressure-2026-10-15/README.md)
retains a prospectively selected 512-worker matrix across four client processes,
with 45, 57 and 68 offered products/s and three paired rounds per rate. All 27
formal stages passed the source, field and all-arrivals audits, with zero failed,
unknown, timed-out or rejected products. At 68, the metadata candidate reduced
database CPU per completed product against the original Weir by 42.6%, 11.2% and
6.6%, while paired completed throughput differed by less than 0.2%. At 45 and 57,
the CPU direction reversed between rounds. Against native single-record updates,
the candidate did not establish a stable CPU saving at any of the three rates.
None of the 27 formal stages satisfied the sustained database CPU gate, so the
short calibration's CPU pressure does not establish a formal database bottleneck
or a capacity gain. The retained 2048-worker calibration failed with timeouts,
unknown outcomes and field-audit failures; it is unqualified evidence. These
measurements support a limited metadata improvement over the previous Weir at
one tested rate, without establishing Weir's benefit for this crawler workload.

These crawler results must be assessed separately from the simpler fixed atomic
transform below. Its observed gain does not establish a gain for the crawler's
submission pattern and completion requirements.

## Sustained Linux benchmark, 2026-10-15

The independent [complete benchmark report](https://github.com/batchstream/weir-tests/blob/main/results/value-2026-10-15/README.md)
retains original JSON, commands, configuration, source/binary identities, logs,
cleanup receipts and offline analysis. It measures this server at
`8955cde10e574dae55d890b0c9c824fd39219d1c`, with Go 1.27.1, SDK v0.15.0,
protocol v0.13.0, MongoDB 8.0.32 and Elasticsearch 8.19.22.
Ten successful workflows produced 94 paired comparisons and 564 timed stages:
95,142,464 successful records, zero errors, timeouts or unknown outcomes, with
all independent final-state checks and owned-fixture cleanups passing.

At a 1-CPU database quota, these observed improvements exceed 10% in every
paired round. Rates use all successful records divided by all measured time.
Each path shares a four-logical-CPU runner; Weir uses additional unrestricted
service CPU, so equal database quota does not mean equal total CPU demand.

| Work | Workers | Native records/s | Weir records/s | Change | Native p95 / p99 ms | Weir p95 / p99 ms |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| MongoDB read | 256 | 9,468.6 | 14,050.8 | +48.4% | 73.727 / 90.111 | 30.719 / 40.959 |
| MongoDB 90/10 read/write | 256 | 4,876.2 | 9,743.8 | +99.8% | 106.495 / 180.223 | 49.151 / 73.727 |
| MongoDB write | 128 | 2,530.2 | 3,390.3 | +34.0% | 98.303 / 425.983 | 90.111 / 212.991 |
| Search fixed atomic transform | 256 | 1,628.6 | 3,526.0 | +116.5% | 196.607 / 229.375 | 131.071 / 180.223 |

Latency numbers are pooled histogram upper bounds, with approximation up to
12.5% + 1µs. Three paired rounds alternate path order; each path receives 10s
warmup and 20s timed work. Four independent client processes issue one record
per call against 8192 hot records with 1024-byte padding and disjoint writer
keys. The transform control uses one native atomic update computing
`revision = 1 - revision`, compared with the same computation in Weir Lua;
there is no unnecessary native client read or transaction.

A same-run grouping ablation supports the mechanism: at 128 MongoDB read workers,
grouping limit 1 gives 7,433.0 Weir versus 8,644.4 native records/s; limit 32 gives
15,667.8 versus 8,643.0. The native baseline is essentially unchanged, while
Weir's selected MongoDB commands per record fall from 1.0000 to 0.1251.
The configured maximum is 32, but the observed adapter average is 7.99 records.

The report also retains the negative evidence. Low-concurrency ordinary MongoDB
requests are slower through Weir. Search pure reads lose at every tested
concurrency. Native MongoDB bulk reads achieve about 102,000–104,000 records/s,
versus 19,000–29,000 through the SDK with 32 records per call. A 2-CPU database
sensitivity run removes several apparent gains, and some Search write rounds
vary substantially. Weir's CPU/RSS, client samples and database resource counters
are included; lower total CPU or production cost is not established. Only
MongoDB ordinary writes at 128 workers qualify on both paths for the primary
database saturation and successor plateau conditions. Other rates are observed
throughput within the tested ladder, not maximum-capacity claims.

See the [method](https://github.com/batchstream/weir-tests/blob/main/results/value-2026-10-15/METHOD.md)
and [complete round/latency/resource tables](https://github.com/batchstream/weir-tests/blob/main/results/value-2026-10-15/TABLES.md)
before applying these numbers to a deployment. Single-owner loopback measurements
with hot data do not establish cross-host, cold-data, conflicting-write or fleet
behavior. The finite local experiment below is separate historical mechanism
evidence; its timings are not combined with this sustained benchmark.

## Local MongoDB mechanism experiment, 2026-10-15

This local experiment **does not justify Weir for an application that can already
issue native database batches**. Direct native batches were much faster.
It also does not justify Weir on single-caller latency.

The executable test is
[`TestValueProofMongoReadPaths`](../internal/server/value_proof_integration_test.go).
It uses MongoDB 8.0.32, the real adapter, Store scheduler, gRPC transport and released
Go SDK. There is no mocked database, scheduler pause, collection timer, injected
database delay or assertion that Weir must win.

Each sample reads the same 512 distinct BSON documents, including a 256-byte
payload. Direct and Weir use separate MongoDB pools, both with retries disabled,
no maximum pool size and two simultaneous connection establishments. Both paths
pass through the same loopback MongoDB wire observer. The Weir path additionally
uses a real loopback gRPC listener and SDK discovery. One complete workload per
case warms pools at the tested concurrency and populates target metadata before
measurement. Three measured rounds alternate forward/reverse case order.

One worker has one outstanding request. Request size 1 therefore means independent
one-record Execute RPCs in the Weir path; 32 workers means at most 32 outstanding
single-record calls. Workers share one SDK client: this demonstrates independent
RPC aggregation, not throughput across independent operating-system processes.
Request size 32 means sixteen native `$in` commands in the
direct path or sixteen SDK Read calls with 32 individual stream records in the
Weir path. Request input reverses insertion order. Every returned document is
compared byte-for-byte with its original BSON, and SDK results must retain request
order. All results passed; no errors or missing documents were accepted as work.

The observer counts actual `find`, `getMore` and `listCollections` commands before
they reach MongoDB. These warm samples contained only `find`; no hidden metadata
or cursor continuation calls were omitted from the table. Connection handshakes
and pool warmup are outside the samples. Batch histograms come from each actual
command's `_id` selector, not a configured batch limit.

| Path | Records per client request | Concurrent requests | Backend calls, rounds 1 / 2 / 3 | Elapsed ms, rounds 1 / 2 / 3 | Median ms |
| --- | ---: | ---: | --- | --- | ---: |
| Direct driver | 1 | 1 | 512 / 512 / 512 | 42.420 / 41.037 / 40.659 | 41.037 |
| Weir SDK | 1 | 1 | 512 / 512 / 512 | 94.959 / 93.476 / 94.397 | 94.397 |
| Direct driver | 1 | 32 | 512 / 512 / 512 | 12.633 / 12.485 / 11.069 | 12.485 |
| Weir SDK | 1 | 32 | 267 / 304 / 259 | 18.282 / 22.659 / 16.111 | 18.282 |
| Direct driver | 1 | 128 | 512 / 512 / 512 | 14.132 / 15.735 / 13.412 | 14.132 |
| Weir SDK | 1 | 128 | 129 / 76 / 144 | 13.774 / 12.341 / 13.327 | 13.327 |
| Direct native batch | 32 | 1 | 16 / 16 / 16 | 2.749 / 3.142 / 2.601 | 2.749 |
| Weir SDK | 32 | 1 | 95 / 99 / 85 | 12.479 / 12.644 / 12.476 | 12.479 |

For example, the first Weir single-record round at concurrency 128 observed this
`records per find : number of finds` distribution:

```text
1:48 2:19 3:17 4:11 5:7 6:10 7:5 8:2 9:2 11:2 16:1
20:1 23:1 27:1 28:1 31:1
```

The histogram sums to 129 commands and 512 returned records. These are independent
single-record SDK calls, so their coalescing is actual cross-request batching.
Across the three rounds, concurrency 32 reduced database calls by 40.6–49.4%, and
concurrency 128 reduced them by 71.9–85.2%. Correct BSON and caller association were
preserved. **Fewer backend calls are proven; proportional CPU or cost savings are
not measured.**

The counterexamples matter:

- At concurrency 1, every record still needs its own backend command. Weir adds
  transport and scheduling work, and elapsed time is about 2.3 times direct here.
- At concurrency 32, Weir reduces command count but takes longer. Command reduction
  alone is not an end-to-end performance result.
- At concurrency 128, elapsed times are close. The modest median difference in
  this short, shared-process experiment does not establish maximum throughput.
- A 32-record SDK Read does not imply one 32-record backend command. Execute
  receives individual stream records, and the scheduler immediately dispatches
  available compatible work without waiting for the rest. Its 85–99 backend
  commands compare poorly with direct native batching's 16; the Weir median takes
  about 4.5 times as long. This is an explicit limit of the current design.

No backend saturation, p95/p99 latency, database CPU, total host CPU, RSS, network
egress, failover capacity or production monetary cost was measured. Client, Weir
and MongoDB share one Apple M2 host; client and Weir share the test process.
Concurrency and CPU scheduling can influence observed batch sizes. These finite
results support mechanism and counterexample claims, not a production capacity
number. The shared wire observer adds forwarding and per-command bookkeeping;
that extra fixed cost can favor fewer-command paths compared with uninstrumented
database connections. Search performance is not inferred from MongoDB.

## Correctness evidence alongside batching

The following existing tests were executed and passed on the measured source
snapshot recorded below, with the same owned MongoDB fixture.
They are separate from the timed read comparison; the table identifies where a
fixture replaces a component.

| Executed test | Actual evidence | Boundary |
| --- | --- | --- |
| `store.TestNativeCoalescedRecordsKeepLargeResultsAndRetainedOwners` | 32 prepared requests share one actual MongoDB `find`; duplicate ordinals do not mix caller results; more than 32 MiB of total result data is preserved and independently released | The scheduler is deliberately populated before dispatch and reads share one key. This proves the ideal queued mechanism, not naturally occurring batch size or network throughput |
| `server.TestRecordStreamPublishesFIFOAfterOutOfOrderBackendCompletion` | Real gRPC streams publish indexes 1 then 2 even when backend work completes in the opposite order | The adapter is an in-memory fixture, not a database |
| `server.TestRouteMongo2MiBRecordLuaScanAndPartialBatch` | Real MongoDB and gRPC preserve a 2 MiB BSON record; twelve same-resource Lua increments in one stream persist `n=12`; a rejected Create does not erase a peer's confirmed Put | Functional correctness, not Lua throughput or an application transaction across requests |
| `server.TestRouteMongoAppliedWriteAndNativeReplyLossAreNotReplayed` | A wire proxy drops a real database acknowledgement after it is received; typed writes report `UNKNOWN`, Native reports an incomplete response, actual backend state changes once and the wire command is not replayed | Controlled acknowledgement loss, not every network or replica-set failure |
| `mongodb.TestMongoMixedWriteLostReplyDoesNotReplayExpression` | A real mixed batch's acknowledgement is dropped; both results remain `UNKNOWN`; an independent database read sees `$inc` increment once and a peer Create persisted; exactly one `bulkWrite` was sent | Adapter-level real database evidence; no SDK in this particular test |
| `mongodb.TestMongoLuaConflictRetriesStopAtAttemptBound` | A real external writer conflicts with each Lua transaction; configured three attempts produce exactly three reads, writes and confirmed aborts, no commit, and a final `NOT_APPLIED` conflict; a canceled caller starts no additional attempt | Deliberately induced conflicts and custom retry settings; this proves the configured bound, not throughput or every failure mode |

UNKNOWN is necessary information, not successful exactly-once delivery. Once an
acknowledgement is lost, Weir cannot promise that the mutation was not applied.
Applications still need their own reconciliation or idempotency policy.
Same-resource ordering is per stream; there is no global cross-client ordering
or transaction guarantee. These limits must survive any optimization.

## The smallest defensible product

| Component | Decision | Reason |
| --- | --- | --- |
| Store queue, compatible batching and caller identity | Keep | Cross-caller aggregation is the mechanism measured here; per-caller result and cancellation ownership are required for correctness |
| MongoDB and Search adapters | Keep | They represent two real protocols with different batching, visibility and conditional-write semantics |
| In-process Lua | Keep only for applications needing this transform contract | A shared trusted transform can avoid application-side read/modify/write races; MongoDB transactions and Search OCC provide its backend safety. Its performance benefit is not proven by the read experiment |
| Stream flow control, bounded waiting queue and write-outcome evidence | Keep | These are required to make aggregation finite and safely report partial/uncertain work; they do not constitute an unlimited memory guarantee |
| Separate Lua executable, IPC or worker process | Remove / do not introduce | It adds a deployment and failure boundary with no need established here. Lua belongs in the one Weir process |
| Connection pool duplicated per application | Merge into shared Store only where deployment evidence supports it | Centralization can reduce aggregate client pools, but peak connections and failover behavior require actual fleet measurements |
| Store discovery | Keep only for the logical Store mapping requirement | It does not cause batching and is not a performance justification. If a deployment needs only one known backend endpoint, ordinary DNS plus a direct driver is simpler |
| Native forwarding | Keep only for a concrete compatibility requirement | It preserves native access for existing users; it does not create typed batching value and is not a reason by itself to add a network service |
| Another scheduling framework, global ordering layer or hypothetical backend | Remove / do not add | No requirement or evidence here pays for the extra state and coordination |

Starting from scratch, the minimum useful path is **client → one Weir Store queue
and scheduler → native database batch**, with adapters owning database semantics
and a single process owning Lua and lifecycle. Discovery and Native need their own
user requirements; adding them does not strengthen the batching proof.

## Adoption gate

Database CPU per completed record is the primary load measure here. Batching can
reduce fixed database command work, while qualification, pre-reads, conflict
retries and visibility barriers add database work or waiting. Moving transforms
outside the database can reduce its per-record computation. Measure their net
effect at the required completed throughput and latency; batch size alone cannot
establish a benefit. Different namespaces, conflicting keys, low overlap and
multiple Weir replicas can fragment the available batches.

Before adopting Weir for performance, require all of the following:

1. Real independent callers have overlapping compatible work and cannot already
   issue equivalent native batches cheaply. Measure actual batch distributions,
   not `batching.max_operations`.
2. A matched direct-driver baseline uses identical submission sizes, data,
   mutation semantics, durability, visibility, retry policy, concurrency and
   database resource limits. A native-batch control must explain how the actual
   callers obtain those batches; do not assume cross-process client aggregation.
   Verify outcomes and persisted mutation effects outside the timed phase.
3. Representative sustained load shows lower actual database load at comparable
   completed throughput, or higher verified completed throughput at comparable
   actual database load, while meeting the declared p95/p99 and drain limits.
   Equal database quotas alone do not establish equal observed load.
4. Both paths reach a demonstrated bottleneck and a higher-concurrency plateau
   before calling the comparison a capacity result. Use multiple runs and an
   explicit gain threshold agreed for the deployment; reject gains inside noise.
5. Cancellation, overload, partial results and lost acknowledgements still meet
   the correctness contract. A result-count or throughput improvement cannot
   compensate for replaying an uncertain mutation.

The sustained benchmark above demonstrates useful aggregation under specific
high-concurrency conditions and retains direct-access counterexamples. It
supports Weir as a **shared batching and transform service for fragmented
concurrent callers**, conditional on the deployment's matched workload, actual
database load, completed throughput and latency requirements. If the adoption gate
fails and no shared transform/discovery contract is needed, use the direct driver
and remove the extra service.

## Reproduce and audit the local mechanism experiment

This measurement used Go 1.27.1 on Darwin/arm64, Apple M2 (8 logical CPUs),
24 GiB host RAM, MongoDB 8.0.32 single-member `weir_local` replica set on
`127.0.0.1:27028`, no authentication, SDK v0.15.0, protocol v0.13.0 and MongoDB
driver v2.9.1. There was no CPU quota or separate CPU allocation. The fixture
creates a unique `weir_test_<pid>_<sequence>` database and drops only that database
at cleanup. It verifies the replica-set identity before creating data. No
credentials or production endpoints are used.

```sh
scripts/mongo-local.sh start
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/server \
  -run '^TestValueProofMongoReadPaths$' -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -race -tags integration ./internal/server \
  -run '^TestValueProofMongoReadPaths$' -count=1
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/server \
  -run '^(TestRouteMongo2MiBRecordLuaScanAndPartialBatch|TestRouteMongoAppliedWriteAndNativeReplyLossAreNotReplayed|TestRecordStreamPublishesFIFOAfterOutOfOrderBackendCompletion)$' \
  -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/store \
  -run '^TestNativeCoalescedRecordsKeepLargeResultsAndRetainedOwners$' -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/backend/mongodb \
  -run '^(TestMongoMixedWriteLostReplyDoesNotReplayExpression|TestMongoLuaConflictRetriesStopAtAttemptBound)$' \
  -count=1 -v
scripts/mongo-local.sh stop
```

These are explicit integration commands. Default `go test ./...` does not start
MongoDB or execute this experiment. Exact command counts and histograms for Weir
vary with scheduling; the test intentionally validates correctness and baseline
accounting without hard-coding a required speedup.

The table contains only the final source run, made from a retained test executable.
Race checking was run separately and passed; its instrumented durations are not
included in the performance table. The local evidence directory also retains the
raw correctness/race logs, `build-inputs.json`, a snapshot of 96 actual local
compilation input files, and their `source-manifest.sha256`. It contains Go sources, required
embedded inputs and module declarations; it does not capture environment files or
credentials. The directory is ignored by Git and is not a published benchmark.

```sh
mkdir -p .testdata/value-proof-20261015-refactor
GOPROXY=off go test -tags integration -c \
  -o .testdata/value-proof-20261015-refactor/server.test ./internal/server
WEIR_INTEGRATION=1 .testdata/value-proof-20261015-refactor/server.test \
  -test.run '^TestValueProofMongoReadPaths$' -test.count=1 -test.v \
  > .testdata/value-proof-20261015-refactor/value-proof.log 2>&1
shasum -a 256 .testdata/value-proof-20261015-refactor/server.test
```

Run receipt:

```text
source base: db52eed45f6f3812dc63f22935bc8cfb03955a5d
test executable SHA-256: 82f9b660e81bdbacce136261554e1678ecd57fddfbea5e1091b65bb068ba07f6
raw value-proof.log SHA-256: 174b33cf4c5703eabbfbfd7305b03358b05576d262552221e54f2f83be1cdcf0
source-manifest.sha256 SHA-256: efaae6a6287a3663ecdc182d2b4f6f9c08677b9d415124ca8d5b41ec0ba9a6aa
value_proof_integration_test.go SHA-256: 3dee811652dd4238e414e114ecab9ab30f36198567394564604d360ca9de0889
internal/store/runtime.go SHA-256: b5efdd4c8b7e8affa1f6306535000454e608f101b30d0569571531a356b939fd
internal/backend/mongodb/batch.go SHA-256: e5eecc706bb560a0f3efe28ebb3924940bd11d5dd93bb1c9512802d5ecbc1a4c
```

The production/configuration/peer refactor is committed at the source base above;
the checkout additionally included the new value experiment. The base commit alone
does not identify that unpublished test executable.
The retained source snapshot and executable identify this local observation, not
a released binary or a portable performance guarantee. Rebuild the current
checkout to verify current behavior, and retain the complete source/binary receipt
for any production capacity claim.

The final value experiment, separate race check and every listed correctness
test passed. The owned MongoDB process was then stopped through
`scripts/mongo-local.sh stop`; a TCP probe and listener check confirmed that
`127.0.0.1:27028` was closed. The local evidence directory retains `value-proof.log`,
`race.log`, `server-correctness.log`, `store-correctness.log`,
`mongo-correctness.log` and `cleanup.log` alongside the executable and source
receipt.
