# Real database experiments, 2026-10-16

These experiments use four independent OS client processes. Each worker sends
one record directly through the native driver or through the Weir SDK, waits
for completion, then sends its next record. Clients do not assemble bulk requests.
Each process has its own client connection pool. Native and Weir stages alternate
within each of three rounds, at total concurrency 32 and 256.

The runner is the sibling `weir-tests` project at
`a9ed795eef28e9e9c924d0fa8231c190bf79a440`. Its source was not modified. The lab
binary was built from that revision, with SDK v0.15.0 and protocol v0.13.0;
both server versions use the same client binary. Baseline server source is
`30ac2d10e8ec85a94512036abada5041b39ea606`; candidate source and binary checksums
are retained with the build receipts. The server itself uses SDK v0.16.0 and
protocol v0.14.0. Successful calls and persisted fields are verified on both
paths rather than assuming wire compatibility from version labels.

All fixtures are task-owned and local. MongoDB 8.0.32 runs natively on macOS
arm64, without a database CPU quota. Elasticsearch 8.19.22 runs in Docker with
a one-CPU quota. Clients and Weir share the Apple M2 host with the databases;
these results are not isolated production capacity limits.

## Controls and interpretation

- Working set: 2,048 records, 1,024-byte deterministic document padding.
- Default server controls: 128 operations, 4 MiB batch input, 20 ms write
  collection, 16 concurrent adapter invocations. Effective configuration is
  retained in each report. Reads do not wait for future arrivals.
- MongoDB stages use 1 s warmup and 5 s timed measurement; Search stages use
  2 s warmup and 5 s timed measurement. Completion joins count toward elapsed
  time. Every call has a 10 s deadline.
- A tuning run, when present, explicitly overrides the collection window;
  it is separate from the comparison using default settings.
- MongoDB command counts are the measured business-command subset of
  `serverStatus`: find, update, bulkWrite, getMore, killCursors and transaction
  commands. They exclude metadata qualification, monitoring and handshakes.
  They are not counts of fsyncs or total database requests.
- Database CPU per 1,000 completed operations is estimated from observed CPU
  intervals, their quota/host-core denominator and completed operations. Raw
  coverage and all round values are retained. Comparing only CPU percentages
  while completing different amounts of work would be misleading.
- QPS uses total successful records divided by total timed elapsed duration.
  Reported p99 values are histogram bucket upper bounds. Errors, unknown writes,
  incomplete arrivals and persistence checks remain visible in the raw reports.

MongoDB baseline runs overlapped some development-time focused tests and short
microbenchmarks. They are exploratory implementation comparisons; the native
control and each round remain visible. Final candidate and Search confirmation
measurements are serialized with other agent benchmarks paused.

Search at concurrency 32 exhibits substantial JVM warmup drift in the baseline:
native throughput rises from 3,662 to 11,901 and 12,626 operations/s. This point
is retained but excluded from claims about implementation improvement. Its later
concurrency-256 native stages are stable at 12,213–12,913 operations/s. No
universal optimum or sustainable maximum is inferred from these finite stages.

Version VI completed the initial candidate MongoDB read/mixed/write matrix.
Independent final review then found an existing accounting bug when multiple
tickets share one prepared plan: result growth mutated that shared plan and
could undercount later reservations or race with dispatch copying. Version VII
moves the existing plan copy to successful admission, giving every ticket its
own accounting and context; execution uses that private plan directly. Ordinary
gRPC preparation already creates a plan per record, but the reusable core
contract and its benchmarks require this isolation too.

The version-VI MongoDB data is retained under `iteration6-real-mongo-*`. Its
Search run was interrupted deliberately and is marked `interrupted`; it is not
an accepted comparison. Final candidate results identify the new binary and
include the ownership fix.
The new ownership tests were also copied onto the original baseline: they
reproduce both the incorrect reservation and the data race. That expected
baseline failure is retained in `baseline-plan-regression.txt`; final version
VII passes the complete repository race suite and the real integration suites.
Exact validation commands and their exit status are in `validation.json`.

## Observed results

The final candidate completes all seven experiments: 84 timed native/Weir stages and 8,208,279 successful operations, with persistence checks passing and zero errors, timeouts or indeterminate writes. Every current candidate report matches the same version-VII binary checksum, and the final workspace rebuild produces that identical executable.

### Implementation comparison at 256 workers

These are observed baseline/current comparisons using default settings. Native controls are shown alongside Weir so host drift is visible. MongoDB baseline measurements remain exploratory for the overlap noted above; Search at this concurrency has steady native controls.

| Workload | Native old / current ops/s | Weir old / current ops/s | Weir/native old / current | Estimated Weir DB CPU s/1,000 ops old / current |
| --- | ---: | ---: | ---: | ---: |
| mongo read | 40,150 / 42,829 | 29,171 / 34,507 | 0.73 / 0.81 | 0.0089 / 0.0084 |
| mongo mixed | 37,661 / 39,667 | 26,808 / 29,961 | 0.71 / 0.76 | 0.0146 / 0.0149 |
| mongo write | 19,534 / 20,288 | 17,280 / 18,610 | 0.88 / 0.92 | 0.0438 / 0.0417 |
| search read | 12,665 / 12,629 | 15,610 / 18,479 | 1.23 / 1.46 | 0.0505 / 0.0323 |

### Current native comparison and collection profiles

N / W means native / Weir. A lower command count is the observed business-command subset, not total backend requests. CPU values are estimated seconds per 1,000 completed logical operations. The untuned cases use the unchanged 20 ms default; wait1ms is the explicit profile.

| Experiment | Workers | Native / Weir ops/s | Weir/native | Business commands/op N / W | Estimated DB CPU s/1,000 ops N / W |
| --- | ---: | ---: | ---: | ---: | ---: |
| mongo-candidate-mixed | 32 | 36,644 / 12,386 | 0.34 | 1.0000 / 0.6009 | 0.0792 / 0.0547 |
| mongo-candidate-mixed | 256 | 39,667 / 29,961 | 0.76 | 1.0000 / 0.0637 | 0.0968 / 0.0149 |
| mongo-candidate-mixed-wait1ms | 32 | 36,457 / 18,439 | 0.51 | 1.0000 / 0.5552 | 0.0792 / 0.0588 |
| mongo-candidate-mixed-wait1ms | 256 | 38,669 / 30,044 | 0.78 | 1.0000 / 0.0686 | 0.0981 / 0.0171 |
| mongo-candidate-read | 32 | 44,412 / 22,832 | 0.51 | 1.0000 / 0.4076 | 0.0789 / 0.0377 |
| mongo-candidate-read | 256 | 42,829 / 34,507 | 0.81 | 1.0000 / 0.0551 | 0.0825 / 0.0084 |
| mongo-candidate-write | 32 | 3,611 / 1,040 | 0.29 | 1.0000 / 0.0312 | 0.2816 / 0.1038 |
| mongo-candidate-write | 256 | 20,288 / 18,610 | 0.92 | 1.0000 / 0.0078 | 0.2385 / 0.0417 |
| mongo-candidate-write-wait1ms | 32 | 3,632 / 3,686 | 1.01 | 1.0000 / 0.0649 | 0.2784 / 0.0737 |
| mongo-candidate-write-wait1ms | 256 | 21,100 / 20,356 | 0.96 | 1.0000 / 0.0254 | 0.2409 / 0.0629 |
| search-candidate-read † | 32 | 8,669 / 4,142 | 0.48 | - / - | 0.1153 / 0.2391 |
| search-candidate-read | 256 | 12,629 / 18,479 | 1.46 | - / - | 0.0798 / 0.0323 |
| search-candidate-write † | 32 | 1,362 / 1,135 | 0.83 | - / - | 0.3789 / 0.2581 |
| search-candidate-write | 256 | 1,459 / 19,061 | 13.06 | - / - | 0.2122 / 0.0275 |

† The Search 32-worker observations retain startup/JVM drift and are excluded from implementation-benefit claims. Raw per-round QPS and CPU are available for every row.

At 256 readers, current Elasticsearch completes about 18,479 operations/s versus 12,629 native (1.46 times), using about 60% rather than 101% of the one-CPU database budget. Estimated CPU per completed unit is about 59% lower. Relative to the old Weir at this concurrency, observed throughput rises about 18% and estimated database CPU per completed unit falls about 36%.

At 256 writers, Elasticsearch completes about 19,061 operations/s versus 1,459 native (13.06 times). Estimated database CPU per completed unit falls about 87%; absolute database CPU is higher, about 52% versus 31%, because many more writes complete. This is a native/Weir service comparison; the old-Weir pure-write case was not rerun. Neither path establishes a sustainable capacity maximum.

For MongoDB at 32 writers, the 1 ms profile reaches about 3,686 operations/s versus 3,632 native, with about 74% lower estimated database CPU per completed operation. Against the default 20 ms profile, it increases throughput from about 1,040 operations/s. At 256 writers it raises throughput about 9%, but increases database CPU per completed operation about 51% and business commands per record from 0.0078 to 0.0254. This tradeoff does not justify one universal collection setting.

MongoDB reads and mixed traffic remain slower than direct access in these local comparisons, despite reduced business commands and CPU per completed unit. The improvement is workload-specific.

### Tail latency, selected confirmation points

Each value is the minimum–maximum of the three rounds' p99 histogram upper bounds, in milliseconds; it is not a precise quantile or a pooled p99.

| Case | Workers | Native p99 upper range ms | Weir p99 upper range ms |
| --- | ---: | ---: | ---: |
| mongo-candidate-write | 32 | 11.263–12.287 | 36.863–36.863 |
| mongo-candidate-write-wait1ms | 32 | 11.263–15.359 | 11.263–13.311 |
| search-candidate-read | 256 | 61.439–73.727 | 30.719–36.863 |
| search-candidate-write | 256 | 196.607–294.911 | 22.527–45.055 |

The complete round values, sampling coverage and validation flags are in [real-summary.json](real-summary.json). The table can be regenerated with `summarize_real.py --format markdown` from the compressed raw reports.

## Correctness and actual request aggregation

The opt-in integration tests use an independently observed database connection
or HTTP proxy and verify persisted results:

- `TestMongoCrossNamespaceNativeBulkPersistsAndIsolatesFailure`: qualified
  namespaces share one native bulk; duplicate-key and missing-target failures
  retain their own caller positions; successful records persist independently.
- `TestMongoLargeBatchSplitsAtNativeBoundaryWithConfiguredConnectionWorkers`:
  40 MiB of writes split at the actual native command boundary.
- `TestSearchCrossIndexPureReadsUseOneNativeExchange`: 128 caller positions,
  126 unique records and two concrete indexes use one real-time `_mget`;
  sources, missing IDs and equal IDs in different indexes remain correct.
- Existing end-to-end tests cover shared Lua transactions/OCC batches,
  same-resource increments, skewed indexes, generated large documents and
  visibility completion. Unknown mutation acknowledgements are not replayed.

Pure Search reads now bound each multi-record HTTP response using actual bytes.
An oversized response is discarded and its read-only group is split and read
again; no prefix is published. This can amplify database reads for large-source
workloads. A single large document retains the existing allowance to exceed
the exchange target. Final result retention uses the existing soft accounting;
the response window is not a hard bound on Store heap or total retained results.
Lua and mutation pre-reads retain their conservative source window and never
use the pure-read overflow retry mechanism.

## Reproduction and raw evidence

Run the lab from the sibling `weir-tests` root. Build each server with its
`scripts/build_local_server.py` so the binary, input manifest and source tree are
checksummed. The common experiment command is:

```sh
PATH_TO_LAB -mode saturation -weir PATH_TO_SERVER \
  -server-receipt PATH_TO_SERVER.receipt.json \
  -backend mongo -mongod .tools/mongod \
  -client-processes 4 -concurrency-levels 32,256 -batch-sizes 1 \
  -records 2048 -payload-bytes 1024 -write-percent 100 -rounds 3 \
  -warmup-duration 1s -duration 5s -capture-effective-config \
  -output NEW_OUTPUT_DIRECTORY
```

Use write percentages 0/10/100 for read/mixed/write cases. Search uses
`-backend search`, omits `-mongod` and uses `-warmup-duration 2s`. Every output
directory is new; existing evidence is preserved. JSON reports retain the
effective routing configuration, image pins, server checksums, process IDs,
round order, native controls, all resource samples and raw Prometheus snapshots.

`summarize_real.py` reads either JSON or compressed JSON and recomputes completed
QPS, business commands per operation, p99 bounds and estimated CPU per completed
unit. Raw reports and generated summary accompany this file.

The final formatter removes one trailing blank line from a test file. The measured and final-workspace build receipts retain both source-tree hashes, and rebuilding confirms an identical executable. See `workspace-format-audit.json` and `workspace-server.receipt.json`. Production Go source hashes are unchanged.
