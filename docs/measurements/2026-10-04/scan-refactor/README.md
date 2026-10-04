# Scan batch refactor evidence

The same public Execute Scan diagnostic ran against baseline
`70a1e0e571ff6121cae9edbe10af3ce294264ba9` and the refactored production tree.
`source.json` records the production tree hashes; `build-receipts.json` records
the two test binaries. Both use Go 1.27.1 on darwin/arm64 without race instrumentation.
The native MongoDB fixture has no imposed CPU limit. The Elasticsearch container
has two CPUs, a 512 MiB heap and a 1,536 MiB memory limit.

Each backend used one shared, unchanged fixture with 1,024 documents containing
1,024 padding bytes each. Client concurrency and Store concurrency were both
one, connection pool size two, GOMAXPROCS four and logical page size 256.
Each child warmed up with one traversal and measured three more. The order was
baseline, current, current, baseline, yielding six timed samples per version and
backend. The complete document set, original values, terminal count and final
gRPC success were checked on every traversal. Identical loopback observation
proxies counted native calls in both versions.

| Backend | Baseline median docs/s | Refactored median docs/s | Ratio | Baseline median ms | Refactored median ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| MongoDB 8.0.32 | 7,938 | 111,737 | 14.08 | 129.01 | 9.16 |
| Elasticsearch 8.19.22 | 1,435 | 29,528 | 20.58 | 713.81 | 34.68 |

Every timed traversal delivered exactly 1,024 documents through five Execute
RPCs. Native data fetches (`find` or `_search`) dropped from 1,025 to nine, a
99.12% reduction. This count excludes metadata and PIT lifecycle calls; elapsed
time includes the complete traversal. `summary.json` includes all 24 raw samples,
ranges and method details. This is a local Scan overhead diagnostic, not a
database saturation capacity result. Proxy overhead and shared host resources
limit extrapolation to production throughput.

MongoDB large-document validation scanned 64 documents with one MiB of padding
each across a short first page and a resumed page: 23 find calls, 76 native rows
and 79,698,847 native response bytes. The learned capacity of three survives a
one-document logical page remainder. Only the first prefetch reads an excess
tail. Independently measured fixed-capacity prefetch returned 290 rows and
304,100,087 bytes for the same 64-document shape, which identified the repeated
tail-reading defect before it was fixed.

`mongo-seek.json` records 30 explain/response comparisons over 10,000 string IDs
and mixed BSON IDs. Existing `$expr` keysets directly seek the `_id_` index:
both the first 128 rows and the 128 rows after `s:09499` examine 128 keys and
documents. All tested BSON boundaries match the native sorted suffix. Adding
`min` loses MaxKey at mixed-type boundaries, so the refactor preserves `$expr`.

Validation passed: complete offline default tests, default and integration vet,
Store/Execution/backend race tests, actual MongoDB and Elasticsearch Scan race
tests, and an independent review. Public RPC acceptance checks cover default and
256-document pages, 513-record traversal in six fetches, cross-instance resume,
PIT snapshot isolation, 4 MiB publication boundaries, slow consumers, cancellation
and complete resource release. The reviewer independently reran the public RPC
suite after the MongoDB capacity/token fix and found no remaining P1/P2 issues.
Component logs are archived here. Native behavior remains unchanged. Earlier
MongoDB continuation tokens must be replaced by starting a new Scan.

To reproduce, copy `scan_diagnostic_integration_test.go.txt` into
`internal/server/scan_diagnostic_integration_test.go` in separate baseline and
current checkouts, then build each with the commands in `build-receipts.json`.
Use the task-owned loopback fixtures documented by the repository test helpers,
set the binary paths and run the controller command in `command.txt`. The
controller creates and cleans its own unique database and index. The historical
temporary paths in the receipts should be replaced with the new checkout and
output paths.

To independently validate archived samples without running backends or keeping
the binaries, run `python3 analyze.py run.log` from this directory. It checks
the 24 complete samples and rebuilds `report.json`; the result matches
`summary.json`. `python3 verify_source.py current /path/to/weir` verifies every
production source file against `source-manifest.json` and the measured tree hash.
`cleanup.json` and the archived read-only cleanup observer confirm
that the measurement's owned MongoDB database and Elasticsearch index were absent
after the controller finished. The root task's backend services were stopped
separately after validation.
