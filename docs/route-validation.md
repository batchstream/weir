# Route validation report

This report covers the breaking Route refactor on `randy/unified-route`. It is
separate from the historical M-series measurements. Tests use owned loopback
fixtures; no production service was contacted and no deployment was performed.

## Protocol and runtime checks

The default complete Go suite and complete race suite passed with `GOPROXY=off`.
Both ordinary and integration-tagged `go vet` passed. Integration-tagged
compilation passed on Darwin and cross-compilation passed for Linux (not execution). Protocol generation was repeated and generated-file
SHA-256 hashes were identical. Python helper tests passed: 111 tests with four skips in ordinary mode,
and 111 tests with 31 skips under Python optimization (assert-dependent cases and
explicit artifact/profile opt-ins). Final checks after review are recorded below.

The replacement [coverage map](route-test-coverage.md) points to executable tests
for destination/ID/version validation, fragmented responses and missing terminals,
parallel completion, same-RPC record order, cross-RPC batching and cancellation
isolation, hop/deadline/cancel propagation, fixed endpoint selection, half-close,
slow sending, bounded queues and graceful drain. It also identifies the retained
private adapter conformance suites and the deleted old-protocol cases.

Real MongoDB 8.0.32 and Elasticsearch 8.19.22 Route acceptance transferred exact
2 MiB records through two relays and checked record contents. MongoDB additionally
checked twelve same-record Lua increments, complete scanning, partial write success,
confirmed duplicate-create rejection and persisted data. Fault proxies verified a
write whose reply was lost executed once and returned UNKNOWN, and a native reply
loss returned RESPONSE_INCOMPLETE. A disconnected attempt was never replayed.
OpenSearch 2.19.6 Route acceptance also passed. Complete MongoDB/Store and
Elasticsearch/OpenSearch conformance suites passed, including native reply loss,
scan exhaustion/cleanup, transaction/CAS evidence and cancellation. Separate
generated TLS/authentication fixtures passed MongoDB TLS/SCRAM qualification and error-code 391 no-replay,
Elasticsearch HTTPS/Basic, OpenSearch HTTPS/Basic and component evidence checks.
Exact commands, profiles and repair evidence are in the
[verification ledger](measurements/2026-10-01/route-verification.md).

## Slow-consumer resource measurement

Command:

```sh
GOPROXY=off WEIR_ROUTE_MEMORY=1 \
WEIR_ROUTE_MEMORY_REPORT="$PWD/docs/measurements/2026-10-01/route-memory.json" \
go test ./internal/server -run '^TestRouteMemory200MiBTwoRelays$' -count=1 -v
```

[Raw measurements](measurements/2026-10-01/route-memory.json) use three separate
Darwin/arm64 Go 1.27.1 processes: an executor and two forwarding nodes, over real
TCP/HTTP2/gRPC. The executor is a deterministic adapter fixture producing legal
2 MiB document Events; this isolates router memory from database memory. Separate
real database acceptance validates the actual MongoDB/Search record encoders.
One RPC traverses both relays, pauses consumption for 250 ms, then consumes one
response frame per millisecond. The record size, RPC count and limits are unchanged
between 100- and 400-record runs. Every record byte and every request end is checked.

| Measurement | 100 records / 200 MiB | 400 records / 800 MiB |
| --- | ---: | ---: |
| Time | 4.960 s | 19.036 s |
| Response frames | 3,400 | 13,600 |
| Failures | 0 | 0 |
| Entry peak HeapAlloc | 18.05 MiB | 19.52 MiB |
| Middle relay peak HeapAlloc | 17.70 MiB | 17.30 MiB |
| Executor peak HeapAlloc | 42.20 MiB | 42.19 MiB |
| Entry peak RSS | 40.06 MiB | 43.45 MiB |
| Middle relay peak RSS | 39.56 MiB | 40.78 MiB |
| Executor peak RSS | 72.33 MiB | 77.09 MiB |

Each node retained at most eight unfinished IDs. The paused executor performed no
more than eight requests before output backpressure stopped work. Retained output
reservations and publisher count stayed within the configured Store limits. After
load and explicit GC, every RPC/task/input/result/workspace occupancy returned to
zero, heap allocation was below 1 MiB per process, and each relay kept one reusable
connection. Post-GC HeapObjects were approximately 4,000--4,700 per process.

Live admission counters measure owned resources. HeapAlloc/HeapObjects samples
also contain garbage awaiting collection; they are not a precise count of live
application objects. RSS remained around its high-water mark after GC. Peaks are
sampled every 50 ms and aggregated separately by field, not one simultaneous
snapshot. The report demonstrates bounded scaling for this workload, not an exact
allocator or RSS ceiling. Cancellation, stalled sends, shutdown and leaked tasks
are separately asserted by transport/runtime acceptance tests.

## Comparable performance workload

[Raw performance measurements](measurements/2026-10-01/route-performance.json)
use MongoDB 8.0.32 primary reads with majority writes during fixture preparation,
256 preloaded records with a 1 KiB pad, four finite RPCs, four backend execution
slots, batch operations 16, batch input/result caps 8 MiB and a 1 ms collect window.
There are 10,000 reads per RPC, 40,000 reads total. Client and local router share
one process. Database CPU/RSS are measured separately.

The run completed in 5.506 s at 7,264 operations/s, with zero failures. Latency from
Produce callback to the complete business Event callback was p50 4.047 ms,
p95 6.463 ms and maximum 79.284 ms; final RPC drain is excluded from this per-record
latency. Client/router CPU was 9.63 s, peak heap 4.03 MiB and peak RSS 36.00 MiB.
MongoDB CPU was 1.53 s and peak RSS 188.09 MiB. These are measurements of this
workload, not a claim that streaming outperforms old RPCs or unbatched direct calls.

## Limits and omitted profiles

Native input is one bounded body (MongoDB 4 MiB, Search 8 MiB); response consumption
is incremental. Records are at most 2 MiB and scans fetch one document per step.
Collecting all results in a caller callback requires memory for all collected data.
Transport failure cannot prove a write did not happen; IDs offer no persistent
deduplication and no automatic mutation replay.

Lua stays in the main process and retains MongoDB transaction/Search CAS boundaries.
Programs must be trusted. Source/value/stack/time/concurrency limits do not impose
a hard allocation limit on arbitrary Lua VM objects. The process memory envelope
is admission accounting, not an OS RSS sandbox.

Application shared-process secure overload profiles, Linux cgroup/artifact,
packaged release-image, Kubernetes and production profiles
have not been executed for this refactor. Their explicit integration entrypoints
remain separate. Tagged compilation alone is not evidence they ran.

## Final checks and independent review

Final implementation checks passed: complete offline Go tests and race tests,
ordinary/integration vet, integration-tagged capacity-helper race tests, Darwin
integration compilation, Linux CGO0 integration cross-compilation, deterministic
protocol generation and ordinary/optimized Python regressions. Actual app/CLI
integration passed, including three-process Mongo/Search business operations and
process replacement recovery. The verification ledger records the complete actual
backend suites and focused repeated race checks.

An independent Agent started only after PR #20 was created, from published commit
`9a8f30d`. It reproduced three P2 issues: rejecting APPLIED plus a later typed
acknowledgement failure, discarding a validated result on a subsequent RPC error
in `Record`, and bypassing the backend I/O timeout for native requests. All three
have regression tests and fixes. The timeout correction also restores the cap for
Lua/scan singleton execution, while excluding native response publication waits.

Real MongoDB timeout tests verify NOT_APPLIED for a blocked Lua read and UNKNOWN
for a lost commit acknowledgement; independent database readback confirms exactly
one persisted increment. Native tests cover the default 2-second cap, shorter
configured caps and Search cumulative I/O time with slow response consumers.
A CI-only send/receive EOF race was also reproduced and corrected; authoritative
gRPC rejection status now survives, including 100 repeated no-CGO regressions.

Review of the final published fixes is recorded in the review evidence before
delivery; review of the initial commit alone is not final approval.
