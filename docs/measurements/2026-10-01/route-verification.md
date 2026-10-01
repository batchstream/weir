# Route verification evidence

This report records validation of the new Route execution path on Darwin/arm64,
Go 1.27.1. It complements `docs/route-validation.md`, which records repository-wide
checks. Timings below are actual runs, not historical results or tagged compilation.

## Fixtures and safety

MongoDB was the isolated loopback replica set `weir_m1`, version 8.0.32, port 27028.
Search fixtures were isolated, owned Docker containers named
`weir-m17-elasticsearch` and `weir-m17-opensearch`. Elasticsearch was 8.19.22,
build `3b2a41103de35e0af4064d647974032fcc1bcde9`; OpenSearch was 2.19.6,
build `97d3c13bf22a4a72ac11dc503fe44c97662b9161`. The exact pinned official
OpenSearch image was pulled before running its suite. Secure tests created their
own local TLS/authentication fixtures. They did not use user credentials or
production endpoints. Tests remove only their own databases, indices, and secure
fixtures.

## Correctness and resource checks

Independent acceptance tests use real TCP, the production router and scheduler,
and an adapter fixture implementing the shared execution boundary. They verify
large response fidelity and fragmentation, request half-close, invalid IDs and
destination changes after an applied write, unknown/duplicate/missing terminal
responses, non-OK RPC termination, independent completion, same-record execution
order, cross-RPC batching, cancellation isolation, and resource recovery.

Same-record tests stall the first write, verify an independent read completes,
verify the later same-record write has not entered the backend, then verify backend
execution order 1 then 3, two complete APPLIED results, and the final value. Response
order is intentionally unrestricted. Different request IDs may interleave response
fragments; bytes for one ID remain ordered. Backend execution order does not imply
response order.

SDK fault tests verify concurrent production/consumption, malformed and incomplete
responses, producer/consumer errors, canceled blocked sends, early EOF while a
producer waits for more input, and joining both directions after cancellation.
`Complete` fires only after a valid business terminal Event and the separate empty
transport end. It preserves a completed request's evidence if the RPC later fails;
the single-result convenience method still returns an error for a non-OK RPC.

Final focused race run:

```sh
GOPROXY=off go test -race ./internal/server ./routeclient \
  -run '^(TestRouteAcceptance|TestRun|TestRecord)' -count=5
```

Passed: server 4.902 s; SDK 3.273 s. The final SDK-only race run also passed, 1.777 s.

## Actual backend suites

| Run | Result |
| --- | --- |
| MongoDB full backend integration, including owned TLS connection tests | 79 top-level tests passed, 34.625 s; separate TLS391 profile was then run below |
| Store full MongoDB integration | 27 top-level tests passed, 2.453 s |
| MongoDB TLS391 no-replay profile | Ordinary write, expression, and write-error cases passed, 6.371 s |
| Elasticsearch complete backend integration with race detector | 74 top-level tests passed, 21.291 s; explicit secure/component profiles were separate |
| Server complete integration with race detector, MongoDB and Elasticsearch Route tests enabled | Passed, 24.838 s; explicit memory/performance runs were separate |
| OpenSearch complete backend integration | 74 top-level tests passed, 16.661 s; explicit secure/component profiles were separate |
| OpenSearch HTTPS/Basic and loaded-component evidence | All three top-level tests passed, 66.636 s |
| Elasticsearch HTTPS/Basic | Both top-level tests passed, 49.448 s |
| OpenSearch new Route 2 MiB and reply-loss tests with race detector | Passed |

Commands:

```sh
GOPROXY=off WEIR_INTEGRATION=1 WEIR_M10_INTEGRATION=1 \
  go test -json -p 1 -tags integration \
  ./internal/backend/mongodb ./internal/store -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -json -p 1 -tags integration ./internal/backend/mongodb \
  -run '^TestMongoSCRAMTLS391NoReplay$' -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
  go test -json -race -p 1 -tags integration \
  ./internal/backend/search ./internal/server -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch \
  go test -json -p 1 -tags integration ./internal/backend/search -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch \
  WEIR_SEARCH_SECURE_INTEGRATION=opensearch WEIR_SEARCH_COMPONENT_EVIDENCE=1 \
  go test -json -p 1 -tags integration ./internal/backend/search \
  -run '^(TestSecureSearch|TestOpenSearchComponentEvidence)' -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_SEARCH_SECURE_INTEGRATION=elasticsearch \
  go test -json -p 1 -tags integration ./internal/backend/search \
  -run '^TestSecureSearch' -count=1

GOPROXY=off WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=opensearch \
  go test -race -tags integration ./internal/server \
  -run '^TestRouteSearch2MiB' -count=1
```

Real Route tests independently insert exactly 2 MiB BSON/JSON records and read
them through two relays with exact byte fidelity. MongoDB Route tests also verify
twelve same-record Lua increments and independent database readback, scan terminal
evidence, mixed successful/failed mutations, and lost record/native write replies.
Search Route tests verify lost bulk replies. Fault proxies forward the operation
to the actual database before losing its reply; an independent read confirms the
write happened once, and command counts verify there was no automatic replay.

The full backend suites retain record actions, safe batching, native exchanges,
scan traversal, cancellation, write concern/CAS/transaction behavior, partial
failures, and Lua atomicity tests. Scans now fetch one document per step. Traversal
tests check bounded pages, termination, full count, no duplicates and fidelity,
including 32 × 300 KiB MongoDB documents whose total exceeds 8 MiB.

## Issues found and resolved

- A cold scheduler starting with one permit could stall an independent request
  behind a slow request indefinitely. Its initial window now equals configured
  concurrency; congestion still reduces it. Independent completion passed cold.
- SDK early EOF could wait for an uncanceled producer. EOF now cancels and joins
  it, including after an earlier request completed.
- Forced shutdown discarded synchronous tickets' indeterminate write evidence
  before callers read it. Evidence now survives until Ack. Both default regression
  and actual backend shutdown tests passed.
- The graceful-drain fixture failed to consume its bounded Event channel. The
  revised test concurrently consumes/releases Events and acknowledges terminal
  frames, verifies six APPLIED writes, database count six, and zero retained state.
- Scan and physical read-batch tests assumed old page/batch sizes. They now verify
  the new bounds without dropping traversal, fidelity, result or error assertions.
- Owner remote-tail tests assumed no cleanup command after a canceled bulk write.
  They now observe exactly one acknowledged `killSessions`, balanced owned
  connections, two distinct writes once each, and final independent readback.

An eight-iteration OpenSearch Route fault-test repetition passed. The next two
iterations failed during real backend fixture setup with HTTP 429: the fixture's
512 MiB JVM heap hit its parent circuit breaker. Only the owned OpenSearch instance
was restarted; the full backend suite and final Route race run then passed. This
is not reported as ten successful repetitions or as a router memory result.

## Measurement scope

`route-memory.json` is produced by the explicit three-process memory acceptance:

```sh
GOPROXY=off WEIR_ROUTE_MEMORY=1 \
  WEIR_ROUTE_MEMORY_REPORT="$PWD/docs/measurements/2026-10-01/route-memory.json" \
  go test ./internal/server -run '^TestRouteMemory200MiBTwoRelays$' -count=1 -v
```

One active RPC and a fixed 2 MiB record size are used for 100 and 400 records:
200 MiB and 800 MiB of legal responses. The consumer initially pauses 250 ms,
then waits 1 ms per response frame. Executor and two relays are independent OS
processes; Lua remains in the Weir process. This fixture measures real protobuf,
gRPC/HTTP2 and application buffering, but its executor is an adapter fixture,
not a database throughput measurement.

The report separates route ledgers, runtime tasks/bytes/publishers, allocated
objects, heap allocation/in-use, goroutines, and RSS. `HeapObjects` during load
includes objects not yet collected; active-state gauges and post-GC measurements
establish recovery. RSS is observed, not assumed to return immediately. The test
asserts configured queue/budget limits, full byte fidelity, zero failed requests,
zero retained tasks/bytes after load, and a relay working-set plateau when total
volume increases fourfold. The report includes sampled maxima and post-GC
baselines/recovery, rather than one instantaneous reading.

The final memory run passed, 25.295 s. All 200 MiB / 800 MiB were consumed in
3,400 / 13,600 response frames, with zero failures. Entry relay heap peaks were
18.05 / 19.52 MiB and the second relay's were 17.70 / 17.30 MiB; corresponding RSS
peaks were 40.06 / 43.45 MiB and 39.56 / 40.78 MiB. Executor heap peaks were
42.21 / 42.19 MiB. All layers peaked at eight unfinished IDs. The executor peaked
at eight publishers; all retained tasks, bytes, publishers and RPCs were zero
afterward. Post-GC heap allocation was 0.87–0.95 MiB per process. The JSON records
baseline, peak and post-GC object counts and goroutines separately.

`route-performance.json` is produced by:

```sh
GOPROXY=off WEIR_INTEGRATION=1 WEIR_ROUTE_PERFORMANCE=1 \
  WEIR_ROUTE_PERFORMANCE_REPORT="$PWD/docs/measurements/2026-10-01/route-performance.json" \
  go test -tags integration ./internal/server -run '^TestRouteMongoPerformance$' \
  -count=1 -v
```

The workload is 40,000 point reads of 256 preloaded BSON records with a 1 KiB pad,
four finite RPCs, backend concurrency four, 16 operations/8 MiB per input batch,
8 MiB result batch and 1 ms collection window. It verifies every result. Latency
runs from the Produce callback to the complete business Event callback, including
admission and excluding final RPC drain. Client and local router share one process;
MongoDB CPU/RSS are sampled separately. Throughput, p50/p95/max latency, CPU,
heap/RSS and failure count are reported. There is no claim that streaming is faster
than old RPCs or unmatched direct database calls.

The final isolated run completed all 40,000 reads in 5.506 s: 7,264.34 operations/s,
p50 4.047 ms, p95 6.463 ms, maximum 79.284 ms, and zero failures. Client plus router
CPU was 9.63 s, observed peak heap allocation 4.03 MiB, and peak RSS 36.00 MiB.
MongoDB CPU was 1.53 s and observed peak RSS 188.09 MiB. Setup and database seeding
are excluded from the measured interval.

These are local measurements on one host. Production topology, sustained load,
fair backend-to-backend comparisons, and every deployment-specific TLS/budget or
artifact profile are not established by this report.
