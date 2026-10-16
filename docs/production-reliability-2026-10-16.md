# Weir local reliability verification 2026 10 16

Local verification found and fixed one cause of false overload admission: Linux
`memory.current` includes reclaimable inactive file cache. Ordinary SDK reads,
distinct writes, Native queries and Lua updates completed correctly against real
MongoDB and Elasticsearch. Slow Native consumers, resident Go idle heap and
bounded backend cleanup still create operational latency boundaries described
below. These results qualify the tested local scenarios; production instances
and sustainable maximum throughput are outside this run.

## Environment and scope

The starting server revision was `63bafddb0b459a44dedce2fe11441cae276f76c9`.
Production changes are limited to cgroup pressure accounting and its metrics.
Lua remains in the Weir process. Verification used Go 1.27.1, the published
`weir-go v0.16.0`, macOS arm64, and native Linux arm64 containers with cgroup v2.
MongoDB 8.0.32 ran as an owned loopback replica set. Elasticsearch 8.19.22 ran
in an owned container, with normal and deliberately constrained write capacity
tested separately. No production endpoint or existing credential file was used.

## False overload caused by file cache

An owned 256 MiB Linux container wrote and synced 224 MiB into its own file.
The original Guard latched overload despite a small process and working set:

| Measurement | Bytes |
| --- | ---: |
| Container limit | 268435456 |
| Raw memory.current | 251035648 |
| Reclaimable inactive_file | 234901504 |
| Working set | 16134144 |
| Independent process RSS | 7798784 |

The original implementation compared raw usage with the 80% watermark. The
fix compares `current - inactive_file` with each visible cgroup level's own
limit. It retains raw Current and exports `weir_memory_cgroup_working_set_bytes`.
Missing, malformed, oversized or inconsistent statistics, including duplicate
`inactive_file` entries, retain raw usage; an inactive count larger than current
is not subtracted. This preserves
conservative behavior during incomplete observations.

The corrected native test also allocated 224 MiB of anonymous memory, confirmed
overload, released it, refilled the file cache and confirmed recovery while raw
usage remained 250601472 bytes and the working set was 15720448 bytes. A separate
224 MiB shared-memory test correctly latched with a 244731904-byte working set
and only about 7.5 MB process RSS, then recovered after release. Process RSS alone
would have missed that real container pressure.

The [before](reliability-results-2026-10-16/page-cache-before.log),
[after](reliability-results-2026-10-16/page-cache-after.log) and
[shared-memory](reliability-results-2026-10-16/shared-memory.log) logs retain
independent kernel observations. The existing
[Linux native test](reliability-results-2026-10-16/existing-linux-native.log)
also passed. An independent agent reviewed hierarchy selection, zero/unlimited
limits, conservative fallback, arithmetic and recovery hysteresis.

## Low load business results

Each backend completed three sequential rounds with omitted Store/backend/SDK
tuning. Store defaults were 128 operations, 4 MiB, 20 ms write collection and
16 executions. Each round used 2 KiB padding and performed 32 distinct increasing
Put values with immediate readback, 128 reads, 128 Native point queries, 32 Lua
increments, 32 deliberately cancelled idle streams and 128 subsequent reads.
Readbacks were outside the Put timer. Native checks validated backend status
and document contents, including MongoDB `ok` and cursor results or HTTP 200 and
Search `_source`; complete transport alone did not count as query success.
Independent database observations verified the final counter and padding.

Every timed call had a two-second deadline. The table reports the range of
per-round nearest-rank p99 observations, in milliseconds. With only 32 mutation
samples per round, their p99 is the maximum observed sample. These are local
latency observations rather than a performance threshold or capacity estimate.

| Weir operation | MongoDB p99 range ms | Elasticsearch p99 range ms |
| --- | ---: | ---: |
| Typed read | 0.767 to 1.120 | 2.352 to 3.192 |
| Native point query | 0.485 to 0.541 | 1.665 to 1.814 |
| Put | 31.212 to 32.633 | 36.385 to 36.699 |
| Lua increment | 32.366 to 36.334 | 29.520 to 34.012 |
| Read after idle cancellation | 0.502 to 0.818 | 1.671 to 2.451 |

The largest typed-read samples were 1.128 ms for MongoDB and 13.292 ms for
Elasticsearch; the summary retains maxima as well as per-round quantiles.

All reads and acknowledged distinct writes matched expected contents. No
admission rejection occurred; queue, active execution, retained result, working
and encoded response accounting returned to zero. The 20 ms write collection
window contributes to sparse mutation latency; reads bypass it. A separate
scheduler regression held writes in 16 namespaces for a one-hour collection
window while 32 independent sparse reads completed within one second without
flushing those writes.

See [raw SDK observations](reliability-results-2026-10-16/low-load-verified.txt)
and [numeric summary](reliability-results-2026-10-16/low-load-summary.json).
The cancellation phase covers streams awaiting further input after a confirmed
read; backend execution cancellation is exercised by the backend integration
suites.

## Remaining operational boundaries

### Native output shares execution slots

Sixteen Native streams with zero HTTP/2 output windows occupied all default
Store execution slots, leaving ordinary reads queued despite minimal backend
work. A real TCP and gRPC regression set `Stall=500ms`: the output watchdog
closed the stalled connection and independent queued reads recovered. Store and
RPC resources drained; encoded wire accounting reached zero after test-triggered
GC established that orphaned transport buffers were unreachable. That wire
reclamation check does not establish an automatic 500 ms reclamation bound.
Other requests on the closed connection lost their responses.
The production default is 30 seconds; the shorter regression configuration does
not establish a default recovery SLA.

Continuously progressing long Native streams can hold slots indefinitely
without a caller deadline. Use prompt consumption and finite deadlines. For
independent latency requirements, use a separate Store and its existing finite
execution budget for Native traffic, and separate gRPC connections to reduce
connection-close impact. Separate connections alone do not isolate Store slots.
Additional Stores create additional adapters and backend connections. Removing
the execution limit is not a remedy for this contention.

### Freed Go objects may leave resident heap

In the 256 MiB container, a 224 MiB Go allocation followed by GC left only about
1.3 MB HeapAlloc but approximately 243 MB RSS and a 246 MB working set after
five seconds. Remaining overload reflected real residency. Test-only explicit
scavenging returned memory and restored admission; production does not force
GC or override the runtime memory limit.

An external `GOMEMLIMIT=160MiB` control recovered in this fixture.
`GOMEMLIMIT=192MiB` remained above the 70% recovery watermark and did not
recover within five seconds. These examples are specific to the fixture. Choose
a soft limit below the recovery watermark with headroom for other resident
memory, then verify burst recovery with actual documents and concurrency.
See the [unlimited](reliability-results-2026-10-16/go-heap-release.log),
[192 MiB](reliability-results-2026-10-16/go-heap-release-gomemlimit.log), and
[160 MiB](reliability-results-2026-10-16/go-heap-release-gomemlimit160.log) controls.

### Cleanup occupies a bounded execution tail

With `max_in_flight=1`, a lost MongoDB find reply followed by a test-held
`killSessions` cleanup occupied the only slot. A healthy query remained queued
and reached its 200 ms caller deadline without touching MongoDB. Releasing
cleanup restored progress and sockets drained. Cleanup uses the separate
two-second default lifecycle budget. This experiment did not reproduce the
same condition at the default 16 slots and does not establish a default outage.
The [log](reliability-results-2026-10-16/mongo-cleanup-tail.txt) and
[experimental source](reliability-results-2026-10-16/mongo_cleanup_probe_source.go.txt)
preserve that configured boundary.

### Lua type inspection traverses descendants

`weir.kind(table)` currently materializes the full descendant tree while
validating it. An isolated depth-1000 microbenchmark observed about 264 microseconds,
1.05 MB and 2032 allocations per inspection. Depth 1 took about 300 ns and
816 bytes. This is a potential cost when scripts repeatedly inspect large
subtrees, not evidence of a measured business-query delay. The implementation
was retained to preserve its existing validation semantics. The new benchmark
and [raw measurements](reliability-results-2026-10-16/kind-benchmark.log) make
the cost reproducible.

## Verification and reproduction

The ordinary suite, full race suite, static vet and 38 Python script tests
passed. The server and application suites also passed against both real
backends with race detection, including multi-process lifecycle and fault cases;
see the [ordinary](reliability-results-2026-10-16/final-default.txt),
[race](reliability-results-2026-10-16/final-race.txt), and
[real integration](reliability-results-2026-10-16/integration-race-final.txt) summaries.
The updated Linux arm64 binary built and ran its version command in a container;
business latency above was measured by the macOS application tests.
MongoDB backend integration with race passed. Elasticsearch normal
integration with race passed; congestion tests ran separately against the
constrained fixture and confirmed definite native capacity rejections.
The first attempted congestion run against normal capacity did not produce the
required overload; it was corrected by selecting the documented fixture profile.
The [capacity log](reliability-results-2026-10-16/backend-search-capacity.txt)
records 128 applied and 384 rejected bulk items, plus one applied and seven
rejected expression updates.

Use a matching Go 1.27.1 toolchain with prepared modules. Backend packages must
run serially because MongoDB fault injection is server-global:

```sh
GOWORK=off GOPROXY=off go test -race -count=1 ./...
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch GOWORK=off GOPROXY=off \
  go test -p 1 -tags integration -race -count=1 ./internal/server ./internal/app
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch GOWORK=off GOPROXY=off \
  go test -tags integration -count=3 -v \
  -run '^(TestMongoLowLoadBusinessReliability|TestSearchLowLoadBusinessReliability)$' ./internal/app
```

The gated native memory regressions use a self-owned 256 MiB container and
never modify host cgroups:

```sh
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -c -tags integration \
  -o /tmp/weir-overload.test ./internal/overload
docker run --rm --memory=256m --memory-swap=256m --network=none \
  --cpus=1 --pids-limit=64 -e WEIR_MEMORY_PAGE_CACHE=1 \
  -v /tmp/weir-overload.test:/weir/overload.test:ro busybox:1.37.0 \
  /weir/overload.test -test.v -test.timeout=30s \
  -test.run '^TestLinux(ReclaimablePageCacheDoesNotOverload|ReleasedGoHeapRecoversAfterScavenge)$'
```

Use `--shm-size=240m -e WEIR_MEMORY_SHMEM=1` and
`-test.run '^TestLinuxSharedMemoryDoesOverload$'` for the independent shared-memory
case. The tests create and clean only their own files.
