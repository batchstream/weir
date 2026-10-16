# Complete-process production qualification

Status: the first formal sustained run failed at its 300-second mixed burst.
PRs 79 and 80 corrected the Native ownership and transient admission boundaries.
A separate proven typed large-read OOM must be corrected before the original
600-second sustained and fault qualification gates are run again.

## Fixed acceptance workload

The opt-in `TestProductionProcessQualification` runs a native Linux arm64 CLI in
a cgroup-v2 container with 1 GiB memory and two CPUs, against owned real MongoDB
8.0.32 and Elasticsearch 8.19.22. The driver and backend oracles run outside that
container. All service queue, concurrency, batching, watermarks and transport
settings retain their defaults. The primary client uses the real SDK's Resolve
initialization and refresh path.

After 30 seconds of warmup, the formal run offers 200 logical RPCs per second
for 600 seconds at absolute five-millisecond arrival times. Read, Put, Native,
Lua and four-record batch operations are reported separately for each backend,
including payload mix, offered/completed counts, driver lag and latency tails.
Most records are 1 KiB, with fixed fractions of 256 KiB and 2 MiB. A separate
256-call burst overlaps the steady workload. Thirty-two large Native consumers
receive output every 500 ms for 120 seconds, crossing the default thirty-second
stall interval. Independent backend oracles verify every confirmed mutation.

The predeclared steady p99 gates are Read <=100 ms, Put/Native <=250 ms and
Lua/batch <=500 ms. Every normal call must finish within three seconds, with no
business failures or unexpected resource rejections. The fixed driver capacity
is 512 calls; saturation is an explicit failure rather than suppressed arrival.
Final queue, execution, publication, session and output-byte gauges must become
zero within five seconds, with memory admission recovered. The 120-second
smoke option is explicitly not formal qualification. Formal evidence additionally
requires the supplied binary hash and exact clean source identity.

## Preserved smoke failures and network control

The [first smoke](production-process-results-2026-10-16/production-qualification-smoke-first.txt)
found a driver error: its BSON document did not put `_id` first, which Weir
correctly rejected. The driver now produces ordered BSON. The
[next smoke](production-process-results-2026-10-16/production-qualification-smoke-retry.txt)
failed with a Native HTTP incomplete response at about two seconds while CPU,
resident memory and admission pressure were low.

An independent Linux HTTP probe, without Weir or its SDK, reproduced fresh TCP
connection timeouts through Docker Desktop's host-published port. A simultaneous
same-process control offered 15 requests per second over each route for 120
seconds: the direct Elasticsearch bridge completed 1800/1800, p99 4.87 ms; the
host-published route failed 51/1800 at its two-second TCP connect deadline. DNS
timings and server backlog counters did not explain those failures. The
[manifest](production-process-results-2026-10-16/native-http-diagnostic-manifest.json),
[raw route control](production-process-results-2026-10-16/native-http-diagnostic-network-probe.txt)
and source probes retain the identities, schedules, stages and hashes. Attribution
of the original individual Native request remains an inference because that
request did not have an internal TCP trace.

The fixture now verifies the owned Elasticsearch container's pinned image and
private bridge address before using that Linux route. The host oracle still
uses loopback. No service timeout or opaque-command retry was changed.

## Successful exploratory smoke

The [bridge smoke](production-process-results-2026-10-16/production-qualification-smoke-bridge.txt)
completed 6000 warmup calls, 24000 steady calls and 256 burst calls without a
business failure, Native HTTP error or resource rejection. Per-method steady
p99 ranged from 18.3 to 73.4 ms. Its 32 slow consumers continued for sixty
seconds, with a maximum progress gap of 646 ms. Oracles checked 6174 Mongo and
6175 Elasticsearch documents/effects. Final resources became zero, and the CLI
exited normally without OOM. This exploratory binary predates the final shared
memory and full-output corrections; its dirty identity is preserved in the raw
log. These observations are not a clean-source release qualification.

## Remaining full-output boundary

Distributing sixteen slow Mongo and sixteen slow Search replies did not prove
single-Store progress with larger retained results. A
[focused complete-process test](production-process-results-2026-10-16/native-output-budget-before-probes.txt)
issued 32 Mongo Native queries with 8 MiB replies to one default Store. At a
stable 16 active, three paused and thirteen queued, working set was about 199 MB
of 1 GiB and CPU was low. Both an independent typed Read and short Native query
exceeded their three-second deadlines. The backend had already supplied the
retained replies, and no output watchdog had expired. Cancellation returned all
Store/RPC/output bytes to zero and the CLI exited normally.

The input queue's 32 MiB limit also bounds borrowed Native output reservations;
when that budget fills, the remaining output waits keep all backend execution
slots. Correcting that responsibility boundary is required before final
qualification. Merely increasing the queue or silently retrying opaque Native
commands would not establish the required reliability.

## Additional backend coverage

Owned native OpenSearch 3.3.0 passed its normal-capacity real-backend integration
race suite and three repeated Native progress cases. The
[start identity](production-process-results-2026-10-16/production-opensearch-start.txt),
[integration result](production-process-results-2026-10-16/production-opensearch-integration.txt),
[progress result](production-process-results-2026-10-16/production-opensearch-native-progress.txt)
and [cleanup](production-process-results-2026-10-16/production-opensearch-cleanup.txt)
are retained. Its container was removed after those checks; this is additional
compatibility coverage, separate from the Elasticsearch sustained workload.
