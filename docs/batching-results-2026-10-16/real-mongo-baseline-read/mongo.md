# Database saturation comparison

Backend **mongo**. Started 2026-10-16T10:46:18.003577Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 0%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 237451 | 0 | 237451 | 0 | 47481.7 | 1.151 | 45.2 | 0.0 | 82.4 | true |
| 1 | 32 | 1 | weir | 100019 | 0 | 100019 | 0 | 19997.7 | 3.327 | 9.7 | 0.0 | 82.8 | true |
| 1 | 32 | 2 | direct | 225323 | 0 | 225323 | 0 | 45056.3 | 1.279 | 45.1 | 0.0 | 82.8 | true |
| 1 | 32 | 2 | weir | 111827 | 0 | 111827 | 0 | 22360.9 | 2.815 | 10.5 | 0.0 | 82.6 | true |
| 1 | 32 | 3 | direct | 232398 | 0 | 232398 | 0 | 46468.1 | 1.151 | 44.1 | 0.0 | 82.4 | true |
| 1 | 32 | 3 | weir | 86715 | 0 | 86715 | 0 | 17339.1 | 3.839 | 8.5 | 0.0 | 82.9 | true |
| 1 | 256 | 1 | direct | 218354 | 0 | 218354 | 0 | 43625.5 | 11.263 | 44.3 | 0.0 | 83.0 | true |
| 1 | 256 | 1 | weir | 146252 | 0 | 146252 | 0 | 29222.8 | 16.383 | 3.2 | 0.0 | 82.5 | true |
| 1 | 256 | 2 | direct | 191060 | 0 | 191060 | 0 | 38160.9 | 12.287 | 36.9 | 0.0 | 83.6 | true |
| 1 | 256 | 2 | weir | 154172 | 0 | 154172 | 0 | 30801.3 | 16.383 | 3.4 | 0.0 | 82.5 | true |
| 1 | 256 | 3 | direct | 193571 | 0 | 193571 | 0 | 38664.4 | 13.311 | 38.2 | 0.0 | 83.0 | true |
| 1 | 256 | 3 | weir | 137582 | 0 | 137582 | 0 | 27489.9 | 18.431 | 3.1 | 0.0 | 82.6 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 40636 | 2.461 | 100019 | 0.066 | 0.512 | 0 |
| 1 | 32 | 2 | 44635 | 2.505 | 111827 | 0.063 | 0.439 | 0 |
| 1 | 32 | 3 | 35243 | 2.460 | 86715 | 0.073 | 0.594 | 0 |
| 1 | 256 | 1 | 7992 | 18.300 | 146252 | 0.721 | 2.088 | 0 |
| 1 | 256 | 2 | 8585 | 17.958 | 154172 | 0.698 | 1.963 | 0 |
| 1 | 256 | 3 | 7539 | 18.249 | 137582 | 0.750 | 2.222 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 46335.4 (32 workers), Weir 29171.4 (256 workers), observed change -37.04%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 46335.4 | 19899.2 | -57.05% | 0.767 / 1.279 / 1.535 | 1.535 / 3.327 / 5.119 |
| 256 | 40149.9 | 29171.4 | -27.34% | 7.167 / 12.287 / 16.383 | 8.191 / 16.383 / 24.575 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
