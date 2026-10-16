# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:15:14.19514Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 100%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 0 | 18214 | 18214 | 0 | 3637.1 | 10.239 | 12.5 | 0.0 | 82.7 | true |
| 1 | 32 | 1 | weir | 0 | 18523 | 18523 | 0 | 3699.2 | 10.239 | 3.3 | 0.0 | 82.2 | true |
| 1 | 32 | 2 | direct | 0 | 17696 | 17696 | 0 | 3530.2 | 11.263 | 12.4 | 0.0 | 82.5 | true |
| 1 | 32 | 2 | weir | 0 | 18007 | 18007 | 0 | 3596.4 | 11.263 | 3.3 | 0.0 | 82.5 | true |
| 1 | 32 | 3 | direct | 0 | 18669 | 18669 | 0 | 3730.1 | 10.239 | 13.0 | 0.0 | 82.8 | true |
| 1 | 32 | 3 | weir | 0 | 18836 | 18836 | 0 | 3762.1 | 10.239 | 3.6 | 0.0 | 82.4 | true |
| 1 | 256 | 1 | direct | 0 | 109475 | 109475 | 0 | 21850.8 | 18.431 | 63.0 | 0.0 | 83.0 | true |
| 1 | 256 | 1 | weir | 0 | 106116 | 106116 | 0 | 21186.5 | 18.431 | 12.0 | 0.0 | 82.3 | true |
| 1 | 256 | 2 | direct | 0 | 102395 | 102395 | 0 | 20427.7 | 18.431 | 63.6 | 0.0 | 82.7 | true |
| 1 | 256 | 2 | weir | 0 | 100192 | 100192 | 0 | 19994.4 | 20.479 | 17.6 | 0.0 | 82.3 | true |
| 1 | 256 | 3 | direct | 0 | 105271 | 105271 | 0 | 21022.8 | 18.431 | 64.1 | 0.0 | 82.9 | true |
| 1 | 256 | 3 | weir | 0 | 99581 | 99581 | 0 | 19886.5 | 20.479 | 18.5 | 0.0 | 82.4 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 1216 | 15.233 | 18523 | 0.914 | 6.938 | 0 |
| 1 | 32 | 2 | 1176 | 15.312 | 18007 | 0.944 | 7.162 | 0 |
| 1 | 32 | 3 | 1204 | 15.645 | 18836 | 0.935 | 6.800 | 0 |
| 1 | 256 | 1 | 2706 | 39.215 | 106116 | 0.732 | 8.375 | 0 |
| 1 | 256 | 2 | 2580 | 38.834 | 100192 | 0.720 | 9.236 | 0 |
| 1 | 256 | 3 | 2496 | 39.896 | 99581 | 0.746 | 9.250 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 21100.3 (256 workers), Weir 20355.8 (256 workers), observed change -3.53%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 3632.4 | 3685.9 | +1.47% | 9.215 / 11.263 / 12.287 | 9.215 / 11.263 / 13.311 |
| 256 | 21100.3 | 20355.8 | -3.53% | 12.287 / 18.431 / 30.719 | 12.287 / 20.479 / 45.055 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
