# Database saturation comparison

Backend **mongo**. Started 2026-10-16T10:43:25.482221Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 100%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 0 | 18427 | 18427 | 0 | 3678.9 | 10.239 | 12.3 | 0.0 | 82.3 | true |
| 1 | 32 | 1 | weir | 0 | 5312 | 5312 | 0 | 1057.3 | 36.863 | 1.1 | 0.0 | 81.5 | true |
| 1 | 32 | 2 | direct | 0 | 17696 | 17696 | 0 | 3534.5 | 11.263 | 7.8 | 0.0 | 81.8 | true |
| 1 | 32 | 2 | weir | 0 | 5334 | 5334 | 0 | 1060.6 | 32.767 | 1.2 | 0.0 | 82.4 | true |
| 1 | 32 | 3 | direct | 0 | 18523 | 18523 | 0 | 3699.2 | 11.263 | 7.9 | 0.0 | 81.5 | true |
| 1 | 32 | 3 | weir | 0 | 5312 | 5312 | 0 | 1058.3 | 36.863 | 1.1 | 0.0 | 82.4 | true |
| 1 | 256 | 1 | direct | 0 | 107293 | 107293 | 0 | 21400.2 | 18.431 | 60.3 | 0.0 | 82.4 | true |
| 1 | 256 | 1 | weir | 0 | 83056 | 83056 | 0 | 16578.3 | 20.479 | 7.0 | 0.0 | 83.5 | true |
| 1 | 256 | 2 | direct | 0 | 99561 | 99561 | 0 | 19803.1 | 20.479 | 62.5 | 0.0 | 82.3 | true |
| 1 | 256 | 2 | weir | 0 | 86896 | 86896 | 0 | 17290.0 | 22.527 | 9.9 | 0.0 | 81.7 | true |
| 1 | 256 | 3 | direct | 0 | 87157 | 87157 | 0 | 17396.9 | 28.671 | 58.2 | 0.0 | 82.8 | true |
| 1 | 256 | 3 | weir | 0 | 90367 | 90367 | 0 | 17970.1 | 22.527 | 11.4 | 0.0 | 81.5 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 166 | 32.000 | 5312 | 20.367 | 8.232 | 0 |
| 1 | 32 | 2 | 167 | 31.940 | 5334 | 20.271 | 8.129 | 0 |
| 1 | 32 | 3 | 166 | 32.000 | 5312 | 20.295 | 8.256 | 0 |
| 1 | 256 | 1 | 656 | 126.610 | 83056 | 1.622 | 10.494 | 0 |
| 1 | 256 | 2 | 679 | 127.976 | 86896 | 1.561 | 10.158 | 0 |
| 1 | 256 | 3 | 706 | 127.999 | 90367 | 1.436 | 10.070 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 19534.2 (256 workers), Weir 17280.3 (256 workers), observed change -11.54%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 3637.6 | 1058.8 | -70.89% | 9.215 / 11.263 / 12.287 | 30.719 / 36.863 / 36.863 |
| 256 | 19534.2 | 17280.3 | -11.54% | 12.287 / 22.527 / 36.863 | 13.311 / 22.527 / 45.055 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
