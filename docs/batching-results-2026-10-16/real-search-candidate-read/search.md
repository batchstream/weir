# Database saturation comparison

Backend **search**. Started 2026-10-16T11:13:34.321187Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 2s and measurement 5s per path/stage, 3 paired rounds, requested writes 0%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 19565 | 0 | 19565 | 0 | 3848.7 | 65.535 | 101.4 | 85.7 | 85.7 | true |
| 1 | 32 | 1 | weir | 11309 | 0 | 11309 | 0 | 2259.6 | 73.727 | 100.9 | 85.8 | 85.8 | true |
| 1 | 32 | 2 | direct | 57253 | 0 | 57253 | 0 | 11446.8 | 4.607 | 100.6 | 83.1 | 83.1 | true |
| 1 | 32 | 2 | weir | 13326 | 0 | 13326 | 0 | 2663.4 | 65.535 | 100.2 | 84.5 | 84.5 | true |
| 1 | 32 | 3 | direct | 54013 | 0 | 54013 | 0 | 10789.9 | 6.143 | 97.8 | 89.8 | 89.8 | true |
| 1 | 32 | 3 | weir | 37537 | 0 | 37537 | 0 | 7502.7 | 7.167 | 95.9 | 83.3 | 83.3 | true |
| 1 | 256 | 1 | direct | 61735 | 0 | 61735 | 0 | 12318.4 | 61.439 | 101.1 | 83.9 | 83.9 | true |
| 1 | 256 | 1 | weir | 89362 | 0 | 89362 | 0 | 17841.8 | 26.623 | 60.5 | 0.0 | 83.4 | true |
| 1 | 256 | 2 | direct | 63470 | 0 | 63470 | 0 | 12581.5 | 57.343 | 101.2 | 85.1 | 85.1 | true |
| 1 | 256 | 2 | weir | 93240 | 0 | 93240 | 0 | 18612.1 | 24.575 | 55.9 | 0.0 | 85.5 | true |
| 1 | 256 | 3 | direct | 65398 | 0 | 65398 | 0 | 12986.0 | 57.343 | 100.1 | 83.9 | 83.9 | true |
| 1 | 256 | 3 | weir | 95029 | 0 | 95029 | 0 | 18983.9 | 24.575 | 62.4 | 0.0 | 84.0 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 5883 | 1.922 | 11309 | 0.815 | 12.562 | 0 |
| 1 | 32 | 2 | 7115 | 1.873 | 13326 | 0.824 | 10.571 | 0 |
| 1 | 32 | 3 | 20149 | 1.863 | 37537 | 0.120 | 3.257 | 0 |
| 1 | 256 | 1 | 8356 | 10.694 | 89362 | 0.639 | 6.160 | 0 |
| 1 | 256 | 2 | 8560 | 10.893 | 93240 | 0.687 | 5.867 | 0 |
| 1 | 256 | 3 | 8740 | 10.873 | 95029 | 0.673 | 6.320 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 12629.1 (256 workers), Weir 18479.2 (256 workers), observed change +46.32%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 8669.4 | 4141.6 | -52.23% | 1.791 / 7.167 / 65.535 | 4.607 / 49.151 / 73.727 |
| 256 | 12629.1 | 18479.2 | +46.32% | 13.311 / 57.343 / 65.535 | 13.311 / 24.575 / 32.767 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
