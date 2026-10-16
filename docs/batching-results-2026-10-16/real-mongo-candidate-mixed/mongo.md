# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:11:51.741403Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 10%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 163348 | 18277 | 181625 | 0 | 36289.1 | 6.655 | 36.2 | 0.0 | 82.3 | true |
| 1 | 32 | 1 | weir | 57067 | 6310 | 63377 | 0 | 12633.2 | 18.431 | 8.7 | 0.0 | 82.0 | true |
| 1 | 32 | 2 | direct | 165409 | 18526 | 183935 | 0 | 36734.3 | 6.655 | 36.3 | 0.0 | 82.3 | true |
| 1 | 32 | 2 | weir | 57023 | 6296 | 63319 | 0 | 12628.7 | 18.431 | 8.6 | 0.0 | 81.7 | true |
| 1 | 32 | 3 | direct | 166241 | 18608 | 184849 | 0 | 36908.9 | 6.655 | 36.4 | 0.0 | 82.4 | true |
| 1 | 32 | 3 | weir | 53817 | 5975 | 59792 | 0 | 11896.6 | 18.431 | 8.2 | 0.0 | 81.9 | true |
| 1 | 256 | 1 | direct | 175439 | 19470 | 194909 | 0 | 38868.9 | 13.311 | 47.8 | 0.0 | 83.2 | true |
| 1 | 256 | 1 | weir | 137011 | 15110 | 152121 | 0 | 30255.4 | 24.575 | 5.3 | 0.0 | 82.2 | true |
| 1 | 256 | 2 | direct | 179243 | 19904 | 199147 | 0 | 39738.9 | 13.311 | 47.9 | 0.0 | 82.9 | true |
| 1 | 256 | 2 | weir | 136907 | 15088 | 151995 | 0 | 30335.1 | 24.575 | 6.0 | 0.0 | 82.3 | true |
| 1 | 256 | 3 | direct | 182287 | 20231 | 202518 | 0 | 40394.5 | 13.311 | 48.4 | 0.0 | 83.0 | true |
| 1 | 256 | 3 | weir | 132253 | 14589 | 146842 | 0 | 29292.4 | 26.623 | 5.5 | 0.0 | 82.7 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 38016 | 1.667 | 63377 | 1.122 | 0.293 | 0 |
| 1 | 32 | 2 | 38651 | 1.638 | 63319 | 1.117 | 0.328 | 0 |
| 1 | 32 | 3 | 35397 | 1.689 | 59792 | 1.105 | 0.358 | 0 |
| 1 | 256 | 1 | 9447 | 16.103 | 152121 | 1.582 | 1.708 | 0 |
| 1 | 256 | 2 | 9956 | 15.267 | 151995 | 1.574 | 1.708 | 0 |
| 1 | 256 | 3 | 9337 | 15.727 | 146842 | 1.594 | 1.839 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 39667.4 (256 workers), Weir 29961.2 (256 workers), observed change -24.47%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 36644.2 | 12385.8 | -66.20% | 0.223 / 6.655 / 8.191 | 0.895 / 18.431 / 26.623 |
| 256 | 39667.4 | 29961.2 | -24.47% | 7.167 / 13.311 / 18.431 | 7.167 / 24.575 / 36.863 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
