# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:10:32.077243Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 0%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 213059 | 0 | 213059 | 0 | 42603.5 | 1.407 | 41.4 | 0.0 | 82.8 | true |
| 1 | 32 | 1 | weir | 113407 | 0 | 113407 | 0 | 22677.9 | 2.815 | 10.5 | 0.0 | 82.6 | true |
| 1 | 32 | 2 | direct | 222144 | 0 | 222144 | 0 | 44421.1 | 1.279 | 45.3 | 0.0 | 82.9 | true |
| 1 | 32 | 2 | weir | 113855 | 0 | 113855 | 0 | 22763.5 | 2.815 | 10.4 | 0.0 | 82.6 | true |
| 1 | 32 | 3 | direct | 231100 | 0 | 231100 | 0 | 46209.9 | 1.279 | 44.7 | 0.0 | 82.6 | true |
| 1 | 32 | 3 | weir | 115296 | 0 | 115296 | 0 | 23054.1 | 2.559 | 11.4 | 0.0 | 82.7 | true |
| 1 | 256 | 1 | direct | 213717 | 0 | 213717 | 0 | 42695.1 | 11.263 | 44.9 | 0.0 | 82.8 | true |
| 1 | 256 | 1 | weir | 173309 | 0 | 173309 | 0 | 34629.2 | 14.335 | 3.5 | 0.0 | 82.4 | true |
| 1 | 256 | 2 | direct | 209550 | 0 | 209550 | 0 | 41867.9 | 11.263 | 42.3 | 0.0 | 83.1 | true |
| 1 | 256 | 2 | weir | 172756 | 0 | 172756 | 0 | 34523.7 | 14.335 | 3.7 | 0.0 | 82.7 | true |
| 1 | 256 | 3 | direct | 219841 | 0 | 219841 | 0 | 43924.0 | 11.263 | 45.3 | 0.0 | 83.0 | true |
| 1 | 256 | 3 | weir | 172017 | 0 | 172017 | 0 | 34366.8 | 14.335 | 3.7 | 0.0 | 82.4 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 46057 | 2.462 | 113407 | 0.061 | 0.420 | 0 |
| 1 | 32 | 2 | 46069 | 2.471 | 113855 | 0.061 | 0.435 | 0 |
| 1 | 32 | 3 | 47500 | 2.427 | 115296 | 0.057 | 0.452 | 0 |
| 1 | 256 | 1 | 9360 | 18.516 | 173309 | 0.664 | 1.691 | 0 |
| 1 | 256 | 2 | 9631 | 17.937 | 172756 | 0.662 | 1.723 | 0 |
| 1 | 256 | 3 | 9576 | 17.963 | 172017 | 0.664 | 1.723 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 44411.5 (32 workers), Weir 34506.6 (256 workers), observed change -22.30%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 44411.5 | 22831.8 | -48.59% | 0.767 / 1.279 / 1.663 | 1.407 / 2.815 / 3.583 |
| 256 | 42829.0 | 34506.6 | -19.43% | 6.655 / 11.263 / 13.311 | 7.167 / 14.335 / 18.431 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
