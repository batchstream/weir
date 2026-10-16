# Database saturation comparison

Backend **mongo**. Started 2026-10-16T10:58:48.271332Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 100%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 0 | 18355 | 18355 | 0 | 3665.2 | 10.239 | 13.0 | 0.0 | 82.5 | true |
| 1 | 32 | 1 | weir | 0 | 5152 | 5152 | 0 | 1023.7 | 36.863 | 1.4 | 0.0 | 82.5 | true |
| 1 | 32 | 2 | direct | 0 | 18564 | 18564 | 0 | 3707.5 | 10.239 | 12.9 | 0.0 | 82.7 | true |
| 1 | 32 | 2 | weir | 0 | 5184 | 5184 | 0 | 1035.0 | 36.863 | 1.4 | 0.0 | 82.7 | true |
| 1 | 32 | 3 | direct | 0 | 18469 | 18469 | 0 | 3687.0 | 10.239 | 13.1 | 0.0 | 82.1 | true |
| 1 | 32 | 3 | weir | 0 | 5216 | 5216 | 0 | 1038.4 | 36.863 | 1.2 | 0.0 | 83.0 | true |
| 1 | 256 | 1 | direct | 0 | 105471 | 105471 | 0 | 21053.7 | 18.431 | 60.1 | 0.0 | 83.0 | true |
| 1 | 256 | 1 | weir | 0 | 97920 | 97920 | 0 | 19552.6 | 20.479 | 8.1 | 0.0 | 82.0 | true |
| 1 | 256 | 2 | direct | 0 | 102849 | 102849 | 0 | 20538.5 | 18.431 | 61.8 | 0.0 | 82.7 | true |
| 1 | 256 | 2 | weir | 0 | 96256 | 96256 | 0 | 19203.3 | 20.479 | 11.0 | 0.0 | 82.0 | true |
| 1 | 256 | 3 | direct | 0 | 105248 | 105248 | 0 | 20999.0 | 18.431 | 64.2 | 0.0 | 83.8 | true |
| 1 | 256 | 3 | weir | 0 | 88274 | 88274 | 0 | 17556.4 | 24.575 | 10.5 | 0.0 | 81.5 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 161 | 32.000 | 5152 | 20.155 | 8.825 | 0 |
| 1 | 32 | 2 | 162 | 32.000 | 5184 | 20.236 | 8.642 | 0 |
| 1 | 32 | 3 | 163 | 32.000 | 5216 | 20.209 | 8.727 | 0 |
| 1 | 256 | 1 | 765 | 128.000 | 97920 | 1.327 | 9.162 | 0 |
| 1 | 256 | 2 | 752 | 128.000 | 96256 | 1.342 | 9.326 | 0 |
| 1 | 256 | 3 | 690 | 127.933 | 88274 | 1.548 | 10.138 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 20863.8 (256 workers), Weir 18769.3 (256 workers), observed change -10.04%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 3686.6 | 1032.4 | -72.00% | 9.215 / 10.239 / 11.263 | 32.767 / 36.863 / 36.863 |
| 256 | 20863.8 | 18769.3 | -10.04% | 12.287 / 18.431 / 36.863 | 13.311 / 20.479 / 28.671 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
