# Database saturation comparison

Backend **search**. Started 2026-10-16T10:51:35.101532Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 2s and measurement 5s per path/stage, 3 paired rounds, requested writes 0%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 18573 | 0 | 18573 | 0 | 3662.4 | 65.535 | 104.4 | 85.4 | 85.4 | true |
| 1 | 32 | 1 | weir | 10455 | 0 | 10455 | 0 | 2089.3 | 73.727 | 102.3 | 83.9 | 83.9 | true |
| 1 | 32 | 2 | direct | 59556 | 0 | 59556 | 0 | 11900.9 | 4.095 | 100.3 | 83.6 | 83.6 | true |
| 1 | 32 | 2 | weir | 13731 | 0 | 13731 | 0 | 2743.3 | 65.535 | 100.5 | 83.8 | 83.8 | true |
| 1 | 32 | 3 | direct | 63153 | 0 | 63153 | 0 | 12626.5 | 3.839 | 101.0 | 82.9 | 82.9 | true |
| 1 | 32 | 3 | weir | 36124 | 0 | 36124 | 0 | 7220.0 | 8.191 | 97.3 | 83.2 | 83.2 | true |
| 1 | 256 | 1 | direct | 61151 | 0 | 61151 | 0 | 12212.8 | 57.343 | 101.3 | 84.7 | 84.7 | true |
| 1 | 256 | 1 | weir | 79663 | 0 | 79663 | 0 | 15908.7 | 36.863 | 79.1 | 0.0 | 83.9 | true |
| 1 | 256 | 2 | direct | 64663 | 0 | 64663 | 0 | 12912.6 | 57.343 | 100.6 | 83.5 | 83.5 | true |
| 1 | 256 | 2 | weir | 76562 | 0 | 76562 | 0 | 15273.9 | 36.863 | 78.6 | 0.0 | 83.1 | true |
| 1 | 256 | 3 | direct | 64477 | 0 | 64477 | 0 | 12870.4 | 57.343 | 100.7 | 85.2 | 85.2 | true |
| 1 | 256 | 3 | weir | 78370 | 0 | 78370 | 0 | 15646.5 | 36.863 | 78.8 | 0.0 | 83.1 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 5584 | 1.872 | 10455 | 0.887 | 13.289 | 0 |
| 1 | 32 | 2 | 7414 | 1.852 | 13731 | 0.767 | 10.051 | 0 |
| 1 | 32 | 3 | 20013 | 1.805 | 36124 | 0.141 | 3.391 | 0 |
| 1 | 256 | 1 | 10255 | 7.768 | 79663 | 0.516 | 6.233 | 0 |
| 1 | 256 | 2 | 9822 | 7.795 | 76562 | 0.601 | 6.519 | 0 |
| 1 | 256 | 3 | 10045 | 7.802 | 78370 | 0.512 | 6.208 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 12665.3 (256 workers), Weir 15609.6 (256 workers), observed change +23.25%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 9370.6 | 4017.3 | -57.13% | 1.663 / 6.655 / 61.439 | 4.607 / 53.247 / 73.727 |
| 256 | 12665.3 | 15609.6 | +23.25% | 13.311 / 57.343 / 73.727 | 14.335 / 36.863 / 49.151 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
