# Database saturation comparison

Backend **search**. Started 2026-10-16T11:18:17.110426Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 2s and measurement 5s per path/stage, 3 paired rounds, requested writes 100%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 0 | 5588 | 5588 | 0 | 1112.5 | 53.247 | 98.2 | 83.4 | 83.4 | true |
| 1 | 32 | 1 | weir | 0 | 5760 | 5760 | 0 | 1146.6 | 32.767 | 36.8 | 0.0 | 82.3 | true |
| 1 | 32 | 2 | direct | 0 | 7512 | 7512 | 0 | 1495.9 | 24.575 | 34.7 | 0.0 | 82.3 | true |
| 1 | 32 | 2 | weir | 0 | 5696 | 5696 | 0 | 1137.8 | 32.767 | 31.2 | 0.0 | 83.1 | true |
| 1 | 32 | 3 | direct | 0 | 7413 | 7413 | 0 | 1476.1 | 24.575 | 21.9 | 0.0 | 82.0 | true |
| 1 | 32 | 3 | weir | 0 | 5632 | 5632 | 0 | 1121.1 | 36.863 | 19.8 | 0.0 | 82.9 | true |
| 1 | 256 | 1 | direct | 0 | 7532 | 7532 | 0 | 1460.5 | 245.759 | 36.0 | 0.0 | 99.7 | true |
| 1 | 256 | 1 | weir | 0 | 91392 | 91392 | 0 | 18253.3 | 20.479 | 58.0 | 0.0 | 82.6 | true |
| 1 | 256 | 2 | direct | 0 | 7662 | 7662 | 0 | 1487.1 | 196.607 | 31.3 | 0.0 | 80.0 | true |
| 1 | 256 | 2 | weir | 0 | 99712 | 99712 | 0 | 19909.2 | 18.431 | 49.4 | 0.0 | 82.6 | true |
| 1 | 256 | 3 | direct | 0 | 7390 | 7390 | 0 | 1429.3 | 212.991 | 25.7 | 0.0 | 99.4 | true |
| 1 | 256 | 3 | weir | 0 | 95552 | 95552 | 0 | 19019.0 | 18.431 | 50.1 | 0.0 | 82.4 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 180 | 32.000 | 5760 | 19.975 | 5.107 | 0 |
| 1 | 32 | 2 | 178 | 32.000 | 5696 | 19.996 | 5.453 | 0 |
| 1 | 32 | 3 | 176 | 32.000 | 5632 | 19.979 | 5.790 | 0 |
| 1 | 256 | 1 | 714 | 128.000 | 91392 | 2.028 | 8.285 | 0 |
| 1 | 256 | 2 | 779 | 128.000 | 99712 | 1.982 | 7.417 | 0 |
| 1 | 256 | 3 | 747 | 127.914 | 95552 | 1.933 | 7.873 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 1458.9 (256 workers), Weir 19060.5 (256 workers), observed change +1206.49%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 1361.5 | 1135.2 | -16.62% | 22.527 / 32.767 / 57.343 | 28.671 / 32.767 / 36.863 |
| 256 | 1458.9 | 19060.5 | +1206.49% | 180.223 / 196.607 / 262.143 | 13.311 / 18.431 / 40.959 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
