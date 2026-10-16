# Database saturation comparison

Backend **mongo**. Started 2026-10-16T10:47:37.853486Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 10%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 164307 | 18380 | 182687 | 0 | 36487.5 | 6.655 | 37.2 | 0.0 | 82.2 | true |
| 1 | 32 | 1 | weir | 48829 | 5386 | 54215 | 0 | 10806.3 | 20.479 | 7.5 | 0.0 | 82.2 | true |
| 1 | 32 | 2 | direct | 163320 | 18285 | 181605 | 0 | 36282.9 | 6.655 | 36.3 | 0.0 | 82.4 | true |
| 1 | 32 | 2 | weir | 52580 | 5814 | 58394 | 0 | 11660.6 | 18.431 | 7.7 | 0.0 | 82.2 | true |
| 1 | 32 | 3 | direct | 158413 | 17737 | 176150 | 0 | 35173.0 | 7.167 | 35.6 | 0.0 | 82.5 | true |
| 1 | 32 | 3 | weir | 49793 | 5494 | 55287 | 0 | 11016.5 | 18.431 | 7.4 | 0.0 | 82.3 | true |
| 1 | 256 | 1 | direct | 166352 | 18429 | 184781 | 0 | 36865.2 | 14.335 | 44.4 | 0.0 | 82.9 | true |
| 1 | 256 | 1 | weir | 120952 | 13350 | 134302 | 0 | 26801.7 | 26.623 | 4.8 | 0.0 | 82.4 | true |
| 1 | 256 | 2 | direct | 175150 | 19399 | 194549 | 0 | 38818.5 | 14.335 | 47.3 | 0.0 | 82.8 | true |
| 1 | 256 | 2 | weir | 121618 | 13432 | 135050 | 0 | 26945.2 | 26.623 | 5.0 | 0.0 | 82.3 | true |
| 1 | 256 | 3 | direct | 168673 | 18695 | 187368 | 0 | 37300.6 | 14.335 | 46.5 | 0.0 | 82.6 | true |
| 1 | 256 | 3 | weir | 120407 | 13296 | 133703 | 0 | 26678.0 | 26.623 | 4.9 | 0.0 | 82.3 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 31626 | 1.714 | 54215 | 1.088 | 0.431 | 0 |
| 1 | 32 | 2 | 34321 | 1.701 | 58394 | 1.082 | 0.377 | 0 |
| 1 | 32 | 3 | 32183 | 1.718 | 55287 | 1.091 | 0.386 | 0 |
| 1 | 256 | 1 | 7797 | 17.225 | 134302 | 1.620 | 2.131 | 0 |
| 1 | 256 | 2 | 7984 | 16.915 | 135050 | 1.591 | 2.133 | 0 |
| 1 | 256 | 3 | 7788 | 17.168 | 133703 | 1.621 | 2.163 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 37661.1 (256 workers), Weir 26808.3 (256 workers), observed change -28.82%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 35981.1 | 11160.8 | -68.98% | 0.239 / 7.167 / 8.191 | 1.151 / 18.431 / 26.623 |
| 256 | 37661.1 | 26808.3 | -28.82% | 7.167 / 14.335 / 18.431 | 7.679 / 26.623 / 36.863 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
