# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:00:10.614638Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 0%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 223057 | 0 | 223057 | 0 | 44602.7 | 1.279 | 45.6 | 0.0 | 82.6 | true |
| 1 | 32 | 1 | weir | 113079 | 0 | 113079 | 0 | 22610.6 | 2.815 | 10.6 | 0.0 | 82.4 | true |
| 1 | 32 | 2 | direct | 233739 | 0 | 233739 | 0 | 46739.3 | 1.151 | 45.8 | 0.0 | 82.8 | true |
| 1 | 32 | 2 | weir | 112959 | 0 | 112959 | 0 | 22586.8 | 2.815 | 10.7 | 0.0 | 82.3 | true |
| 1 | 32 | 3 | direct | 225833 | 0 | 225833 | 0 | 45157.5 | 1.279 | 44.9 | 0.0 | 83.1 | true |
| 1 | 32 | 3 | weir | 113116 | 0 | 113116 | 0 | 22616.8 | 2.815 | 10.6 | 0.0 | 82.4 | true |
| 1 | 256 | 1 | direct | 209635 | 0 | 209635 | 0 | 41883.9 | 11.263 | 44.0 | 0.0 | 82.8 | true |
| 1 | 256 | 1 | weir | 171101 | 0 | 171101 | 0 | 34187.8 | 14.335 | 3.4 | 0.0 | 82.4 | true |
| 1 | 256 | 2 | direct | 216975 | 0 | 216975 | 0 | 43337.7 | 11.263 | 45.4 | 0.0 | 82.9 | true |
| 1 | 256 | 2 | weir | 171570 | 0 | 171570 | 0 | 34281.2 | 14.335 | 3.5 | 0.0 | 82.5 | true |
| 1 | 256 | 3 | direct | 227049 | 0 | 227049 | 0 | 45353.9 | 10.239 | 45.6 | 0.0 | 82.8 | true |
| 1 | 256 | 3 | weir | 169285 | 0 | 169285 | 0 | 33830.9 | 14.335 | 3.4 | 0.0 | 82.6 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 45778 | 2.470 | 113079 | 0.060 | 0.429 | 0 |
| 1 | 32 | 2 | 46322 | 2.439 | 112959 | 0.059 | 0.435 | 0 |
| 1 | 32 | 3 | 46153 | 2.451 | 113116 | 0.059 | 0.426 | 0 |
| 1 | 256 | 1 | 9341 | 18.317 | 171101 | 0.668 | 1.724 | 0 |
| 1 | 256 | 2 | 9297 | 18.454 | 171570 | 0.657 | 1.754 | 0 |
| 1 | 256 | 3 | 9337 | 18.131 | 169285 | 0.679 | 1.750 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 45499.8 (32 workers), Weir 34100.0 (256 workers), observed change -25.05%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 45499.8 | 22604.7 | -50.32% | 0.767 / 1.279 / 1.535 | 1.407 / 2.815 / 3.583 |
| 256 | 43525.3 | 34100.0 | -21.65% | 7.167 / 11.263 / 13.311 | 7.167 / 14.335 / 18.431 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
