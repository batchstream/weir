# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:01:30.284099Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 10%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 163898 | 18332 | 182230 | 0 | 36412.8 | 6.655 | 36.5 | 0.0 | 82.4 | true |
| 1 | 32 | 1 | weir | 55660 | 6156 | 61816 | 0 | 12315.7 | 20.479 | 8.7 | 0.0 | 82.2 | true |
| 1 | 32 | 2 | direct | 166642 | 18659 | 185301 | 0 | 37017.8 | 6.655 | 37.0 | 0.0 | 82.4 | true |
| 1 | 32 | 2 | weir | 57051 | 6307 | 63358 | 0 | 12630.1 | 18.431 | 8.7 | 0.0 | 82.0 | true |
| 1 | 32 | 3 | direct | 164664 | 18428 | 183092 | 0 | 36586.2 | 6.655 | 36.8 | 0.0 | 82.7 | true |
| 1 | 32 | 3 | weir | 57009 | 6313 | 63322 | 0 | 12624.5 | 18.431 | 8.5 | 0.0 | 82.2 | true |
| 1 | 256 | 1 | direct | 176530 | 19567 | 196097 | 0 | 39139.4 | 13.311 | 46.9 | 0.0 | 82.8 | true |
| 1 | 256 | 1 | weir | 132074 | 14582 | 146656 | 0 | 29259.7 | 24.575 | 4.7 | 0.0 | 82.5 | true |
| 1 | 256 | 2 | direct | 182645 | 20282 | 202927 | 0 | 40480.4 | 13.311 | 48.5 | 0.0 | 82.9 | true |
| 1 | 256 | 2 | weir | 128143 | 14193 | 142336 | 0 | 28392.7 | 26.623 | 5.4 | 0.0 | 82.6 | true |
| 1 | 256 | 3 | direct | 176096 | 19503 | 195599 | 0 | 39011.7 | 14.335 | 48.2 | 0.0 | 82.7 | true |
| 1 | 256 | 3 | weir | 135229 | 14952 | 150181 | 0 | 29913.2 | 26.623 | 5.5 | 0.0 | 82.4 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 37269 | 1.659 | 61816 | 1.218 | 0.291 | 0 |
| 1 | 32 | 2 | 38242 | 1.657 | 63358 | 1.143 | 0.291 | 0 |
| 1 | 32 | 3 | 38185 | 1.658 | 63322 | 1.152 | 0.287 | 0 |
| 1 | 256 | 1 | 9773 | 15.006 | 146656 | 1.571 | 2.118 | 0 |
| 1 | 256 | 2 | 9117 | 15.612 | 142336 | 1.618 | 1.859 | 0 |
| 1 | 256 | 3 | 9567 | 15.698 | 150181 | 1.570 | 1.755 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 39543.9 (256 workers), Weir 29188.9 (256 workers), observed change -26.19%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 36672.3 | 12523.4 | -65.85% | 0.223 / 6.655 / 8.191 | 0.831 / 20.479 / 26.623 |
| 256 | 39543.9 | 29188.9 | -26.19% | 7.167 / 13.311 / 18.431 | 7.167 / 26.623 / 36.863 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
