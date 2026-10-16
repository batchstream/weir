# Database saturation comparison

Backend **mongo**. Started 2026-10-16T11:16:36.711258Z.

independent OS client processes send one record per native call or streaming SDK RPC; native FindOne/ReplaceOne or single-ID mget/PUT; no client aggregation; worker-owned disjoint IDs; common phase barriers and unchanged pools across warmup and measurement; alternating AB/BA; latency is each business request including validation, queueing and any server aggregation; independent persisted postflight; Search uses explicit refresh_interval=1s, translog.durability=request, one primary and zero replicas; sustained database CPU requires >=5 intervals, >=80% measurement coverage, mean CPU>=threshold and CPU>=threshold during >=80% measurement duration in every round, plus <=10% next-level throughput gain.

Warmup 1s and measurement 5s per path/stage, 3 paired rounds, requested writes 10%.

Complete business-request budget for both paths: 10s.

4 independent OS client processes. Each worker sends one business request containing one record, waits for its acknowledged result, and validates it. p50/p95/p99 describe individual requests; histograms are pooled rather than averaging process quantiles.

| Batch | Workers | Round | Path | Read | Write | Success | Errors/unknown | Ops/s | p95 call ms | DB CPU budget % | Full CPU time % | Sampling coverage % | Verified |
| ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 32 | 1 | direct | 162324 | 18165 | 180489 | 0 | 36033.1 | 7.167 | 34.7 | 0.0 | 82.2 | true |
| 1 | 32 | 1 | weir | 82947 | 9236 | 92183 | 0 | 18404.9 | 8.191 | 13.3 | 0.0 | 82.6 | true |
| 1 | 32 | 2 | direct | 164943 | 18470 | 183413 | 0 | 36641.5 | 6.655 | 36.3 | 0.0 | 82.5 | true |
| 1 | 32 | 2 | weir | 82719 | 9218 | 91937 | 0 | 18359.4 | 8.191 | 13.4 | 0.0 | 82.4 | true |
| 1 | 32 | 3 | direct | 165305 | 18512 | 183817 | 0 | 36696.0 | 6.655 | 37.3 | 0.0 | 82.5 | true |
| 1 | 32 | 3 | weir | 83593 | 9304 | 92897 | 0 | 18553.7 | 8.191 | 14.0 | 0.0 | 82.4 | true |
| 1 | 256 | 1 | direct | 180425 | 20029 | 200454 | 0 | 39999.6 | 13.311 | 48.3 | 0.0 | 82.8 | true |
| 1 | 256 | 1 | weir | 137214 | 15160 | 152374 | 0 | 30423.5 | 15.359 | 6.1 | 0.0 | 82.6 | true |
| 1 | 256 | 2 | direct | 170623 | 18901 | 189524 | 0 | 37833.1 | 14.335 | 47.8 | 0.0 | 83.1 | true |
| 1 | 256 | 2 | weir | 136306 | 15075 | 151381 | 0 | 30163.5 | 16.383 | 6.5 | 0.0 | 82.7 | true |
| 1 | 256 | 3 | direct | 172175 | 19067 | 191242 | 0 | 38172.4 | 13.311 | 46.1 | 0.0 | 82.9 | true |
| 1 | 256 | 3 | weir | 133259 | 14736 | 147995 | 0 | 29544.4 | 16.383 | 6.6 | 0.0 | 82.6 | true |

Measured Weir adapter and streaming RPC evidence (warmup, reset and postflight excluded):

| Batch | Workers | Round | Adapter invocations | Mean adapter batch | Execute RPCs | Queue mean ms | Adapter mean ms | Rejections |
| ---: | ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: |
| 1 | 32 | 1 | 51121 | 1.803 | 92183 | 0.105 | 0.717 | 0 |
| 1 | 32 | 2 | 50751 | 1.812 | 91937 | 0.107 | 0.724 | 0 |
| 1 | 32 | 3 | 51928 | 1.789 | 92897 | 0.105 | 0.709 | 0 |
| 1 | 256 | 1 | 10343 | 14.732 | 152374 | 0.745 | 3.483 | 0 |
| 1 | 256 | 2 | 10393 | 14.566 | 151381 | 0.747 | 3.319 | 0 |
| 1 | 256 | 3 | 10245 | 14.446 | 147995 | 0.735 | 3.334 | 0 |

**Batch 1: database-full-load throughput comparison unavailable.** both paths must independently demonstrate sustained database CPU saturation and a verified next-level throughput plateau; increase concurrency or isolate client/Weir resources; I/O/network-bound saturation needs additional device/link capacity evidence.

Best verified business QPS within this ladder: direct 38668.5 (256 workers), Weir 30043.9 (256 workers), observed change -22.30%. This is a measured business-throughput comparison, not a maximum-database-capacity claim.

Observed single-request business performance at matched client concurrency (independent of the database-capacity qualification):

| Workers | Direct QPS | Weir QPS | Change | Direct p50/p95/p99 ms | Weir p50/p95/p99 ms |
| ---: | ---: | ---: | ---: | --- | --- |
| 32 | 36456.8 | 18439.3 | -49.42% | 0.223 / 7.167 / 8.191 | 1.023 / 8.191 / 10.239 |
| 256 | 38668.5 | 30043.9 | -22.30% | 7.167 / 13.311 / 18.431 | 8.191 / 16.383 / 20.479 |

These QPS and latency observations apply to this hardware and measured concurrency range. A business-throughput improvement can exist without demonstrating the maximum database capacity. An underfull database or missing next-level plateau keeps the database-capacity comparison unavailable.

Aggregation evidence: the JSON records one business request per record, Execute RPC counts, adapter invocation counts, average adapter batch and its cumulative histogram, and RPCs per adapter invocation. An adapter invocation is not necessarily one physical database wire command. MongoDB physical find/update/bulkWrite/getMore/killCursors/commitTransaction/abortTransaction deltas come from owned serverStatus at timed boundaries; missing counters remain unavailable. Elasticsearch has no equivalent physical HTTP-command counter. Every child PID, configured native pool limit, final revision ranges, exit result and sampled CPU/RSS is retained; coordinator sampling CPU is excluded from business-client CPU.

The JSON retains before/after raw public Weir metrics and their observed deltas, every resource sample, monitor failures, raw cumulative database/container I/O and network counters, CPU quota/host core denominator, and client/Weir cumulative CPU. Docker CPU uses cumulative Engine usage and read timestamps across each entire sampling interval, never CLI instantaneous percentages. Both interval endpoints must succeed; missing intervals reduce full-run coverage. Samples include monitoring traffic and background database work. Native ps CPU has platform-dependent clock resolution. Native databases share the host CPU budget with the client and Weir. Disk/network utilization or capacity is not inferred from byte counters. A plateau without sustained database CPU saturation is not a database-full-load result. Startup/discovery/reset/postflight and warmup are excluded from throughput; all in-flight completion and validation is included.

Resource observation schema 2 additionally retains bounded Linux host CPU ticks (busy, idle, iowait and steal, without double-counted guest ticks), CPU/IO/memory pressure, the sampler's allowed CPU list and runtime parallelism, Docker cumulative throttling counters, fixed sampled Store queue/execution gauges, and projected Search write-pool/indexing-pressure/GC statistics. Host/device evidence includes other runner work; sampler affinity is not a promise that all child processes have that affinity. Missing or unsupported fields remain explicitly unavailable. Pure Search stages retain before/after effective index settings and owned-index indexing/refresh/merge/flush/translog statistics, with explicit 1s refresh and request durability. Lucene flush statistics are not counts or timings of per-request translog fsync. No added metric alone establishes database or storage capacity.
