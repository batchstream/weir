# Performance

Why Weir helps many concurrent writers, and the measurements behind that claim.

Fleets of workers, such as distributed crawlers, often write each result as its
own request, and every worker process keeps its own connection pool. Routed
through Weir, those writers share a small set of backend connections per Store,
and their concurrent single-record writes become native MongoDB `bulkWrite` and
Search `_bulk` calls.

On EKS, 16 client processes sent single 1 KiB upserts to a 2-vCPU database,
directly or through one Weir server on its own node (medians of three rounds,
MongoDB with `w:majority`):

| Concurrent writers | MongoDB ops/s, direct → Weir | Elasticsearch ops/s, direct → Weir |
| --: | --- | --- |
| 64 | 7.8k → 9.7k (1.2×) | 9.3k → 9.1k (0.98×) |
| 256 | 6.1k → 14.8k (2.4×) | 24.5k → 27.9k (1.1×) |
| 1,024 | 4.5k → 11.6k (2.6×) | 22.6k → 64.7k (2.9×) |
| 2,048 | 4.3k → 11.9k (2.8×) | 22.3k → 87.5k (3.9×) |

At 2,048 writers, direct writes held about 2,100 database connections against
about 20 for one Weir Store, and median latency was 3.7–4.8× lower. Across
writer counts, database CPU per write fell to 22–43% of the direct cost.
Counting Weir's own CPU (about half a core for MongoDB, under three cores at 87k
Elasticsearch writes/s), total CPU per write fell by about half for MongoDB; for
Elasticsearch it was 11% higher at 64 writers, 16% lower at 256 and 38–41% lower
from 1,024. With few writers, throughput stays close to direct writes and median
latency can rise by about 2 ms; the gain grows with the number of concurrent
writers. The workload tools live in
[weir-tests](https://github.com/batchstream/weir-tests).
