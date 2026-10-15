# Batching and workload coverage

Weir aggregates independent single-record callers at each Store. Useful batching
requires compatible arrivals, a bounded collection delay and appropriate execution
concurrency. Increasing only `max_operations` cannot create arrivals that do not
exist. Each Search index and visibility mode has its own compatibility group;
dependent business phases and shared-key writes further limit aggregation.

## Flush and admission boundaries

- `batching.max_operations` limits selected operations.
- `batching.max_bytes` limits the retained-input estimate and the final Search
  NDJSON body, including action metadata and newlines. Generated Lua output is
  measured after encoding. Search also uses `backend.max_exchange_bytes`; the
  smaller write aggregation target wins. A single legal oversized item goes alone.
- `batching.max_wait` bounds write collection from the oldest eligible admission. It
  overlaps waiting for an execution slot and is not restarted when a slot opens.
- Reads dispatch with compatible work already queued, without waiting for future
  arrivals. A mixed batch containing a read also bypasses collection.
- Full batches, drain, Native/Scan and shorter caller deadlines bypass collection.
- A positive `batching.max_in_flight` bounds simultaneous adapter invocations.
  Excess work stays in the bounded queue, where it can combine or be canceled.
  Completed execution releases the slot independently of response acknowledgement.
  Omitting this field selects its default; an explicit `0` means unlimited.

Defaults are 128 operations, 4 MiB, a 20 ms write collection window and 16 adapter
invocations per Store. Explicit `max_wait: 0s` disables collection; explicit
`max_in_flight: 0` removes the execution limit. Reads remain eligible immediately.
The existing Store queue is the waiting boundary; no extra HTTP queue or worker
process is introduced.

These values come from the initial preregistered selection among 20
count/wait/concurrency configurations and a four-point byte anchor in the
[2026-10-16 tuning evidence](https://github.com/batchstream/weir-tests/blob/2030036091b1f9c76e32c7b8c8f1dc5124441789/results/default-tuning-2026-10-16/README.md),
with the [selection audit retained at an immutable revision](https://github.com/batchstream/weir-tests/blob/3f47919befc8db3f8e612cc7e0f17feda16ad912/results/default-tuning-2026-10-16/analysis-selection-parser-fix/analysis.json).
They are a finite workload choice, not a globally optimal value for each control.
The selection rules were frozen before selection measurements. Actual
default-path fixtures omit batching entirely and cross-check effective settings
with all four runtime gauges.

The later preregistered [20/40/80/160 ms study](https://github.com/batchstream/weir-tests/blob/2030036091b1f9c76e32c7b8c8f1dc5124441789/results/default-tuning-2026-10-16/WAIT-SENSITIVITY-RESULT.md)
holds count, bytes and executions fixed. Its result is inconclusive: the repeated
20 ms crawler anchor fails backlog stability in round 3, and the 160 ms crawler's
first round exceeds the three-second drain limit. Although 40 and 80 ms meet
their individual gates, the registered comparison requires a qualified anchor.
It proposes no new wait and does not prove that 20 ms beats the longer waits.
The current 20 ms default remains the original finite selection.

The [independent confirmation results](value-proof.md#default-selection-and-independent-confirmation-2026-10-16)
retain all 12 passed read guards against legacy Weir, the crawler CPU/latency
tradeoff, MongoDB normalized comparisons and failed native partial-update cases. At a database
quota of 0.25 CPU and 512 workers, pure Search writes complete 6.74–8.34 times
native throughput at nearly the same measured database CPU in all three rounds.
At comparable completed crawler throughput, actual database CPU falls by
8.1–42.3%. These pure-write and crawler points pass the declared five-second
p99 gate; higher crawler tail latency remains part of the result. Passing p99
alone does not establish stable arrivals, acceptable drain or maximum capacity.
The 64 KiB payload case observes 47.73–48.82 records per bulk and framed body
means of 2.990–3.059 MiB; its throughput gain accompanies higher actual database
CPU. Native access remains faster in the tested Search read cases and MongoDB
reads with four workers. The final registered longer-window capacity study
remains pending.
This finite selection does not establish a universal optimum or maximum
sustainable throughput, and the five-second gate is not a general service
guarantee.

An execution slot includes metadata checks, pre-reads, Lua, writes and any requested
visibility wait. A tiny limit can leave the database idle while writes wait for
refresh. More slots can increase small concurrent requests and memory consumption.
Choose the limit with the real completion mode and document sizes.

## Calibrate count, bytes, wait and concurrency separately

Use native single-record clients and the same single-record traffic through Weir.
Keep durability, query visibility, retry budgets and deadlines equal. Start with a
small bounded set of doubled batch limits, then vary execution concurrency and
collection delay independently around the selected point. Prefer the smaller
setting when throughput has plateaued. Elastic's [indexing guidance](https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed)
recommends measuring both bulk size and concurrency rather than adopting one
universal batch size. Its [refresh guidance](https://www.elastic.co/docs/reference/elasticsearch/rest-apis/refresh-parameter)
also explains why query-visible writes should share bulk requests.

Record completed throughput, scheduled-to-completed tail latency, rejection,
timeouts, drain, actual database CPU per completed unit, and actual bulk body
bytes/record distributions. `weir_search_http_request_body_bytes` observes framed
attempted request bodies, including failures; it excludes headers and does not
prove delivery or acknowledgement. Adapter batch size alone does not describe
the database requests: bounded mget chunks and full-source Lua replacements can
amplify reads and bytes even when bulk call counts fall.

Do not equate equal CPU quotas with equal actual load. Lower CPU at comparable
completed throughput does not require database saturation. A capacity claim needs
an independently identified database bottleneck, sufficient load-generator
headroom and a repeatable throughput plateau. Reject measurements with unknown
writes, dropped arrivals, excessive generator lag or unmatched persisted fields.

## Coverage boundaries

| Scenario | Required evidence |
| --- | --- |
| Pure indexing with application acknowledgement | Single-record native/Weir callers, changed revisions, payload size, full persisted oracle and actual database CPU |
| Small partial updates to larger documents | Identical patch, matching acknowledgement mode, untouched fields retained, explicit postflight refresh before search verification |
| Dependent crawler Lua updates with query visibility | Original phase dependencies, all-arrivals latency, mget/bulk/refresh counts and full business-field oracle |
| Mixed small/large generated documents | Real NDJSON framing, byte-boundary splits and isolated single large entries |
| Many indexes with skewed arrivals | Separate target results, eligible cold targets progress, actual per-request grouping |
| Shared-key increments | Persisted counter and native version match APPLIED outcomes; known conflicts are accounted for and uncertain work is not replayed |
| Sparse and burst arrivals, slow database | Oldest-admission wait bound, no second collection delay after slot waiting, finite executions, cancellation, queue backpressure and drain |

The last four are correctness and scheduling boundaries as well as possible
performance workloads. Passing an integration test does not establish a
throughput gain for that distribution. The real Search suite exercises skewed
eight-index traffic, 1/256 KiB sources and shared increments through four gRPC
connections with four execution slots and a 20 ms collection window. It is an
owned loopback correctness fixture, not a distributed-network benchmark.

The selector microbenchmark drains 1024 queued operations over 1, 8, 64 and 1024
compatibility groups. When the oldest group is already eligible, the selector
avoids constructing groups that cannot win that dispatch. It still scans and
compacts the bounded queue; the high-cardinality worst case remains quadratic.

The [executed batching coverage report](https://github.com/batchstream/weir-tests/blob/17ab0506defcdb9f374aa994e7bb4467d43ebf82/results/batch-coverage-2026-10-15/README.md)
retains count/wait/concurrency calibration, all three formal workloads and their
negative observations, raw receipts and independent audits. Its fixed candidate
was an explicit workload experiment and retained the defaults in effect on
2026-10-15. The later constrained selection described above determines the current
defaults.
