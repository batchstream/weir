# Store batching performance, 2026-10-16

This study measures independent, synchronous, single-record callers. Each caller
submits one operation, waits for its result and acknowledges it before sending
the next. No caller constructs a batch. The Store performs aggregation.

The accepted revision is **VII**. Store scheduling is **1.76–10.45x faster**
for the measured drains, and all **25 Store scenarios allocate fewer objects
and bytes**. VII also fixes the reproduced shared-plan reservation accounting
race, with unchanged per-operation allocations in the VI/VII controls.

These Store results use an instrumented fake adapter; the Search mechanism
comparison uses a loopback HTTP fixture. Neither establishes real database CPU
or capacity. See the separate [real MongoDB/Search report](batching-results-2026-10-16/real-experiments.md)
for actual multiprocess throughput, commands, CPU and measurement exclusions.
The complete Store matrix is available as [CSV](batching-results-2026-10-16/summary.csv)
and [JSON](batching-results-2026-10-16/summary.json).

Real confirmation uses four client OS processes and one record per call. At
256 Elasticsearch readers, observed throughput rises from **15,610 to 18,479
operations/s** against the old Weir, with about **36% less estimated database
CPU per completed unit**. The current native/Weir matrix completes **84 timed
stages and 8,208,279 successful operations**, with persistence checks passing
and no errors, timeouts or indeterminate writes. Local MongoDB reads and mixed
traffic remain slower than direct access; the 1 ms write profile has a measured
throughput/CPU tradeoff. Those limitations and all raw rounds remain in the
real database report.

## Measured Store results

All raw Go benchmark output and dependency-free comparison code are retained in
[`batching-results-2026-10-16`](batching-results-2026-10-16/).
The accepted 25-case comparison is available as
[`summary.csv`](batching-results-2026-10-16/summary.csv) and
[`summary.json`](batching-results-2026-10-16/summary.json), including both raw
source filenames and sample counts for every row.

The selector drains 1,024 already admitted records, with identical physical
batch counts on both revisions:

| Namespaces | Baseline drain | Candidate drain | Speedup | Allocations/drain |
| --- | ---: | ---: | ---: | ---: |
| 1 | 310.6 us | 176.1 us | 1.76x | 769 → 546 |
| 8 | 396.7 us | 181.2 us | 2.19x | 769 → 553 |
| 64 | 940.5 us | 214.6 us | 4.38x | 1,345 → 981 |
| 1,024 | 9,634.8 us | 922.1 us | 10.45x | 9,217 → 6,465 |

Selected end-to-end runtime cases, using the newly captured adjacent baseline:

| Case | Baseline ops/s | Candidate ops/s | Allocations/op | Bytes allocated/op |
| --- | ---: | ---: | ---: | ---: |
| Read, 1 caller | 4,150 | 3,982 | 32 → 27 | 2,485 → 2,062 |
| Read, 32 callers | 8,308 | 8,012 | 23 → 19 | 1,859 → 1,571 |
| Read, 256 callers | 66,007 | 65,096 | 14 → 12 | 1,408 → 1,121 |
| Write, 32 callers | 58,972 | 58,799 | 20 → 14 | 2,283 → 1,559 |
| Write, 256 callers | 230,133 | 269,296 | 18 → 14 | 2,185 → 1,771 |
| Mixed, 256 callers | 64,624 | 63,994 | 14 → 12 | 1,409 → 1,125 |
| Mixed, 64 namespaces | 12,113 | 12,270 | 20 → 17 | 1,681 → 1,409 |
| Mixed, 256 namespaces | 4,199 | 4,222 | 32 → 27 | 2,551 → 2,118 |
| Hot-key writes, 256 callers | 4,252 | 4,230 | 32 → 27 | 2,546 → 2,112 |
| Mixed, 32 callers, 20-ms window | 4,750 | 4,480 | 38 → 24 | 3,831 → 2,102 |
| Fixed cost 0, per-item 2 us | 319,099 | 347,552 | 15 → 12 | 1,488 → 1,151 |
| Batch-size-one control | 4,225 | 4,240 | 32 → 27 | 2,543 → 2,125 |

All 25 cases reduce both allocations and allocated bytes. The fixed-cost-zero
case improved throughput 8.9%, and 256-caller writes improved 17.0%, with
operations per invocation rising from 71.39 to 82.3. Most timer-bound cases
remain within approximately ±6% of their paired baseline, including slower
cases shown above; the data does not establish a universal throughput gain.

Tail latency is retained for every runtime row. The write-256 p99 bucket upper
bound changed from 4.19 ms to 2.10 ms. In the batch-size-one control, mean latency
was 52.8 ms on both versions and the p99 upper bound remained 67.1 ms. These
power-of-two histograms cannot establish a precise small tail-latency difference.

## Reproduction

Host: Apple M2, macOS arm64, 8 logical CPUs, 24 GiB RAM, Go 1.27.1.
Baseline source: `30ac2d10e8ec85a94512036abada5041b39ea606`.
The benchmark files in this change were added to that baseline before freezing
the test binary. Baseline `internal/store/runtime.go` SHA256:
`02814d0187bd3de8be7ffe3e725e2d7ce2dab45b7653b3a3c6c54c8f2e558837`.
Baseline test binary SHA256:
`2870e9e0a53e6ef68f643c57e3bb0b768ddb3849d6b79e584bcb6c7e7b3b1f8f`.
Accepted candidate: iteration VII, runtime SHA256
`4332d31167075f213521a557ac217595520d8aca19ede51a53f7c8ba417bb300`,
test binary SHA256
`99bf26e34469b1121bcebb2ccd173f829071ad1ad5692c94a0609aa1d1a5cd76`.
Both runtime source snapshots, the raw-output checksums, exact commands and
measurement exclusions are retained in the result directory's
[`manifest.json`](batching-results-2026-10-16/manifest.json).

Run against each revision, preserving its output:

```sh
go test -c ./internal/store -o /tmp/weir-batching.test
/tmp/weir-batching.test -test.run '^$' -test.bench '^BenchmarkSelectorDrain$' \
  -test.benchtime=150ms -test.count=3 -test.benchmem > selector.txt
/tmp/weir-batching.test -test.run '^$' -test.bench '^BenchmarkRuntimeFragmented$' \
  -test.benchtime=250ms -test.count=3 -test.benchmem > runtime.txt
python3 docs/batching-results-2026-10-16/compare.py before.txt after.txt > comparison.json
python3 docs/batching-results-2026-10-16/compare.py before.txt after.txt --format csv > comparison.csv
```

To reproduce the baseline, check out the baseline revision separately and copy
only `internal/store/runtime_benchmark_test.go` from this change into that
checkout. Its existing selector benchmark already has the correct old setup.
Compile and run inside that checkout. Do not copy the candidate runtime or
candidate selector setup into the baseline.

The baseline selector setup appends directly to the old pending slice. The
candidate setup uses the new enqueue helper to maintain the FIFO links. Setup
is excluded from selector timing on both revisions; selector execution and all
allocation counts during selection are included. The runtime benchmark uses
the public Store admission/result lifecycle on both revisions.

## Workloads and metrics

The 21 runtime scenarios include reads, writes and 90/10 mixed traffic at 1,
32 and 256 callers; 8/64/256 namespaces at 256 callers; a single hot key for
reads and writes; 64-byte and 64-KiB documents; fixed backend cost 0/200 us/2 ms;
a per-item 2 us cost; collection windows 0/250 us/20 ms; and a batch-size-one
control. Default execution concurrency is 16, batch operations 128, batch bytes
4 MiB. Runtime measurements use a 4,096-operation/512-MiB waiting queue to keep
admission rejection out of the comparison.

The fake backend serializes its service phase using a mutex and sleeps for
`fixed + items * perItem`. All cases except `cost` use a 200 us fixed cost and
zero per-item cost. It releases the mutex before emitting individual results.
This models command overhead on a constrained service, including the benefit
of fewer commands. It is not a database simulator and cannot establish database
CPU, network usage or maximum production throughput. Timer precision and host
load affect the measured sleep cost.

`operations/s` counts successful logical operations; `ops/call` and `calls/op`
measure aggregation and adapter invocation reduction. `mean-us` is the observed
end-to-end Submit/Wait/Ack mean. `p50-upper-us` and `p99-upper-us` are upper
bounds from power-of-two nanosecond histogram buckets, not exact quantiles.
The benchmark fails if admission or completion fails or backend completed-item
counts differ from requested operations. `B/op` and `allocs/op` include Store
work, timers and fake adapter execution; preparation and fixture setup are
excluded. Reported comparisons are medians of three independent benchmark
measurements and are descriptive, not significance estimates.

The fake adapter reuses immutable response documents. Payload sizes exercise
protobuf sizing and Store accounting but exclude encoding, copying, backend
document processing and network transfer. Results across payload sizes must
not be interpreted as real large-document throughput comparisons. The hot-write
case intentionally cannot combine duplicate mutations of one key in a single
physical invocation. Namespace cases preserve distinct BatchKeys, isolating
Store scheduling from adapter-specific cross-namespace aggregation.

## Tuning and measurement controls

The first selector candidate was measured while a separate real MongoDB
experiment could be active; its file is explicitly marked `with-mongo-interference`.
It is retained for transparency and excluded from accepted final comparisons.

Tuning was iterative rather than accepting the first static improvement:

1. Caching all candidate groups accelerated a static 1,024-namespace drain but
   repeatedly rebuilt hundreds of groups under new arrivals. Dynamic namespace
   allocations rose from 20 to 106 per operation at 64 namespaces and from 32
   to 453 at 256 namespaces. This version was rejected.
2. Refilling only a dispatched group removed repeated full-round rebuilding on
   static drains. A second full runtime run was deliberately interrupted to
   avoid overlapping the independent real-database measurements; that file is
   marked `interrupted` and is excluded from comparisons.
3. Bounding ready groups by available execution slots removed the dynamic
   namespace allocation explosion. Independent review found that a collecting
   write could hide a ready read omitted by that bound. A deterministic
   regression test now requires the read to dispatch after a slot is released
   without waiting for the write's collection window.
4. Notifications were coalesced around completed batches and queued cancellation;
   acknowledging an already completed result no longer wakes admission. Common
   dynamic allocations fell, but the matrix still exposed unnecessary pending
   array allocations in the batch-size-one and hot-write controls. These cases
   remain explicit tuning checks, rather than being dropped from the report.
5. A one-operation limit now uses the direct FIFO path, and candidate item arrays
   grow with selected records. This fixed the unbatched control but still left
   the hot-write case allocating 4,954 bytes per operation, versus 2,541 in its
   original baseline. This version was rejected.
6. Candidate backlogs now link existing tickets instead of allocating a fresh
   slice for the whole pending queue. The accepted version reduced hot-write
   allocation to 2,111 bytes per operation against its adjacent 2,557-byte
   baseline, and both adverse controls now reduce allocations as well.
7. Final reliability review found a pre-existing shared-plan accounting race:
   concurrent submissions reusing one prepared Plan could overwrite each other's
   result reservation. Each admitted ticket now owns a private Plan and context.
   The copy moved from dispatch to admission, so successful operations retain
   one Plan allocation. Same-batch and 64-concurrent-batch regressions pass with
   the fix; the coordinator reproduced failure and a race against the original
   baseline in [`baseline-plan-regression.txt`](batching-results-2026-10-16/baseline-plan-regression.txt).
   Adjacent VI/VII controls retain exactly 27/12/27 allocations per operation
   for single reads, 256-caller reads and the unbatched control. The complete VI
   performance evidence is frozen in [`iteration6`](batching-results-2026-10-16/iteration6/).

The comparison script rejects incomplete output or mismatched benchmark sets.
Thus an interrupted or partial run cannot silently replace the complete matrix.

The first baseline was captured earlier than later candidates. Most timer-bound
cases then appeared about 11% slower, including a single caller that performs
no aggregation. A later isolated ABBA check reproduced the slowdown in the
unchanged baseline itself: single-read baseline A measured 265.5/265.1 us while
candidate B measured 265.0/270.6 us. At 256 read callers, A achieved 59.6/60.1k
operations/s and B 59.5/58.9k; the batch-size-one control achieved A 3,852/3,877
and B 3,891/3,885 operations/s. This showed a timing-period effect rather than
an implementation-wide throughput regression. The final full matrix therefore
uses newly captured, adjacent baseline and candidate runs; the original runs
and all four ABBA files remain available. No throughput claim uses the earlier
baseline against the later final candidate.

## Search adapter HTTP mechanism comparison

The frozen candidate also compares two grouping policies through an actual
loopback HTTP fixture returning 1-KiB documents. The comparison policy models
the previous per-index, seven-document exchange window without read deduplication;
the coalesced policy uses the current adapter. Both execute current code in
one binary. This is not an old-server versus new-server measurement, and the
fixture performs no database work.

Each operation below means one complete input batch containing the stated
number of records. Medians use three 1-second measurements per policy. Raw
output is [`final-backend-http.txt`](batching-results-2026-10-16/final-backend-http.txt),
with parsed [`JSON`](batching-results-2026-10-16/backend-http-comparison.json)
and [`CSV`](batching-results-2026-10-16/backend-http-comparison.csv).

| Input batch | HTTP exchanges/batch | Time/batch, before → after | Allocated bytes/batch, before → after |
| --- | ---: | ---: | ---: |
| 1 record | 1 → 1 | 66.7 → 66.8 us | 24,824 → 24,912 |
| 32 distinct records, 1 index | 5 → 1 | 801.9 → 625.5 us | 506,614 → 529,583 |
| 32 reads of one record | 5 → 1 | 796.4 → 68.9 us | 507,380 → 28,855 |
| 32 distinct records, 8 indexes | 8 → 1 | 973.2 → 641.8 us | 597,013 → 535,434 |
| 128 reads, one hot record per 8 indexes | 24 → 1 | 3,427.5 → 200.4 us | 2,067,895 → 155,598 |

The single-record control remains effectively unchanged. Distinct records in
one index reduce exchanges and time but allocate 4.5% more bytes in this
fixture; the result is retained as a cost, not described as a memory win.
Hot-read cases benefit from fetching each unique resource once and sharing its
immutable result across callers. These measurements establish HTTP request
coalescing and adapter overhead effects. Actual database CPU and end-to-end
multiprocess benefits are reported separately below.

## Real database validation boundary

The independent real MongoDB/Search matrix, actual database command counts,
CPU per completed operation and measurement exclusions are in
[`real-experiments.md`](batching-results-2026-10-16/real-experiments.md).

The sibling `weir-tests` project provides an independently owned blackbox lab:
four OS client processes, one record per native driver/SDK call, alternating
native/Weir rounds, database command counters, CPU/RSS samples and persistence
checks. Its `.tools/mongod` and `.tools/performance-lab` were present and Docker
was available with no running containers at discovery. No sibling source or
results were edited by this Store benchmark work.

Real MongoDB comparisons are run separately by the coordinating agent with an
isolated fixture and frozen baseline/candidate server binaries. Only that
evidence can support actual database command/CPU claims. macOS native MongoDB
results describe this host; they do not establish Linux CPU-quota saturation.

The real Search baseline at 32 callers showed substantial JIT/warmup drift
across rounds and is excluded from implementation-benefit comparisons. The
256-caller native baseline was substantially steadier. These exclusions are
retained alongside the command-count mechanism evidence and the complete raw
data; a command-count reduction does not by itself prove a throughput gain.
