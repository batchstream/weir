# What justifies Weir?

Weir has measured value when it aggregates independent concurrent single-record
operations across callers. The current baseline uses multiple OS client processes
and concurrent workers; each native or SDK call contains one record, matching
fragmented crawler updates. Both paths must meet the same durability and
visibility requirements.
The objective is lower actual database load at comparable completed throughput,
or higher completed throughput at comparable actual database load. Historical
native client-preassembled bulk tests remain boundary evidence; they are not the
current crawler baseline.

## Default selection and independent confirmation, 2026-10-16

The current defaults are **128 operations, 4 MiB, 20 ms write collection and
16 simultaneous adapter invocations per Store**. They come from the initial
finite selection among 20 count/wait/concurrency configurations and a four-point
byte anchor, scoped to the declared workloads and gates. This does not establish
a globally optimal value for each control. The selection rules were frozen
before selection measurements. Typed reads and mixed batches containing reads
become eligible without collection;
explicit `max_wait: 0s` and `max_in_flight: 0` retain immediate and unlimited
execution policies. The owned default-path fixtures omit the entire batching
block and audit the effective configuration plus all four runtime controls.

The [measured-defaults report](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/README.md),
[frozen selection audit](https://github.com/batchstream/weir-tests/blob/3f47919befc8db3f8e612cc7e0f17feda16ad912/results/default-tuning-2026-10-16/analysis-selection-parser-fix/analysis.json)
and [complete 25-workflow, 591-row audit](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/analysis-final-registered-capacity/analysis.json)
retain the source/configuration receipts, raw outcomes and independent audits.
The confirmation server is
[`084572d`](https://github.com/batchstream/weir/commit/084572d79b7a53230801297f90ca2363fd864913).
The results below distinguish actual database CPU from CPU per completed unit;
equal configured database quotas do not establish equal actual load.

For pure Search writes with 1 KiB padding, a database quota of 0.25 CPU and
512 concurrent workers across four OS clients, both paths pass the measurement,
rate-stability, generator/host-headroom and sustained database-CPU checks in all
three rounds:

| Round | Native writes/s | Default Weir writes/s | Throughput ratio | Measured DB CPU, native / Weir (% of quota) | Weir p99 upper bound |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 643.322 | 5270.143 | 8.19 | 99.82 / 99.14 | 327.679ms |
| 2 | 664.674 | 4480.537 | 6.74 | 99.98 / 99.48 | 327.679ms |
| 3 | 655.657 | 5468.189 | 8.34 | 100.05 / 99.14 | 294.911ms |

This establishes 6.74–8.34 times the completed throughput at nearly the same
measured database CPU for this tested point. Actual Weir bulks average
115.52–118.60 records across these rounds. The 512-to-1024 gains are
11.1%, 40.6% and 9.0%; the third 1024-worker round's second/first-half completion
ratio is 0.864048. The successor therefore establishes neither the repeatable
plateau nor stability required for maximum capacity.

A separately registered 0.125-CPU study retains its original preparation failure
and the named readiness correction. The correction removes a duplicate fixture
deadline; it changes no Weir code, timed operations or qualification gates.
At 512 workers, all three corrected rounds pass the original quality, headroom,
stability and sustained database-CPU checks:

| Round | Native writes/s | Default Weir writes/s | Throughput ratio | Measured DB CPU, native / Weir (% of quota) | Weir p99 upper bound |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 677.454 | 5197.600 | 7.67 | 103.409 / 103.640 | 393.215ms |
| 2 | 695.808 | 5379.045 | 7.73 | 103.944 / 103.603 | 425.983ms |
| 3 | 695.287 | 5794.242 | 8.33 | 103.967 / 103.947 | 327.679ms |

Actual database CPU is approximately 0.129–0.130 cores; the paired Weir/native
CPU ratios are 0.99672–1.00223. This independently confirms higher completed
throughput at comparable actual database CPU for the tested point. Its
512-to-1024 gains are 11.220%, 12.062% and 3.510%, and the third 1024-worker
round's half-window completion ratio is 0.879494. Those observations still
do not qualify a Weir maximum. The initial 256-worker round has strong
cold-state effects and is not used as the principal benefit claim. The failed
preparation, all measured rounds and unchanged zero-check soak derivations
remain in the report.

The larger-body Search case uses 64 KiB payloads, a 2048-record namespace,
a one-CPU database quota and 512 workers:

| Round | Native writes/s | Default Weir writes/s | Native / Weir DB CPU ms per write | Mean Weir records per bulk | Mean framed Weir bulk body |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 961.448 | 1891.298 | 0.803 / 0.471 | 47.73 | 2.990 MiB |
| 2 | 1399.400 | 1978.225 | 0.545 / 0.418 | 47.79 | 2.994 MiB |
| 3 | 1305.379 | 2089.408 | 0.609 / 0.395 | 48.82 | 3.059 MiB |

Completed throughput is 1.41–1.97 times native and CPU per completed write is
23.3–41.3% lower. Actual database CPU is 4.6–16.0% higher, so these rows do not
support an equal-load throughput claim. The legacy immediate/unlimited case
retains means of 1.35–1.72 records per bulk. These are observed request-body
means, not inferred batch sizes or maximum-body bounds. Neither path passes
the sustained database-CPU gate in this larger-body case.

With a one-CPU database quota and 68 offered two-step crawler products/s, the three
paired rounds complete at comparable throughput, within 1.3%:

| Round | Native products/s | Default Weir products/s | Native p99 upper bound | Default Weir p99 upper bound |
| --- | ---: | ---: | ---: | ---: |
| 1 | 65.737 | 65.676 | 3.408s | 3.932s |
| 2 | 65.355 | 65.502 | 3.146s | 4.194s |
| 3 | 66.312 | 65.457 | 3.146s | 3.932s |

Measured database CPU is 8.1–42.3% lower across these paired rounds. Database
CPU per completed product is 8.3–42.3% lower. The p99 increase remains part of the
result. The pure-write points above and these crawler points pass the declared
five-second completion-p99 gate. This is not a general service latency guarantee,
and p99 alone cannot substitute for arrival, backlog, drain or capacity gates.

At 512 offered partial updates/s, default Weir completes all 30,720 arrivals in
each round. Native rejects 4,608, 3,072 and 2,625 arrivals respectively. This
prevents a qualified three-round comparison at matched completed throughput;
these rows cannot establish equal-throughput CPU savings or a maximum.

MongoDB ordinary writes, mixed read/write traffic and Lua updates also compare
the new default with the legacy immediate/unlimited settings. Each Weir result
is first normalized against its own paired native result. The following ratios
then compare default/native with legacy/native; they are not direct raw QPS or
database CPU ratios between separately run cases.

| MongoDB workload | Normalized default/legacy completed QPS | Normalized default/legacy DB CPU per completed operation |
| --- | ---: | ---: |
| Ordinary write | 1.079–1.100 | 0.586–0.644 |
| Mixed read/write | 0.989–1.022 | 0.934–0.987 |
| Lua update | 1.251–1.331 | 0.363–0.454 |

All 12 predefined read guards pass, with the same normalized default/legacy
completed-QPS ratio ranging from 0.934 to 1.065. These guards compare the new
defaults with legacy Weir settings; passing does not establish a read speedup
over native calls. The [absolute read measurements](https://github.com/batchstream/weir-tests/blob/acc5ec2e0e214e0edb28a20fc33d04788bb9e70a/results/default-tuning-2026-10-16/read-guards.json)
retain these three-round ranges:

| Backend | Total workers | Default Weir/native completed QPS | Default Weir p99 upper bound |
| --- | ---: | ---: | ---: |
| Search | 4 | 0.326–0.394 | 4.607–5.119ms |
| Search | 128 | 0.682–0.709 | 61.439–65.535ms |
| MongoDB | 4 | 0.418–0.447 | 2.303–2.815ms |
| MongoDB | 128 | 1.236–1.289 | 32.767ms |

Native access is faster in both tested Search read cases and the MongoDB
four-worker read case. The measured adoption case is collecting concurrent
writes and reducing crawler database load. Read guards check default-change
regressions in their declared cases, with no latency guarantee for arbitrary
workloads.

The later preregistered [wait sensitivity result](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/WAIT-SENSITIVITY-RESULT.md)
is **inconclusive**. It measures 20/40/80/160 ms on both fixed-arrival profiles,
holding 128 operations, 4 MiB and 16 executions fixed. All 24 stages complete
every arrival with full-field audits and zero failed, unknown, timed-out or
rejected operations; every p99 is below five seconds. Nevertheless, the 20 ms
crawler anchor's third round ends with backlog 254, above the allowed
154 + 68 = 222. The 160 ms crawler's first round drains in 3.17392354 seconds,
above the three-second limit. Although 40 and 80 ms meet their individual gates,
the registered selector requires a qualified anchor in every round and
proposes no replacement. It does not establish that 20 ms wins; the current
default retains the initial finite selection.

The original duration-parser repair restored 48 pure-write/large-document rows
after accepting the valid Go duration `1m0s`, preserving all 501 rows analyzed
before that repair. These are historical equivalence counts, not the final
inventory. The complete wait audit contains 591 rows; its
[equivalence receipt](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/wait-sensitivity-equivalence.json)
preserves the prior 567 rows and original
selection/capacity decisions. The independent crawler ladder with a database quota
of 0.25 CPU has no rate that passes all three rounds, across all seven declared
rates from 8 through 544 products/s. The independent partial-update ladder has
qualified three-round lower points at 32 and 64 updates/s, but no valid upper
bound. Higher invalid rows cannot establish where sustainable capacity ends.

Six higher-pressure native-paired crawler/partial cases with a database quota
of 0.25 CPU record native warmup HTTP deadline errors, followed by default-Weir
startup qualification failure before its measured stage. Cleanup preserves the
nonzero exit while confirming owned process/container removal. They are neither valid
paired comparisons nor a demonstrated Weir capacity upper bound. The roughly
five-second qualification failure is consistent with the configured startup
budget, but the generic product-identification error does not prove the exact
HTTP failure cause. No qualification gate or startup budget is relaxed.

The [archived variability inspection](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/CAPACITY-VARIABILITY.md)
records sustained database CPU during the unstable rounds without establishing
a cause. Young-GC time changes from 332 to 46 ms at 0.125 CPU and from
163 to 174 ms at 0.25 CPU; these observations do not establish GC as the
cause of the decline. Whole-stage merge and refresh totals were recorded,
but their half-window timing series were not;
they cannot locate activity within the falling interval or assign CPU cost.
Disk latency and utilization were not recorded, and host I/O wait is not disk
utilization. These observations do not prove a GC, merge or disk explanation.

The [final registered capacity workflow](https://github.com/batchstream/weir-tests/actions/runs/38815775471)
planned 96 minutes of warmup and timed work, but fails during fixture preparation:
the Elasticsearch `/_cluster/health?wait_for_status=yellow&timeout=2s` request
times out awaiting response headers. No Weir or client process, warmup or timed
stage starts; actual measured work is zero. All
[400 archived raw files](https://github.com/batchstream/weir-tests/tree/e8c439d31f43596f4fc75d1c920f9a14d36b3623/results/default-tuning-2026-10-16/raw/confirm-pure-extended-q0125)
were committed before interpretation, including successful owned-container
removal and an empty cleanup-error list. This is a preparation failure, not
an observed service-overload boundary. It supplies no new throughput point,
capacity upper bound or eligible five-minute sustained check. The preregistered
study stops here, retaining all earlier unqualified capacity points without a
performance retry, another concurrency level or a new quota.
The [final full audit and unchanged decisions](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/final-analysis-equivalence.json)
retain all 591 rows. The [original derivation still selects zero sustained checks](https://github.com/batchstream/weir-tests/blob/98671e066d1a209439a154de9ded7e1397804b6e/results/default-tuning-2026-10-16/soak-derivation-final-no-qualified-points.json),
so none was dispatched. Later fixture-health and diagnostic fixes have offline
regression coverage; they supplied no replacement performance measurements.

The tested configurations and owned loopback databases bound the selection.
These results establish no maximum for these defaults, universal optimum or
production-network capacity. Host and generator CPU are used to check measurement
headroom and bottleneck attribution; extra Weir CPU
is not an adoption gate for the database-offload objective.

## Broader batching coverage, 2026-10-15

The [complete batching coverage report](https://github.com/batchstream/weir-tests/blob/17ab0506defcdb9f374aa994e7bb4467d43ebf82/results/batch-coverage-2026-10-15/README.md)
retains exactly six prospectively declared workflows and all 60 timed stages:
six-setting pure-write calibration, two observer diagnostics and three independent
formal confirmations. Formal stages use 60s warmup, 60s timing and three rotating
or alternating rounds. Native callers submit single-record operations. The
candidate is 128 operations, 8 MiB, 5ms collection and 16 adapter executions;
the immediate baseline is 32 operations, 8 MiB, zero wait and unlimited executions.
Both Weir paths within each workflow use identical `b9dfbc2` server binaries.

At 68 offered two-step crawler products/s, all nine formal stages complete every
arrival with verified persisted fields and zero rejected, failed, unknown or
timed-out products. Against native single-record updates, the candidate uses less
database CPU per completed product while completed QPS stays within 1.3%:

| Round | DB CPU/product change versus native | Completed QPS change versus native |
| --- | ---: | ---: |
| 1 | -41.6% | -0.70% |
| 2 | -14.7% | -1.22% |
| 3 | -13.7% | -1.22% |

The latency tradeoff remains explicit: reported p99 histogram upper bounds rise
from 3.146s to 4.194s. Candidate drain is 2.33–2.39s versus native 1.57–1.96s.
Actual bulks carry about 12 records, compared with about 2.2 on immediate Weir.
The 512 partial-updates/s profile uses 16 KiB seeded documents and application
acknowledgement. Candidate versus immediate Weir reduces CPU/update by 31.9–35.5%
at comparable completed QPS. Native rejects 1024 arrivals in round 1, preventing
a strict three-round candidate/native comparison for that profile.

Pure indexing at 128/512 workers completes 3.30–4.00 times as many writes/s as
native in every paired round, with 48.2–53.7% lower measured database CPU.
All twelve pure-write stages pass immutable-source, full 8192-record postflight
and measurement checks with zero failed, unknown or timed-out writes. This is
an observed gain at fixed concurrency. Neither path demonstrates sustained CPU
saturation and a successor plateau, so it is not a maximum-capacity claim.

The short calibration retains all six controls and negative immediate-dispatch
results; it does not select a universal optimum. Actual NDJSON bytes, generated
Lua output, sparse/burst scheduling, cancellation and bounded-slot behavior now
have focused coverage. Owned Elasticsearch 8.19.22 and OpenSearch 3.3 tests add
eight-index skew, 1/256 KiB sources and hot-key increments; these are correctness
checks rather than throughput measurements of those distributions. Performance
runs use one owned Elasticsearch host with four OS clients over loopback. All
source/configuration receipts, raw hashes, actual CPU intervals and full-field
oracles remain available. That experiment retained the defaults in effect on
2026-10-15, and established no maximum-capacity, steady-state or production-network
claim.

## Bounded collection follow-up, 2026-10-15

The [collection and phase report](https://github.com/batchstream/weir-tests/blob/22d719cd10f4bc2c390a49772d606ed50922a4f5/results/batch-mechanism-2026-10-15/README.md)
separates a simple partial-update mechanism control from the unchanged crawler
Lua workload. Both Weir paths use the identical `49bab6a` binary, with
`batching.max_wait=0s` or `20ms` as their only configuration difference. Four OS
clients own 512 workers and submit exactly one record per native update or SDK
call. Durability, query visibility and the crawler's reconciliation/product/seller
dependencies remain unchanged. Formal stages use 30s warmup, 20s arrivals and
three rotating rounds per workload, with the HTTP observer disabled.

At 68 offered crawler products/s, all three paths completed every arrival and
passed all nine full-field and all-arrivals checks. The 20ms path used less
database CPU per completed product than native single-record updates in every
paired round, while completed QPS stayed within the predefined 5% band:

| Round | DB CPU/product change versus native | Completed QPS change versus native |
| --- | ---: | ---: |
| 1 | -65.9% | -1.40% |
| 2 | -14.6% | -4.01% |
| 3 | -5.5% | -0.59% |

Against the same Weir binary with immediate dispatch, CPU/product fell by 35.5%,
27.8% and 33.3%. Actual bulk requests carried about 6.8–6.9 records on average,
compared with 2.1–2.3 at zero collection wait. Short diagnostics independently
matched internal HTTP attempt counts and framed records against the HTTP
observer; the report retains all requests and phase measurements. Bulk elapsed
time includes visibility waiting, and concurrent phase durations are not CPU or
exclusive critical-path time.

This establishes a bounded database CPU improvement in the tested crawler
model. The effect size varies substantially across rounds. Native did not meet
the sustained CPU gate in those formal rounds, so this is not a maximum-capacity,
steady-state or production-throughput claim. The separate 512 partial-updates/s
formal control failed the strict comparison gates because of rejected arrivals
and generator lag; only one of its nine stages passed all-arrivals checks.
Larger batches there are mechanism evidence, not a qualified performance win.
All failed and rejected observations remain in the report. The default collection
wait was `0s` during this 2026-10-15 experiment; it did not select a universal
waiting policy. The later default selection is described above.

## Single-record crawler follow-up, 2026-10-15

The [cross-index batching experiment](https://github.com/batchstream/weir-tests/blob/bad363af4f36e366c1a7e7a55b8a36a4b9f713c8/results/search-batching-2026-10-15/README.md)
and [metadata-only experiment](https://github.com/batchstream/weir-tests/blob/bad363af4f36e366c1a7e7a55b8a36a4b9f713c8/results/search-metadata-2026-10-15/README.md)
retain all 54 timed stages, immutable sources, raw hashes and independent audits.
Four client processes own 128 workers. Each native `_update` and Weir `Mutate`
call contains one record; the native caller receives no preassembled batch.
Business completion requires query-visible reconciliation, product mutation and
the independent parallel seller mutations. The offered rates are 16, 48 and 96
products/s, each with three paired rounds and the same database resources.

The cross-index candidate `b13318f` was rejected. Completed throughput regressed
by 3.6–12.3% at 48 offered products/s and 21.2–31.5% at 96. Larger batches did not
compensate for the observed latency and drain regressions. That implementation
was removed before merge.

The metadata-only candidate `7455013` retains the original index-specific batching
and fresh target checks. At 16 and 48, both Weir versions completed all arrivals
and passed all arrival gates, with similar throughput. CPU per completed product
reversed direction in the third round, so these measurements establish no stable
database CPU saving against baseline or native. At 96, every path was overloaded;
those rows do not establish sustainable capacity. Separate real-backend protocol
tests with 128 mapped fields reduced each metadata response from about 4.2 KB to
0.3–0.45 KB while preserving safety controls; that response-byte reduction is not
a throughput claim.

The separate [database-pressure follow-up](https://github.com/batchstream/weir-tests/blob/96a2e8ffc336ae11aea2e367741eb43c20e2275c/results/crawler-pressure-2026-10-15/README.md)
retains a prospectively selected 512-worker matrix across four client processes,
with 45, 57 and 68 offered products/s and three paired rounds per rate. All 27
formal stages passed the source, field and all-arrivals audits, with zero failed,
unknown, timed-out or rejected products. At 68, the metadata candidate reduced
database CPU per completed product against the original Weir by 42.6%, 11.2% and
6.6%, while paired completed throughput differed by less than 0.2%. At 45 and 57,
the CPU direction reversed between rounds. Against native single-record updates,
the candidate did not establish a stable CPU saving at any of the three rates.
None of the 27 formal stages satisfied the sustained database CPU gate, so the
short calibration's CPU pressure does not establish a formal database bottleneck
or a capacity gain. The retained 2048-worker calibration failed with timeouts,
unknown outcomes and field-audit failures; it is unqualified evidence. These
measurements support a limited metadata improvement over the previous Weir at
one tested rate, without establishing Weir's benefit for this crawler workload.

These crawler results must be assessed separately from the simpler fixed atomic
transform below. Its observed gain does not establish a gain for the crawler's
submission pattern and completion requirements.

## Sustained Linux benchmark, 2026-10-15

The independent [complete benchmark report](https://github.com/batchstream/weir-tests/blob/bcda72693ee5a08539c03130db1963ca3164d103/results/value-2026-10-15/README.md)
retains original JSON, commands, configuration, source/binary identities, logs,
cleanup receipts and offline analysis. It measures this server at
`8955cde10e574dae55d890b0c9c824fd39219d1c`, with Go 1.27.1, SDK v0.15.0,
protocol v0.13.0, MongoDB 8.0.32 and Elasticsearch 8.19.22.
Ten successful workflows produced 94 paired comparisons and 564 timed stages:
95,142,464 successful records, zero errors, timeouts or unknown outcomes, with
all independent final-state checks and owned-fixture cleanups passing.

At a 1-CPU database quota, these observed improvements exceed 10% in every
paired round. Rates use all successful records divided by all measured time.
Each path shares a four-logical-CPU runner; Weir uses additional unrestricted
service CPU, so equal database quota does not mean equal total CPU demand.

| Work | Workers | Native records/s | Weir records/s | Change | Native p95 / p99 ms | Weir p95 / p99 ms |
| --- | ---: | ---: | ---: | ---: | --- | --- |
| MongoDB read | 256 | 9,468.6 | 14,050.8 | +48.4% | 73.727 / 90.111 | 30.719 / 40.959 |
| MongoDB 90/10 read/write | 256 | 4,876.2 | 9,743.8 | +99.8% | 106.495 / 180.223 | 49.151 / 73.727 |
| MongoDB write | 128 | 2,530.2 | 3,390.3 | +34.0% | 98.303 / 425.983 | 90.111 / 212.991 |
| Search fixed atomic transform | 256 | 1,628.6 | 3,526.0 | +116.5% | 196.607 / 229.375 | 131.071 / 180.223 |

Latency numbers are pooled histogram upper bounds, with approximation up to
12.5% + 1µs. Three paired rounds alternate path order; each path receives 10s
warmup and 20s timed work. Four independent client processes issue one record
per call against 8192 hot records with 1024-byte padding and disjoint writer
keys. The transform control uses one native atomic update computing
`revision = 1 - revision`, compared with the same computation in Weir Lua;
there is no unnecessary native client read or transaction.

A same-run grouping ablation supports the mechanism: at 128 MongoDB read workers,
grouping limit 1 gives 7,433.0 Weir versus 8,644.4 native records/s; limit 32 gives
15,667.8 versus 8,643.0. The native baseline is essentially unchanged, while
Weir's selected MongoDB commands per record fall from 1.0000 to 0.1251.
The configured maximum is 32, but the observed adapter average is 7.99 records.

The report also retains the negative evidence. Low-concurrency ordinary MongoDB
requests are slower through Weir. Search pure reads lose at every tested
concurrency. Native MongoDB bulk reads achieve about 102,000–104,000 records/s,
versus 19,000–29,000 through the SDK with 32 records per call. A 2-CPU database
sensitivity run removes several apparent gains, and some Search write rounds
vary substantially. Weir's CPU/RSS, client samples and database resource counters
are included as resource and headroom observations. Only
MongoDB ordinary writes at 128 workers qualify on both paths for the primary
database saturation and successor plateau conditions. Other rates are observed
throughput within the tested ladder, not maximum-capacity claims.

See the [method](https://github.com/batchstream/weir-tests/blob/bcda72693ee5a08539c03130db1963ca3164d103/results/value-2026-10-15/METHOD.md)
and [complete round/latency/resource tables](https://github.com/batchstream/weir-tests/blob/bcda72693ee5a08539c03130db1963ca3164d103/results/value-2026-10-15/TABLES.md)
before applying these numbers to a deployment. Single-owner loopback measurements
with hot data do not establish cross-host, cold-data, conflicting-write or fleet
behavior. The finite local experiment below is separate historical mechanism
evidence; its timings are not combined with this sustained benchmark.

## Local MongoDB mechanism experiment, 2026-10-15

This local experiment **does not justify Weir for an application that can already
issue native database batches**. Direct native batches were much faster.
It also does not justify Weir on single-caller latency.

The executable test is
[`TestValueProofMongoReadPaths`](../internal/server/value_proof_integration_test.go).
It uses MongoDB 8.0.32, the real adapter, Store scheduler, gRPC transport and released
Go SDK. There is no mocked database, scheduler pause, collection timer, injected
database delay or assertion that Weir must win.

Each sample reads the same 512 distinct BSON documents, including a 256-byte
payload. Direct and Weir use separate MongoDB pools, both with retries disabled,
no maximum pool size and two simultaneous connection establishments. Both paths
pass through the same loopback MongoDB wire observer. The Weir path additionally
uses a real loopback gRPC listener and SDK discovery. One complete workload per
case warms pools at the tested concurrency and populates target metadata before
measurement. Three measured rounds alternate forward/reverse case order.

One worker has one outstanding request. Request size 1 therefore means independent
one-record Execute RPCs in the Weir path; 32 workers means at most 32 outstanding
single-record calls. Workers share one SDK client: this demonstrates independent
RPC aggregation, not throughput across independent operating-system processes.
Request size 32 means sixteen native `$in` commands in the
direct path or sixteen SDK Read calls with 32 individual stream records in the
Weir path. Request input reverses insertion order. Every returned document is
compared byte-for-byte with its original BSON, and SDK results must retain request
order. All results passed; no errors or missing documents were accepted as work.

The observer counts actual `find`, `getMore` and `listCollections` commands before
they reach MongoDB. These warm samples contained only `find`; no hidden metadata
or cursor continuation calls were omitted from the table. Connection handshakes
and pool warmup are outside the samples. Batch histograms come from each actual
command's `_id` selector, not a configured batch limit.

| Path | Records per client request | Concurrent requests | Backend calls, rounds 1 / 2 / 3 | Elapsed ms, rounds 1 / 2 / 3 | Median ms |
| --- | ---: | ---: | --- | --- | ---: |
| Direct driver | 1 | 1 | 512 / 512 / 512 | 42.420 / 41.037 / 40.659 | 41.037 |
| Weir SDK | 1 | 1 | 512 / 512 / 512 | 94.959 / 93.476 / 94.397 | 94.397 |
| Direct driver | 1 | 32 | 512 / 512 / 512 | 12.633 / 12.485 / 11.069 | 12.485 |
| Weir SDK | 1 | 32 | 267 / 304 / 259 | 18.282 / 22.659 / 16.111 | 18.282 |
| Direct driver | 1 | 128 | 512 / 512 / 512 | 14.132 / 15.735 / 13.412 | 14.132 |
| Weir SDK | 1 | 128 | 129 / 76 / 144 | 13.774 / 12.341 / 13.327 | 13.327 |
| Direct native batch | 32 | 1 | 16 / 16 / 16 | 2.749 / 3.142 / 2.601 | 2.749 |
| Weir SDK | 32 | 1 | 95 / 99 / 85 | 12.479 / 12.644 / 12.476 | 12.479 |

For example, the first Weir single-record round at concurrency 128 observed this
`records per find : number of finds` distribution:

```text
1:48 2:19 3:17 4:11 5:7 6:10 7:5 8:2 9:2 11:2 16:1
20:1 23:1 27:1 28:1 31:1
```

The histogram sums to 129 commands and 512 returned records. These are independent
single-record SDK calls, so their coalescing is actual cross-request batching.
Across the three rounds, concurrency 32 reduced database calls by 40.6–49.4%, and
concurrency 128 reduced them by 71.9–85.2%. Correct BSON and caller association were
preserved. **Fewer backend calls are proven; proportional CPU or cost savings are
not measured.**

The counterexamples matter:

- At concurrency 1, every record still needs its own backend command. Weir adds
  transport and scheduling work, and elapsed time is about 2.3 times direct here.
- At concurrency 32, Weir reduces command count but takes longer. Command reduction
  alone is not an end-to-end performance result.
- At concurrency 128, elapsed times are close. The modest median difference in
  this short, shared-process experiment does not establish maximum throughput.
- A 32-record SDK Read does not imply one 32-record backend command. Execute
  receives individual stream records, and the scheduler immediately dispatches
  available compatible work without waiting for the rest. Its 85–99 backend
  commands compare poorly with direct native batching's 16; the Weir median takes
  about 4.5 times as long. This is an explicit limit of the current design.

No backend saturation, p95/p99 latency, database CPU, total host CPU, RSS, network
egress, failover capacity or production monetary cost was measured. Client, Weir
and MongoDB share one Apple M2 host; client and Weir share the test process.
Concurrency and CPU scheduling can influence observed batch sizes. These finite
results support mechanism and counterexample claims, not a production capacity
number. The shared wire observer adds forwarding and per-command bookkeeping;
that extra fixed cost can favor fewer-command paths compared with uninstrumented
database connections. Search performance is not inferred from MongoDB.

## Correctness evidence alongside batching

The following existing tests were executed and passed on the measured source
snapshot recorded below, with the same owned MongoDB fixture.
They are separate from the timed read comparison; the table identifies where a
fixture replaces a component.

| Executed test | Actual evidence | Boundary |
| --- | --- | --- |
| `store.TestNativeCoalescedRecordsKeepLargeResultsAndRetainedOwners` | 32 prepared requests share one actual MongoDB `find`; duplicate ordinals do not mix caller results; more than 32 MiB of total result data is preserved and independently released | The scheduler is deliberately populated before dispatch and reads share one key. This proves the ideal queued mechanism, not naturally occurring batch size or network throughput |
| `server.TestRecordStreamPublishesFIFOAfterOutOfOrderBackendCompletion` | Real gRPC streams publish indexes 1 then 2 even when backend work completes in the opposite order | The adapter is an in-memory fixture, not a database |
| `server.TestRouteMongo2MiBRecordLuaScanAndPartialBatch` | Real MongoDB and gRPC preserve a 2 MiB BSON record; twelve same-resource Lua increments in one stream persist `n=12`; a rejected Create does not erase a peer's confirmed Put | Functional correctness, not Lua throughput or an application transaction across requests |
| `server.TestRouteMongoAppliedWriteAndNativeReplyLossAreNotReplayed` | A wire proxy drops a real database acknowledgement after it is received; typed writes report `UNKNOWN`, Native reports an incomplete response, actual backend state changes once and the wire command is not replayed | Controlled acknowledgement loss, not every network or replica-set failure |
| `mongodb.TestMongoMixedWriteLostReplyDoesNotReplayExpression` | A real mixed batch's acknowledgement is dropped; both results remain `UNKNOWN`; an independent database read sees `$inc` increment once and a peer Create persisted; exactly one `bulkWrite` was sent | Adapter-level real database evidence; no SDK in this particular test |
| `mongodb.TestMongoLuaConflictRetriesStopAtAttemptBound` | A real external writer conflicts with each Lua transaction; configured three attempts produce exactly three reads, writes and confirmed aborts, no commit, and a final `NOT_APPLIED` conflict; a canceled caller starts no additional attempt | Deliberately induced conflicts and custom retry settings; this proves the configured bound, not throughput or every failure mode |

UNKNOWN is necessary information, not successful exactly-once delivery. Once an
acknowledgement is lost, Weir cannot promise that the mutation was not applied.
Applications still need their own reconciliation or idempotency policy.
Same-resource ordering is per stream; there is no global cross-client ordering
or transaction guarantee. These limits must survive any optimization.

## The smallest defensible product

| Component | Decision | Reason |
| --- | --- | --- |
| Store queue, compatible batching and caller identity | Keep | Cross-caller aggregation is the mechanism measured here; per-caller result and cancellation ownership are required for correctness |
| MongoDB and Search adapters | Keep | They represent two real protocols with different batching, visibility and conditional-write semantics |
| In-process Lua | Keep only for applications needing this transform contract | A shared trusted transform can avoid application-side read/modify/write races; MongoDB transactions and Search OCC provide its backend safety. Its performance benefit is not proven by the read experiment |
| Stream flow control, bounded waiting queue and write-outcome evidence | Keep | These are required to make aggregation finite and safely report partial/uncertain work; they do not constitute an unlimited memory guarantee |
| Separate Lua executable, IPC or worker process | Remove / do not introduce | It adds a deployment and failure boundary with no need established here. Lua belongs in the one Weir process |
| Connection pool duplicated per application | Merge into shared Store only where deployment evidence supports it | Centralization can reduce aggregate client pools, but peak connections and failover behavior require actual fleet measurements |
| Store discovery | Keep only for the logical Store mapping requirement | It does not cause batching and is not a performance justification. If a deployment needs only one known backend endpoint, ordinary DNS plus a direct driver is simpler |
| Native forwarding | Keep only for a concrete compatibility requirement | It preserves native access for existing users; it does not create typed batching value and is not a reason by itself to add a network service |
| Another scheduling framework, global ordering layer or hypothetical backend | Remove / do not add | No requirement or evidence here pays for the extra state and coordination |

Starting from scratch, the minimum useful path is **client → one Weir Store queue
and scheduler → native database batch**, with adapters owning database semantics
and a single process owning Lua and lifecycle. Discovery and Native need their own
user requirements; adding them does not strengthen the batching proof.

## Adoption gate

Database CPU per completed record is the primary load measure here. Batching can
reduce fixed database command work, while qualification, pre-reads, conflict
retries and visibility barriers add database work or waiting. Moving transforms
outside the database can reduce its per-record computation. Measure their net
effect at the required completed throughput and latency; batch size alone cannot
establish a benefit. Different namespaces, conflicting keys, low overlap and
multiple Weir replicas can fragment the available batches.

Before adopting Weir for performance, require all of the following:

1. Real independent callers have overlapping compatible work and cannot already
   issue equivalent native batches cheaply. Measure actual batch distributions,
   not `batching.max_operations`.
2. A matched direct-driver baseline uses identical submission sizes, data,
   mutation semantics, durability, visibility, retry policy, concurrency and
   database resource limits. A native-batch control must explain how the actual
   callers obtain those batches; do not assume cross-process client aggregation.
   Verify outcomes and persisted mutation effects outside the timed phase.
3. Representative sustained load shows lower actual database load at comparable
   completed throughput, or higher verified completed throughput at comparable
   actual database load, while meeting the declared p95/p99 and drain limits.
   Equal database quotas alone do not establish equal observed load.
4. Both paths reach a demonstrated bottleneck and a higher-concurrency plateau
   before calling the comparison a capacity result. Use multiple runs and an
   explicit gain threshold agreed for the deployment; reject gains inside noise.
5. Cancellation, overload, partial results and lost acknowledgements still meet
   the correctness contract. A result-count or throughput improvement cannot
   compensate for replaying an uncertain mutation.

The sustained benchmark above demonstrates useful aggregation under specific
high-concurrency conditions and retains direct-access counterexamples. It
supports Weir as a **shared batching and transform service for fragmented
concurrent callers**, conditional on the deployment's matched workload, actual
database load, completed throughput and latency requirements. If the adoption gate
fails and no shared transform/discovery contract is needed, use the direct driver
and remove the extra service.

## Reproduce and audit the local mechanism experiment

This measurement used Go 1.27.1 on Darwin/arm64, Apple M2 (8 logical CPUs),
24 GiB host RAM, MongoDB 8.0.32 single-member `weir_local` replica set on
`127.0.0.1:27028`, no authentication, SDK v0.15.0, protocol v0.13.0 and MongoDB
driver v2.9.1. There was no CPU quota or separate CPU allocation. The fixture
creates a unique `weir_test_<pid>_<sequence>` database and drops only that database
at cleanup. It verifies the replica-set identity before creating data. No
credentials or production endpoints are used.

```sh
scripts/mongo-local.sh start
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/server \
  -run '^TestValueProofMongoReadPaths$' -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -race -tags integration ./internal/server \
  -run '^TestValueProofMongoReadPaths$' -count=1
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/server \
  -run '^(TestRouteMongo2MiBRecordLuaScanAndPartialBatch|TestRouteMongoAppliedWriteAndNativeReplyLossAreNotReplayed|TestRecordStreamPublishesFIFOAfterOutOfOrderBackendCompletion)$' \
  -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/store \
  -run '^TestNativeCoalescedRecordsKeepLargeResultsAndRetainedOwners$' -count=1 -v
WEIR_INTEGRATION=1 GOPROXY=off go test -tags integration ./internal/backend/mongodb \
  -run '^(TestMongoMixedWriteLostReplyDoesNotReplayExpression|TestMongoLuaConflictRetriesStopAtAttemptBound)$' \
  -count=1 -v
scripts/mongo-local.sh stop
```

These are explicit integration commands. Default `go test ./...` does not start
MongoDB or execute this experiment. Exact command counts and histograms for Weir
vary with scheduling; the test intentionally validates correctness and baseline
accounting without hard-coding a required speedup.

The table contains only the final source run, made from a retained test executable.
Race checking was run separately and passed; its instrumented durations are not
included in the performance table. The local evidence directory also retains the
raw correctness/race logs, `build-inputs.json`, a snapshot of 96 actual local
compilation input files, and their `source-manifest.sha256`. It contains Go sources, required
embedded inputs and module declarations; it does not capture environment files or
credentials. The directory is ignored by Git and is not a published benchmark.

```sh
mkdir -p .testdata/value-proof-20261015-refactor
GOPROXY=off go test -tags integration -c \
  -o .testdata/value-proof-20261015-refactor/server.test ./internal/server
WEIR_INTEGRATION=1 .testdata/value-proof-20261015-refactor/server.test \
  -test.run '^TestValueProofMongoReadPaths$' -test.count=1 -test.v \
  > .testdata/value-proof-20261015-refactor/value-proof.log 2>&1
shasum -a 256 .testdata/value-proof-20261015-refactor/server.test
```

Run receipt:

```text
source base: db52eed45f6f3812dc63f22935bc8cfb03955a5d
test executable SHA-256: 82f9b660e81bdbacce136261554e1678ecd57fddfbea5e1091b65bb068ba07f6
raw value-proof.log SHA-256: 174b33cf4c5703eabbfbfd7305b03358b05576d262552221e54f2f83be1cdcf0
source-manifest.sha256 SHA-256: efaae6a6287a3663ecdc182d2b4f6f9c08677b9d415124ca8d5b41ec0ba9a6aa
value_proof_integration_test.go SHA-256: 3dee811652dd4238e414e114ecab9ab30f36198567394564604d360ca9de0889
internal/store/runtime.go SHA-256: b5efdd4c8b7e8affa1f6306535000454e608f101b30d0569571531a356b939fd
internal/backend/mongodb/batch.go SHA-256: e5eecc706bb560a0f3efe28ebb3924940bd11d5dd93bb1c9512802d5ecbc1a4c
```

The production/configuration/peer refactor is committed at the source base above;
the checkout additionally included the new value experiment. The base commit alone
does not identify that unpublished test executable.
The retained source snapshot and executable identify this local observation, not
a released binary or a portable performance guarantee. Rebuild the current
checkout to verify current behavior, and retain the complete source/binary receipt
for any production capacity claim.

The final value experiment, separate race check and every listed correctness
test passed. The owned MongoDB process was then stopped through
`scripts/mongo-local.sh stop`; a TCP probe and listener check confirmed that
`127.0.0.1:27028` was closed. The local evidence directory retains `value-proof.log`,
`race.log`, `server-correctness.log`, `store-correctness.log`,
`mongo-correctness.log` and `cleanup.log` alongside the executable and source
receipt.
