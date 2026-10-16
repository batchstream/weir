# Typed execution, result and network memory qualification

Status: this typed-memory milestone passes the fixed local real-backend gates.
Independent PR review and CI remain required. Overall default-production
qualification still requires ingress, Lua, sustained-load and fault checks.

## Preserved failure

Clean source `fde3b1775b0a36dbb4f4044be1299f731db43314`, Linux/arm64 Go 1.27.1
CLI SHA256 `795601c60e8f4398a8108843939361032f88249acc8f75ee42ae322ca7269c79`,
used the default Store and automatic Go memory policy. The owned MongoDB fixture
contained 32 distinct documents with 12 MiB padding. Thirty-two RPCs withheld
Recv for three seconds, with their original five-second deadlines and static
64 KiB stream / 4 MiB connection flow-control windows. There were no retries.

In a 512 MiB/two-CPU service cgroup, Docker independently reported OOMKilled=true,
exit 137. The last sampled Store working reservation was 1,610,612,736 bytes;
it was an observation, not an enforced Node reservation. Native leases stayed
zero. Samples are not synchronized allocator traces, so their maxima do not
establish the exact OOM-time RSS.

The 1 GiB control survived and exited zero during cleanup. Its sampled peaks
included RSS 829,177,856, cgroup working set 840,069,120, encoded response bytes
402,655,936 and Store result reservation 536,887,296. All 32 receives exceeded
the original deadline, so this is a survival/ownership control, not document
correctness evidence. Neither capacity was repeated to select a passing result.

The [manifest](typed-memory-results-2026-10-17/typed-large-read-C0-manifest.json),
[failed race-driver output](typed-memory-results-2026-10-17/typed-large-read-C0-race.log)
and per-process JSON observations preserve the original scope and actual exit.
The probe used new owned records; shared fixtures remained running, records were
cleaned and owned CLI containers were removed.

## Correction and acceptance gates

Typed execution, retained documents and encoded responses must share the
existing finite Node budget with Native. Budget must be acquired before the
corresponding allocation. A batch shares its backend reply reservation; small
reads retain useful batching. Native keeps its existing 1024-invocation limit;
ordinary record and network ownership use the same byte account with a finite
32768-total-entry metadata bound, preserving the existing 2048-small-read
concurrency test. Nineteen fixed Node metrics separate total and Native counts. Application Ack and actual network buffer
ownership need separate references. Opaque and typed mutations keep their
existing execution evidence and are never automatically replayed.

Acceptance retains the original slow-reader pressure profile, adds successful
unstalled large-document content hashes, and requires healthy small queries,
finite accounts, bounded cancellation and clean process exit. Genuine capacity
failures are allowed under deliberate large slow-output pressure. The original
sustained 600-second profile and fault recovery gates remain required after
this correction. Implementation and affected package race/vet checks are complete. Clean-artifact
real-backend acceptance and new independent final-head review remain pending.

The original 64-session/2048-small-read success test was retained. A mechanical
1024 limit on all typed records failed it during development; total byte-bound
record ownership is now distinguished from the unchanged 1024 Native limit.
This is recorded as a failed intermediate attempt, not an accepted regression.

An additional deterministic failure showed an 8192-capacity, 1024-length result
losing its unused backing-capacity charge at Runtime completion. The correction
retains the source capacity until application ownership ends, while actual wire
ownership remains independently charged. Before logs and source copies are
preserved with the passing regressions.

Minimum encoded buffers are 1025 bytes because smaller gRPC buffers do not call
the supplied pool owner. Queue/terminal reservations include this minimum before
publication. The IO progress reserve is derived from the negotiated reply bound,
publication windows, one bounded borrowed short result and short-request frame
copies. It does not grow permanently with a large mutation frame.

Plain BSON Put validation scans supported raw types, UTF-8, canonical arrays and
its matching first unique _id without constructing a disposable Value tree.
Eighteen parity fixtures preserve the prior plain-Put behavior, including its
acceptance of non-ID duplicates and non-finite doubles. The separate expression
and Lua validation rules retain their existing restrictions. The measured
<256 KiB validation-allocation case is specifically a three-field near-maximum
scalar document, not a claim about arbitrary many-field documents.

Expensive payload tree validation now runs after memory admission and before
business IO. Such invalid payloads fail their typed record with INVALID_ARGUMENT
and NOT_STARTED instead of closing the RPC from preparation. Cheap resource and
content-type validation retains its earlier channel. Tests verify zero business
IO for preflight failures, and valid writes retain their APPLIED evidence.

The [runtime validation manifest](typed-memory-results-2026-10-17/typed-memory-runtime-validation.json)
records source hashes, exact commands, original failures and the final passing
checks. These are dirty-development-tree unit checks, not clean CLI process
qualification. Final core race: Store 3.190 s, server 20.277 s, execution 2.079 s;
backend race: Mongo 8.649 s, Search 56.846 s. App/CLI race and integration-tag
compilation also pass. The remaining acceptance gates are unchanged.

## Previous milestone

[PR 80](https://github.com/batchstream/weir/pull/80) merged as
`8c1b1b3bd1bd1519079f69a163c1772d171791e8` after new independent approval of
`2a6352eee591840e8dc63b4e3c45569dcc10d850` and
[green full default/race/vet CI](https://github.com/batchstream/weir/actions/runs/38928544351).
Its reviewer independently repeated 2560/2560 mixed calls and 20 persisted-data
oracles, p99 37.37–65.17 ms, no Native budget denial or Guard latch, all logical
ownership zero and no OOM. The [review raw log](typed-memory-results-2026-10-17/review80-burst.txt)
retains that separate admission qualification; it does not repair this typed OOM.

## First clean candidate failed acceptance

Clean candidate `45356f57972ee9a4032a84a3c11808426a6baea4`, Linux CLI SHA256
`cab795a3a2e55c150b2a2fe605317b720f64d76eaec636e24e030930b4518f77`,
passed the original 512/1024 MiB slow-reader cases. Five and sixteen responses,
respectively, completed with full document hashes and RPC OK; remaining calls
received explicit capacity failures. Healthy probes completed within the
original deadline, and logical ownership returned to zero within 5 seconds.

The subsequent 256 MiB unstalled case failed: both seeded large reads and the
12 MiB SDK Put/Read/Delete chain succeeded, but the near-maximum 16,777,152-byte
BSON Put returned NOT_STARTED / RESOURCE_EXHAUSTED. The actual test exit was 1.
All three CLI processes survived without OOM and exited zero during cleanup;
cleanup does not count as completing the failing case's acceptance gates.
No later cases or dependent regressions ran. The [original log](typed-memory-results-2026-10-17/typed-memory-fixed-process.txt)
and [validation manifest](typed-memory-results-2026-10-17/typed-memory-fixed-process-validation.json)
preserve the failure without retrying for a passing outcome.

A single diagnostic run added mutex-held numeric denial logs and auxiliary
business-step metric snapshots. Its binary used an explicit source overlay,
reported dev identity and is not a clean qualification artifact. The auxiliary
HTTP snapshots could change timing. This run reached APPLIED for the same
near-maximum Put, independently verified its content hash, then failed its next
SDK Read at result reservation. It therefore does not establish the original
Put's precise rejection branch.

At that Read denial, the mutex-held budget was 113,544,396 bytes, usage
80,574,244 bytes and the requested increase 33,553,279 bytes: a 583,127-byte
deficit. One lease still held 48,003,006 bytes of working reservation after
receiving its actual reply, alongside an input promise of 32,571,238 bytes
with only 3,296 bytes used. No previous wire owner or other lease existed.
The [diagnostic manifest](typed-memory-results-2026-10-17/typed-memory-causal-manifest.json),
[raw output](typed-memory-results-2026-10-17/typed-memory-causal-process.txt) and
[locked observations](typed-memory-results-2026-10-17/typed-memory-causal-observations.json)
record these values and distinguish precondition samples from the rejection
instant. All CLI processes survived, but the diagnostic test also exited 1.

The next correction must release genuinely unused reply workspace at its
actual lifetime boundary and preserve useful admission for a single legal
large input. It must retain the shared budget and original deadlines, sizes,
small-request burst gates and ownership checks. No PR has been opened for this
candidate; production qualification remains incomplete.

## Follow-up correction awaiting clean-process acceptance

A single owner can now exchange only unused input-queue promises for its
actual first execution or first typed-result peak. The decision simultaneously
checks the total and resident budgets before changing any account. Real input,
working memory and the protected IO reserve remain charged. Multiple owners,
existing result/wire owners and genuine capacity failures retain their prior
decisions. No budget, timeout or count limit increased.

Mongo retains the complete negotiated message allowance before IO and throughout
nonterminal cursor pages. Only a validated terminal page shrinks its working
allowance to the actual reply backing capacity plus the maximum normal wire
prefix and already charged request bookkeeping. Later metadata/write IO
restores the complete allowance first. Cursor cleanup preserves its budget
context and does not send unapproved IO.

The first typed result also prepays the existing 512-byte metadata allowance
for its protobuf wrapper. A complete Runtime regression had shown the source
reservation succeeding but the subsequent encoded envelope causing another
capacity denial. The source backing-capacity charge remains intact; future
wire ownership converges to the actual encoded length. The fixed tiny-terminal
reservation stays unchanged.

Deterministic regressions preserve the old large-Put start failure, both
first-result budget failures, the complete Runtime wrapper failure, and the
Mongo reply-lifetime over-reservation. The final focused checks include actual
Mongo Adapter + Runtime + network ownership, a TCP SDK Read through the actual
codec, maximum uint64 response index, spare source capacity, live-wire healthy
Native progress, cursor pages/cleanup, malformed responses and subsequent IO.
These focused checks pass. Full affected-package race/vet and a new clean
Linux artifact's original process qualification remain required.

Full affected-package race and vet now pass on the frozen correction: Store
3.209 s, Server 20.576 s, Execution 2.304 s, App 8.600 s, CLI 19.697 s, Mongo
8.701 s and Search 54.934 s. Source hashes stayed unchanged. The
[runtime receipt](typed-memory-results-2026-10-17/typed-memory-single-owner-validation.json)
and [backend receipt](typed-memory-results-2026-10-17/typed-memory-mongo-phase-final-receipt.json)
include exact commands, actual exits and preserved before/after evidence.
An intermediate phase-test run incorrectly counted the driver's endSessions
cleanup as business IO; its failed output is retained, and the corrected
command-specific oracle passes. This does not convert either failed Linux
process candidate into a pass.

## Second clean candidate: typed acceptance passes, burst regression fails

Clean source `21d92ef4c9daf5dc97437b095ab3c6cdba6566ef`, Linux CLI SHA256
`3aec17a432d69a4409e3b0bddb1f86be8b003863255b91aed0ac7970bffe94a4`,
passed all seven original typed-memory process cases in 30.606 seconds. This
includes the original slow-reader pressure, unstalled 256/512/1024 MiB complete
12 MiB and near-maximum Put/independent hash/Read/Delete/absence chains,
cross-Store Typed/Native and Search large/many-field documents.

The required subsequent 10-by-256 mixed small-call burst failed: 42 Search Put
calls in the first round returned UNKNOWN / RESOURCE_EXHAUSTED in 33.015–46.883
ms. The Node budget-rejection counter increased once; rounds 1–9 succeeded.
Search's independent final enumeration found all 430 offered write IDs plus
the seeded record, while only 388 writes had APPLIED acknowledgements. The
42 UNKNOWN IDs existed, but the original oracle only content-checks APPLIED
IDs, so their contents were not validated. This is neither replay evidence nor
a claim that UNKNOWN writes succeeded correctly.

All eight CLI processes survived without OOM and exited normally. Logical
recovery succeeded, but business failures continue to block qualification.
The [combined manifest](typed-memory-results-2026-10-17/typed-memory-fixed-r2-validation.json),
[typed raw log](typed-memory-results-2026-10-17/typed-memory-fixed-r2-process.txt)
and [failed burst log](typed-memory-results-2026-10-17/typed-memory-fixed-r2-burst.txt)
preserve actual exits and unchanged source hashes. Diagnostics, five output
rounds, three small-query repetitions and new ingress probes were not run.

One diagnostic-only repeat of the original burst used numeric-denial overlays
on clean21 and a test overlay changing only the expected dev identity. It
passed 2560/2560 with no budget rejection or denial log; it did not reproduce
the failure or identify its exact branch. No retries followed. Its
[manifest](typed-memory-results-2026-10-17/typed-memory-r2-burst-causal-manifest.json)
and original failed clean run are both retained. A deterministic shared-budget
regression is being prepared to test the post-send normal-ACK reservation
path. Production acceptance and PR review remain incomplete.

## Deterministic normal-ACK competition and correction

A real Search Adapter, HTTP transport, Runtime and shared ExecutionMemory test
reproduced the same failure class at a fixed 135,268,088-byte Node budget. Four
Native HTTP calls withheld headers while holding 100,663,296 bytes of initial
working reservation. The input promise was 33,554,432 bytes; real wire was zero.
A 42-item small write batch had a valid 7,166-byte success reply whose existing
JSON-validation reservation required 1,111,034 bytes. At send, only 196,608 bytes
remained. All 42 mock backend writes occurred once, then one budget rejection
produced 42 UNKNOWN / RESOURCE_EXHAUSTED results. The
[before receipt](typed-memory-results-2026-10-17/typed-memory-search-ack-before-21/receipt.json)
retains both failed old-source runs and their actual exits. The test budget is
calculated from the old known-input formula, so adding the correction cannot
raise its acceptance budget. This deterministic failure establishes the
mechanism; the original clean burst's precise rejection instant was not logged.

Normal mutation ACK costs now enter each Plan before Runtime dispatch. A single
fixed envelope cost is shared by the batch. Costs derive from supported normal
reply syntax, optional _type/forced_refresh/ingest_took fields, bounded physical
index and ID escaping, numeric evidence, parser nodes and chunked-buffer peaks.
They are workspace reservations, not new response-size restrictions.

Search keeps known input/frame/source ownership separate from this immutable
reply-scratch promise. Working is actual held input/framing/sources plus the
maximum of promised reply scratch and current scratch; it does not add both
copies of the scratch promise. Header callbacks obey the same bound. Dynamic
Lua/pre-read sources or frames cannot consume the promised ACK workspace.
Existing cleanup releases it after validation. Runtime scheduling, budgets,
count limits and mutation evidence remain unchanged; no retry is added.

At the identical fixed budget, the corrected known-length and chunked cases
retain all 42 tickets in the existing bounded queue with zero business HTTP and
zero rejection. Releasing one Native allows all 42 writes to become APPLIED,
each effect occurring once, while the other three Native calls remain held.
Header/body gates, 1/42/128-item maximum shapes, cancellation/drain and dynamic
source/frame accounting pass. Truly oversized unknown replies remain bounded
and keep correct pre-send NOT_APPLIED versus post-send UNKNOWN evidence.

The [focused receipt](typed-memory-results-2026-10-17/typed-memory-search-ack-focused-final/receipt.json)
records the final passing race checks (3.160 seconds) and preserved intermediate
failures: an early consumer-done ownership assertion, an insufficient framing
fixture and a caught temporary compiler error. Full affected-package checks
and a new clean Linux artifact's unchanged process gates remain pending.

The combined frozen correction now passes all affected package race checks:
Store 3.162 s, Server 20.511 s, Execution 2.249 s, App 8.179 s, CLI 18.590 s,
Mongo 8.829 s and Search 55.713 s, plus vet. The first complete Search run
failed two old assertions requiring exactly 64 KiB of mutation IOReserve;
those assertions now check the exact known-input plus per-item ACK cost and
shared envelope cost. Production code and the new ACK tests were unchanged
by those two assertion updates. The failed run is retained.

The [core receipt](typed-memory-results-2026-10-17/typed-memory-search-ack-core-receipt.json)
and [final backend receipt](typed-memory-results-2026-10-17/typed-memory-search-ack-full-final-receipt.json)
record unchanged source hashes and actual exits. The next acceptance step is
a new clean Linux CLI running the unchanged seven typed-memory cases, mixed
burst, diagnostics and slow-output regressions. No production qualification
claim or PR approval follows from these unit checks alone.

## Third clean candidate passes the unchanged milestone gates

Clean source `007bc59ed4c6de59a92e05a667da9b6a8efd6f64`, Linux/arm64 Go
1.27.1 CLI SHA256
`adc3f4a07ac381a78fddfb3c04b7a3d60a4f1176b28616259f2317f9e0c1cdf4`,
passes all original gates without overlays, retries or relaxed limits:

- Seven typed-memory process cases (31.243 s), including 256/512/1024 MiB
  complete 12 MiB and near-maximum Put/hash/Read/Delete/absence chains, original
  slow-reader pressure, cross-Store Typed/Native and Search large/many-field data.
- 2560/2560 mixed small calls (38.248 s), no budget rejection or business
  failure; independent Mongo 421/421 and Search 431/431 ID/n/pad/cardinality
  oracles. All logical ownership returned to zero within the original 5 s.
- 1092-series maximum static diagnostics; five original 32-by-8-MiB slow-Native
  rounds (21.481 s), healthy small calls 2.567–4.227 ms and cancellation
  ownership recovery 7.531–13.673 ms; three original small-query repetitions
  (3.141 s), healthy calls 3.158–8.467 ms.

All thirteen CLI containers independently reported no OOM and exit zero, and
were removed. Diagnostics and small-query checks use the real backends through
in-process app assembly; they are not presented as CLI cases. Slow-Native
rounds prove bounded retention and progress, not complete client-side large
response hashes. Typed slow-reader full successes do carry the original hash
and EOF oracle; explicit capacity failures are allowed only under that deliberate
pressure. All 348 source-file hashes and the binary hash remained unchanged.

The [combined receipt](typed-memory-results-2026-10-17/typed-memory-fixed-r3-validation.json),
[build identity](typed-memory-results-2026-10-17/typed-memory-fixed-r3-build.json)
and per-stage raw logs retain actual exits and distinguish all prior failed
candidates and diagnostic-only runs. This documentation-only evidence commit
changes no executable source from the clean artifact. Ingress, Lua, the final
600-second sustained run and fault qualification have not yet run on this
candidate. Overall production readiness is not claimed by this milestone.
