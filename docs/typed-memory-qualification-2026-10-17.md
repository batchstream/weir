# Typed execution, result and network memory qualification

Status: a production blocker is reproduced. The correction is in progress;
this report does not qualify default production use. Local real backends only.

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
