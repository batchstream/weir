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
