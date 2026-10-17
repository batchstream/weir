# Lua memory qualification, 2026-10-17

Status: **allocation repair, including the independent review's numeric-conversion
fix, passes offline, constrained CLI and shipping-artifact qualification**. The
latest qualified source is `9c23c4f58f789aa54b0162b59234f77e5cb0c6a5`.
The original OOM baseline remains failed. The ingress objective was merged as
PR #82; it does not bound arbitrary allocations inside the existing Lua VM.
PR #83 merged after both final-head reviews and CI passed. The later
[complete-process qualification](production-process-qualification-2026-10-17.md)
also passed its full sustained-load and fault gates with this shipping runtime.

The clean source was `a6ccb9b8e0b7397cc7e7e796e0d8b1bd0b8855fc`, consisting of
main merge `6d8c3a5e2cd71e62106a466e9f7123308ee9eb8d` plus the opt-in Lua probe.
The actual Linux arm64 CLI SHA256 was
`1e50a806e1f363115dfa2ac744e5b4357cbf9e98f547a97022297807d5818137`.
Build-info verified the target, exact revision and unmodified checkout. The race
driver used the same clean source; all 957 recorded tracked source/provenance
hashes and the pinned native gRPC source matched before and after. The larger
inventory adds tracked scripts and the new Lua test to the prior 932-file set;
all prior source hashes remained unchanged.

## First actual allocation probe

The CLI ran as the one Weir process in an owned 512 MiB, two-CPU container using
default Store policy and automatic memory management. Only that limited CLI
executed the Lua source; the host driver never interpreted it. Normal small Lua
AtomicTransform first returned APPLIED within its three-second caller budget;
an independent MongoDB read verified the incremented number, unchanged payload
and complete hash.

The original 96-byte script doubled a string 29 times, with a final logical
string size of 536,870,912 bytes, then requested `weir.keep()`. Its original
three-second caller deadline, workload and zero-retry policy were unchanged.
The RPC returned Unavailable/EOF after 169.917 ms without a mutation outcome.
Independent Docker observations established OOMKilled=true and exit code 137.
This is a process-fatal failure, not a safe memory-capacity rejection or a
successful Keep.

The target document remained 1,076 bytes with its complete SHA256 unchanged
before and after. That independent oracle cannot create a client acknowledgement.
Fresh Read/Native, same-original-SDK small Put/Read recovery, and three bounded
ownership-recovery checks failed because the process was dead. Normal exit and
ownership recovery were not established. There were six successful metric
scrapes and 604 failures; asynchronous last-successful RSS/cgroup values are not
an OOM peak and must not be compared as one snapshot. Docker observations,
rather than an inferred sampled peak, establish the OOM.

The owned CLI container was removed. The fixture's original three-second
MongoDB namespace Drop timed out; namespace cleanup was unconfirmed in the original run. The
suggested database name derived from a driver PID was not sufficient ownership
proof for deletion. No retry, diagnostic rerun or extra deletion was performed
inside this baseline.

A separate cleanup follow-up verified the exact local mongod executable, loopback
port, replica set and fixture owner marker. A read-only driver then verified the
one collection, three exact IDs, and complete raw BSON lengths and SHA256 hashes
against the original seed/control records. Only that proven namespace was dropped;
a subsequent independent database listing confirmed absence. The original Drop
timeout and failed qualification remain unchanged. The
[follow-up cleanup receipts](lua-memory-results-2026-10-17/cleanup-followup-index.json)
record this later action separately.

[Original validation receipt](lua-memory-results-2026-10-17/lua-production-baseline-validation.json)
and [raw output](lua-memory-results-2026-10-17/lua-production-baseline-process.txt)
retain all failures and cleanup uncertainty. The evidence index records original
paths, bytes and SHA256 hashes without normalization.

## Allocation repair

The repair retains Lua in the main process and pins a limited source patch of
`iceisfun/golua v1.1.1`. One cumulative allocation budget covers parsing,
compilation, the restricted VM, allowed helpers and document conversion. It
reserves storage before allocation on the existing Node ledger. Protected Lua
calls cannot catch capacity refusal, and backend result ownership retains the
reservation through validation and encoding. Existing capacity is reusable;
new backing blocks and temporary allocations remain charged for the evaluation.
This is a conservative allocation budget, not a measurement of live Go heap.

The frozen repair passed the complete default tests with and without race,
integration-tag race compilation, both vet modes, six nested Lua package race
checks, and the normal benchmark. The 1,052 recorded source/provenance hashes
matched before and after the six root checks. Packaging script tests passed in
normal and optimized Python modes. Four target builds record final compiler
frames for the frontend, VM, allowed library code, Weir host wrappers, Node
ledger and Go sort. These are supporting checks, not process qualification.

The original cancellation test initially reached the standalone allocation
budget before its scheduled cancellation. That first failure is preserved. Its
fixture now supplies a finite test ledger with a larger limit so the existing
script, cancellation timing, CANCELLED assertion, healthy peer assertion and
completion bound continue to test cancellation independently of allocation
refusal. Separate tests require allocation refusal to remain terminal.

The unchanged normal benchmark has a cost for allocation checks: the median
one-field evaluation changed from 13.063 to 20.283 microseconds, and the
100-field evaluation from 50.080 to 68.157 microseconds. All three samples,
including the original 106.388-microsecond outlier, are preserved. These
microbenchmarks do not establish backend latency SLOs. Six allocation diagnostics
reserved more than their measured Go allocations; the tiny case reserved
533,425 bytes, below the one-MiB regression limit. These diagnostics can reveal
omissions but cannot prove an upper bound for all programs.

[Offline evidence](lua-memory-results-2026-10-17/offline-repair-index.json)
records exact commands, source hashes and unmodified output. The original
269-line CLI probe remains byte-for-byte unchanged. A separate opt-in probe adds
both-backend checks for protected-call exhaustion, output-DAG expansion, long
errors, compiler/comments, repeated existing-key updates and quoted controls.
The final static stack audit covers all four release targets and the restricted
Weir call graph. It checks guarded parser/compiler recursion, all nine native or
closure VM entries, nested Lua comparators through Go sort, and the host/ledger
tail. Its limits and compiler-frame evidence are preserved; it does not claim
that this library is an arbitrary Go heap sandbox.

## First allocation repair qualification

The clean qualified source is
`a01d523e6fab728d0e48f3ff84d908710559aa65`. Complete source/provenance inventories
matched before and after the actual runs. Both probes ran in separate owned
512 MiB, two-CPU Linux arm64 CLI processes using automatic memory management,
default Store policy, original three-second callers and no retries.

The original unchanged 96-byte string-doubling probe returned
NOT_APPLIED/RESOURCE_EXHAUSTED in 33.395 ms on the candidate. Normal control
transforms, complete independent BSON hashes, fresh health requests and the same
original SDK's Put/Read recovery all passed. All temporary Node/Store/RPC/ingress/
wire/Q/I owners and connections returned to zero, the process exited normally
without OOM, and independent checks confirmed fixture/container absence.

The separate supplement passed all six fixed cases on each of MongoDB and
Elasticsearch. It verifies precise timely outcomes, full raw document hashes,
same-SDK Put/Read recovery, independent health and final complete backend contents.
Capacity exhaustion through pcall and shared-output expansion returned safe
NOT_APPLIED/RESOURCE_EXHAUSTED; long errors returned bounded INVALID_ARGUMENT;
ordinary transforms, reused table keys, long comments and control quoting passed.

The first supplement retained an incorrect two-connection idle expectation and
timed out while waiting after otherwise successful operations. Public pinned SDK
source independently establishes four channels: two SDKs each open both Stores.
Only those three live-connection expectations changed to a named four-channel
constant; the final closed-SDK zero-owner assertion is unchanged. The first
failure remains preserved. Exact container ownership and every BSON/JSON record
hash were independently audited before separate cleanup, with absence confirmed.

Two independent clean exports reproduced all four platform binaries and archives,
including both patched dependencies' licenses and provenance. The actual shipping
Linux arm64 CLI SHA256 is
`24cb1abf6d5ea9c8623c470cf2ffacccb517e6e1e8a4ab16898ad46807524126`.
The candidate CLI includes Go VCS metadata; clean exported release builds use
their recorded source revision and intentionally have different bytes. The exact
shipping archive binary therefore ran both probes separately, using the same
verified race driver and strict assertions. Both passed: its original allocation
probe safely rejected in 31.496 ms, final owners/connections reached zero within
15 ms, no OOM occurred, and both CLI processes exited zero. All owned namespaces
and containers were independently absent after cleanup.

[Repair qualification evidence](lua-memory-results-2026-10-17/repair-qualification/evidence-index.json)
preserves the first failed supplement, cleanup, corrected candidate, reproducible
packages and exact shipping artifact runs. Large raw logs are gzip-compressed
without changing their decompressed bytes; the index records both hashes.

## Independent review follow-up and latest qualified artifact

The independent runtime reviewer found an additional allocation omission on
`4f3f90fa0ecb43f978925d49447df24028cc3c68`: numeric `for` and table.concat
coercions could repeatedly invoke strconv without reserving its error-input
copies. With a 65,539-byte numeric string, 128 conversions and a one-MiB quota,
the old code returned Keep while charging below 660 KiB and allocating about
9.5 MiB. This is evidence of an allocation-budget bypass, not an observed OOM.
That request-changes review and its original reproduction are preserved.

The final repair uses one private numeric parser for both public unbudgeted
values and VM-owned values. Each actual strconv attempt reserves its possible
input clone and NumError before the call; the hexadecimal suffix is reserved
only when that fallback runs. All reachable loop, arithmetic, index, select,
ipairs, format, helper and concat coercions use the owned path. Duplicate broad
reservations and a needless whole-string lowercase copy were removed. Regression
tests cover strict quota rejection, protected-call termination, wide-quota
success, unchanged Lua semantics and cancellation.

The final clean source `9c23c4f58f789aa54b0162b59234f77e5cb0c6a5` passed all six
root checks, nested race checks and four-platform frame builds, with 1,055
recorded source/provenance hashes unchanged during root checks. The normal
benchmark medians were 18.732 microseconds for one field and 74.826 for 100 fields;
all samples and earlier measurements remain available.

The actual CLI supplement now adds numeric-for and numeric-concat, bringing it
to eight cases per backend, 16 in total. The candidate and exact shipping binary
both passed the original unchanged OOM probe and all 16 cases with original
three-second callers, full raw oracles, same-SDK recovery, final zero owners,
normal exit, zero OOM counters and independently absent fixtures/containers.
Two fresh exports again reproduced all four binaries and archives. The latest
qualified Linux arm64 shipping SHA256 is
`16669a2cd593d523a8bb52446a1140ea157a9224613ee5a34f37fdcbbe8d16d6`.
Its original probe safely rejected in 33.383 ms; its four numeric cases rejected
in 189.735–242.556 ms. The Linux CI for this exact source passed all stages.

The first Linux CI failure concerned a raw gRPC deadline assertion. The test now
starts Recv immediately and records the termination time before opening the
other RPC. It requires the original caller's DeadlineExceeded cause and a return
within the original deadline through one second afterward before accepting
native wire Canceled. Early cancellation, other statuses and late completion fail.
Both stall cases still require Canceled, and connection survival, APPLIED and
no-replay checks are unchanged. Production cancellation code was not changed.
Public upstream source shows the competing timer path; 1,000 upstream attempts
and 80 existing-test diagnostic iterations did not reproduce the CI event, so
the record does not claim an experimentally established cause.

[Numeric follow-up evidence](lua-memory-results-2026-10-17/numeric-followup/evidence-index.json)
contains the original review finding, first CI failure, independent test review,
root regression checks, complete candidate/package/shipping qualification and
compiler-frame comparison with its stated boundaries.

The subsequent [final complete-process qualification](production-process-qualification-2026-10-17.md)
passed the 600-second sustained load, declared latency gates and fault recovery.
The allocation objective remains a separate bounded claim within that report.
