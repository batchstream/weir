# Lua memory qualification, 2026-10-17

Status: **failed baseline; allocation repair passes offline checks; constrained
CLI qualification pending**. The ingress objective was merged as
PR #82; it does not bound arbitrary allocations inside the existing Lua VM.
Overall production-readiness acceptance is incomplete.

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

## Required repair and acceptance

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
Neither probe has yet qualified this repair in a constrained process.

Lua remains in the main Weir process. The repair must prevent this process-fatal
allocation and cover the exposed VM, compiler and bridge allocation paths, with
real ownership on the existing Node memory budget. Sampling, garbage collection,
or a process-wide soft heap limit alone does not prevent a single oversized
allocation. Existing Lua 5.4 behavior, safe helper whitelist, CPU/cancellation
limits, normal atomic-transform semantics and backend failure evidence must be
preserved.

The fixed allocation profile must finish within the original caller budget with
a confirmed Keep or an explicit safe NOT_STARTED/NOT_APPLIED capacity result.
The process must survive, the independent target must remain unchanged, fresh
healthy operations and original-SDK read/write recovery must pass, and closing
the SDKs must return all execution, RPC, wire, ingress and connection owners to
zero. Final 600-second sustained load and fault qualification remain pending.
