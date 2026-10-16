# Native memory ownership qualification, 2026-10-16

The current clean Linux artifact is source
`98ea9adfee65a315ed737c00dad4640364f18648`, SHA-256
`1089f782c7ba580f06b5818eb80d953719740df6acbd5f6d2f9422bdc28fc214`,
Go 1.27.1 linux/arm64. This includes the request-parser ownership correction
found by independent review. Its [build receipt](native-memory-results-2026-10-16/native-memory-search-p1-final-build.json),
[progress validation](native-memory-results-2026-10-16/native-memory-search-p1-final-validation.json)
and [recovery summary](native-memory-results-2026-10-16/native-memory-search-p1-final-summary.json)
record identities, hashes, environment and unchanged acceptance bounds.

Status: corrected production sources passed independent core and transport
review plus clean Linux focused qualification. Final PR review and CI gate
merging. Sustained mixed-load and backend-fault qualification remain a separate
milestone. Earlier artifacts and failures below are preserved as history.

## Reproduced failure and corrected ownership

One default Store retained nineteen 8 MiB MongoDB replies, occupied all sixteen
backend slots and queued thirteen calls, despite only about 200 MB of working
set in a 1 GiB container. Healthy typed Read and short Native probes both reached
their three-second caller deadlines in three rounds. The old output reservation
reused the 32 MiB input-queue limit; once full, slow output kept its backend slot.
The [before evidence](native-memory-results-2026-10-16/native-output-budget-before-repeat.txt)
and [original driver](native-memory-results-2026-10-16/native-output-budget-exploratory-driver.go.txt)
retain that boundary.

A concrete NativeMemory account now belongs to the Node and is shared across
Stores. At most 1024 invocations share two thirds of the trusted remaining
recovery allowance from the existing memory Guard. Inputs, pre-dispatch reply
work, retained replies and double-chunk publication windows stay charged through
cleanup and terminal acknowledgement/cancellation. Store input queues remain
1024 entries/32 MiB; backend concurrency remains sixteen by default. No worker
process, extra sampling loop, public protocol change or retry is introduced.

Within this same total account, an IO allowance initially protects 48,000,000
bytes plus two Native chunks and 8192 bytes, growing for a larger negotiated
MongoDB maximum. Settled resident ownership fits below total minus IO allowance.
Initial IO and actual body reads can borrow the allowance. A completed response
still borrowing it stays explicitly Borrowed while publishing; classification
changes only when actual space becomes available and never changes total used.
Known-zero/shrinking capacity never erases existing ownership. Unknown samples
preserve the last trusted bound; cold unknown returns NOT_STARTED/UNAVAILABLE.

MongoDB reserves its selected server's uncompressed wire maximum before sending
command bytes, then charges actual retained BSON capacity. Wire compression was
already disabled before this change. HTTP socket reads reserve expanding header
parser/metadata/encoding space before the parser sees bytes: eight bytes per wire
byte plus 512 per line and fixed buffers. Body reads reserve bounded transient
workspace. This adds no arbitrary fixed response-header size/count quota.

Only actual MongoDB execution, HTTP exchange or HTTP body reads own backend
slots. Head, Chunk and terminal output own memory, without backend IO credit.
Further reads rejoin the existing FIFO continuation schedule. Temporary initial
or body IO can converge while a Native invocation waits in the bounded queue;
typed work can skip a memory-ineligible Native entry. A truly full account fails
explicitly before dispatch (NOT_STARTED) or after an HTTP attempt (INCOMPLETE),
without replaying the opaque operation.

A further [Runtime regression](native-memory-results-2026-10-16/native-read-capacity-before.txt)
found fast consumers could wait cyclically for body-read workspace when no IO
could free it. The correction distinguishes converging IO from ownership held
only by waiting output. The latter wakes the continuation with a memory error
without granting a backend slot. Coverage includes fast consumers, initial and
ongoing-read convergence, FIFO, full ownership cleanup and an actual HTTP
exchange retaining Head/one attempt/INCOMPLETE with no body read. The exact
full-budget Runtime state is synthetic; it is not a claim that a real HTTP
header reached that estimate. Mixed retained replies or shrinking capacity can
also create it.

## Connection retirement and physical memory

A separate grpc-go v1.83.2 diagnostic found established stream readers could
retain encoded buffers on writer exit without pool Put. Closing those readers
in a diagnostic-only dependency overlay removed residual accounts in five
rounds; a late-DATA-only overlay had no branch hits and did not fix the case.
The [diagnostic manifest](native-memory-results-2026-10-16/grpc-diagnostic-manifest.json)
retains patches, source hashes and controls. No dependency fork is shipped.

Weir ends queued-byte accounting at native gRPC ConnEnd, after HandleStreams
has joined the writer. RPC cancellation, RPC End and raw socket closure are
earlier and do not release live-connection credit. Late OutPayload attaches to
the retired connection state. Pool Put, retirement and unreachable-storage
cleanup finish the account exactly once. Connection ownership retains neither
RPC contexts nor encoded data. Retirement does not call Free or reuse storage:
physical orphan heap can remain until ordinary GC, and actual RSS/cgroup
admission continues observing it.

The [wire-only control](native-memory-results-2026-10-16/wire-only-manifest.json)
left the old scheduler unchanged: all ten healthy probes still timed out, while
all Store/RPC/queued accounts returned in 13.6–17.1 ms without a GC cycle. This
separates transport-accounting evidence from scheduling evidence.

## Prior clean-artifact checks (7787dc)

Five formal 1 GiB/two-CPU Linux rounds retained all thirty-two 8 MiB replies
with zero active/pending IO. Healthy typed/Native probes took 3.65–4.42 ms;
cancellation returned every Store/RPC/wire/Native account in 10.83–74.87 ms,
within the unchanged five-second bound. GC remained at six cycles in every
round; no watchdog, latch or budget rejection occurred. All five CLIs exited
zero without OOM or residual owned connections/containers. Real MongoDB
diagnostics confirmed 1086 fixed Weir series at sixteen local Stores. Mongo
valid/malformed and Elasticsearch small-progress cases passed three rounds.

Full default race/vet passed before the continuation correction; the final
Store/MongoDB/Search complete race suites, application/server/CLI race suites,
focused convergence/FIFO regressions times twenty and integration-tag vet pass.

## Preserved pressure-test failure and predicate correction

The earlier clean `adc4daab` artifact passed all twelve typed-Read memory cases
and four shared-memory cases. The subsequent `ab48b327` matrix passed 11/12:
512 MiB automatic-off round two timed out waiting for its first large response's
wire predicate. Six automatic cases recovered in 142.2–155.7 ms and all shared
cases passed. That [failed matrix](native-memory-results-2026-10-16/native-memory-final-recovery-matrix.txt)
and its explicit all_passed=false summary are retained.

Ten targeted original-predicate diagnostic repetitions did not reproduce the
failure: 140 large replies and 600 ordinary calls passed; first samples were
zero then 12 MiB plus 70 bytes. The earlier failure's specific cause remains
unconfirmed. Its failure-time RSS/RPC result was not captured, so the preceding
steady-state RSS cannot establish what happened during the timeout. See the
[diagnostic receipt](native-memory-results-2026-10-16/native-memory-wire-threshold-diagnostic-manifest.json).

The helper independently had an invalid global-delta premise. Dedicated static
64 KiB stream windows and no Recv keep all preceding 12 MiB messages owned until
cancellation; unrelated healthy responses can disappear between global samples.
The corrected predicate checks (count + 1) times 12 MiB of cumulative ownership.
KiB-sized healthy replies/framing cannot replace a missing large message.
The original three-second response bound, 32 MiB safety headroom, actual RSS/latch
evidence and five-second recovery gate are unchanged. Before/maximum/required
metrics and a bounded failure-only Recv are retained. This corrects an invalid
measurement predicate; it is not a claim that the old failure's cause is established.

This Native account bounds Native ownership, not every typed pipeline allocation,
process RSS or the Go runtime. Actual resident admission still protects the whole
process. Infinite slow consumers remain finite resource consumers and can exhaust
the real account. Production endpoints have not been accessed.

## Prior clean-artifact recovery qualification

The same `7787dc` artifact passed all twelve 256/512 MiB × automatic/off ×
three-round cases using the corrected cumulative predicate. Each establishes
actual 12 MiB typed-Read ownership and real RSS/cgroup overload before release.
Automatic recovery took 125.3–146.4 ms; all six explicit-off controls remained
latched at the fixed five-second observation. Shared 160/192 MiB cases retained
their outside memory and recovered in 163.6/141.1 ms. Shared 344/368 MiB probes
completed 28/29 healthy reads in about three seconds with 0.11 CPU seconds and
one GC each; these low-allowance probes do not claim a prior pressure latch.
All sixteen identities match the clean artifact; exits were zero, with no OOM,
owned connections or CLI container residuals. The
[final recovery summary](native-memory-results-2026-10-16/native-memory-qualified-summary.json)
links the five raw logs and preserves the earlier unexplained failure separately.

## Independent review and request-parser correction

Fresh review of PR79 head `9d62ad1` found valid Search HTTP input with 100,000
short unique headers occupied 8.70–8.73 MB of parsed retained heap for only
1.30 MB of raw input. Prepare allocated that map before admission, and execution
dropped its initial working estimate while the parsed request remained reachable
in Plan.Backend. Passing prior tests did not establish complete Native ownership.
The [before measurement](native-memory-results-2026-10-16/pr79-native-request-memory-before-manifest.json)
retains the independent probe, command, hashes and three observations. PR79 is
draft while the parser correction receives fresh artifact validation and exact-head
independent approval.

The independent serial real-backend race run passed MongoDB, server and app,
but Search had two explicit congestion-test failures against the normal write
pool (size two/queue 10000). These tests require native rejection under a bounded
pool; the raw aggregate result remains FAIL. Separate size-one/queue-one owned
OpenSearch qualification passed both tests three times under race: each create
round applied 256 and rejected 256, and each expression round applied two and
rejected six. Persisted version equals one plus the applied count; rejected create
IDs are absent and the 200 KiB counter padding remains intact. This temporary
OpenSearch fixture was removed after verification. The normal ES fixture was
unchanged. See the bounded-search summary and raw results in this directory.

The correction keeps only borrowed raw request bytes in the prepared plan. An
allocation-free framing scan computes a conservative bound for parsed and cloned
request headers, URL construction and parser buffers. The dispatch lease approves
this bound before the allocating HTTP parser or backend call runs. Request working
credit remains charged through slow Head/Chunk publication and is released only
after response Body.Close, request-body cleanup and context cancellation return.
The terminal ticket retains input bytes and its bounded output window, without a
parsed Header map. Response-header growth includes the still-held request bound.

Focused race checks three times and complete Search/Store/server race checks pass.
The 100,000-header preparation stays at seven allocations and three prepared plans
retain only hundreds of additional heap bytes. Deliberate insufficient admission
allocates 592 bytes and makes zero HTTP attempts. The slow Head keeps more than
61 MB of request working charged. Its unacknowledged terminal no longer owns the
outbound Request. Raw failed and successful test runs are retained alongside the
original independent before measurement.

A first-GC heap-drop assertion was invalid: Go's net/http headerSorterPool may
retain physical header sorting storage, and transport goroutine stacks may unwind
after Body.Close. The stable interface contract permits request reuse after
response Body.Close; the tests check that close/cancel boundary separately from
bounded weak-reference quiescence while the terminal remains unacknowledged.
Explicit GC is confined to these allocation/reachability diagnostics, never the
production path or process recovery qualification. No causal claim is made about
a test observer retaining the request, and no transport join or GC workaround is
added to production. A new clean-artifact validation follows this correction.

## Corrected clean-artifact qualification (98ea9ad)

All five new 32-by-8-MiB single-Store slow-output rounds pass. Healthy typed Read
and short Native probes took 2.45–11.43 ms, cancellation returned every logical
Store/RPC/wire/Native account in 8.64–13.75 ms, and GC remained at six cycles.
All rounds reached 32 paused, zero active and zero pending; there was no latch,
watchdog expiry or Native budget rejection. Diagnostics report 1086 fixed series.
Mongo valid/malformed and Elasticsearch small-progress tests pass three rounds.
Five CLI identities match the clean binary, all exit zero without OOM or residuals.

The same corrected binary passed all twelve 256/512-MiB automatic/off cases and
four shared-memory cases. Automatic recovery took 125.9–156.0 ms; the six off
controls remained latched at five seconds. Shared 160/192-MiB cases recovered in
164.5/157.4 ms while retaining their outside memory. Shared 344/368-MiB probes
completed 29/28 healthy reads, each using 0.10 CPU seconds and one GC in about
three seconds; they do not claim prior latching. All sixteen identities match,
all exits are zero, and no OOM or owned CLI residual remains. These results
supersede earlier artifacts as current qualification, without explaining away
the preserved ab48 pressure-predicate failure.

The fresh independent original 100,000-header probe also passes three race
repetitions against the correction: three prepared plans retained 432/1688/336
bytes; denial allocated 592 bytes and returned NOT_STARTED before any HTTP call.
Its [review manifest](native-memory-results-2026-10-16/pr79-native-request-memory-review-after-manifest.json)
retains the original input, permanent diagnostic overlay, command and raw output.
Current-source CI also passed default CGO0 tests, CGO1 race, both vet variants and
script checks before this evidence-only commit. Exact final-head CI remains a
merge requirement.
