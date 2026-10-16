# Native memory ownership qualification, 2026-10-16

A single Store could retain nineteen 8 MiB MongoDB replies, hold all sixteen
backend execution slots and queue thirteen further calls even though the
1 GiB process had only about 200 MB of working set. Independent typed Read and
short Native probes both reached their three-second caller deadlines. This
reproduced in three rounds: the old output reservation reused the 32 MiB input
queue limit, and a full reservation kept the original backend slot.

The [before observations](native-memory-results-2026-10-16/native-output-budget-before-repeat.txt)
and [original test source](native-memory-results-2026-10-16/native-output-budget-exploratory-driver.go.txt)
retain that failure. They are exploratory controls, not clean release qualification.

## Shared memory ownership and actual backend IO

One concrete NativeMemory account belongs to the Node and is shared by every
local Store. Its byte budget is two thirds of the trusted recovery allowance
already computed by the existing memory Guard; its invocation limit is 1024.
Input queue policy remains 1024 entries / 32 MiB per Store and default backend
execution concurrency remains sixteen. There is no new timer, worker process,
configuration knob, public protocol change or automatic Native retry.

Each admitted invocation owns its input and a double-chunk publication window.
Before backend IO it also reserves the maximum reply working set; after IO,
that reservation becomes actual retained reply/header memory. Backend cleanup
precedes terminal publication. The input and publication account remains owned
until acknowledgement, cancellation or drain, including the terminal publisher.
Known-zero capacity and shrinking budgets never erase existing ownership.
Unknown samples preserve the last trusted bound; a cold unknown capacity
returns NOT_STARTED/UNAVAILABLE rather than running without a bound.

A separate IO allowance within the same total budget protects subsequent work:
48,000,000 bytes plus two Native chunks and 8192 bytes initially, increased when
a selected MongoDB server negotiates a larger reply. Settled resident replies
are limited to the total less that allowance. An IO reservation can temporarily
borrow it; a completed response still borrowing it remains explicitly accounted
as Borrowed while publishing. Promotion changes classification only after real
space becomes available. It never creates credit or discards a valid response.

MongoDB compression is disabled and the negotiated uncompressed maximum is
reserved before command bytes are sent. Retained BSON capacity is charged after
the reply arrives. HTTP response headers reserve expanding parser, metadata and
encoding space before the standard parser sees socket bytes, with eight bytes
per wire byte and 512 per line plus fixed buffers. Body reads reserve their
bounded transient buffers. No new fixed header-size quota is introduced.

Only actual MongoDB execution, HTTP exchange or HTTP body reads own backend
slots. Output Head, Chunk and terminal delivery own memory but no IO slot.
Further reads rejoin the existing FIFO continuation schedule. Temporary initial
IO estimates can converge while new Native work waits in the bounded input
queue; typed work can skip a memory-ineligible Native entry. A real exhausted
Native budget returns NOT_STARTED/RESOURCE_EXHAUSTED before backend IO, or
INCOMPLETE after an HTTP request has already started. Nothing replays a mutation.

## Retired transport accounting

Cancellation diagnosis found an additional grpc-go v1.83.2 boundary. On writer
exit, residual established stream readers can retain encoded buffers without
calling their pool Put. A diagnostic dependency overlay that only closed those
readers eliminated the residual account in five rounds; a late-DATA-only patch
was never exercised and did not fix the case. These are causal experiments,
not production dependency patches. Their [manifest](native-memory-results-2026-10-16/grpc-diagnostic-manifest.json)
and [observations](native-memory-results-2026-10-16/native-wire-ownership-exit-only.txt)
are retained.

Weir now ends the queued-byte account at native gRPC ConnEnd, after HandleStreams
has joined the connection writer. RPC cancellation, RPC End and raw socket
closure are earlier and do not return that credit. Late OutPayload notifications
attach to the same retired connection state. Native pool Put, connection
retirement and unreachable-storage cleanup finish the account exactly once.
The connection retains account owners without retaining RPC contexts or buffers.
This does not call Free or reuse encoded storage: physical orphan heap may remain
until ordinary Go collection, and actual RSS/cgroup admission still observes it.

A [wire-only overlay](native-memory-results-2026-10-16/wire-only-manifest.json)
kept the old Native scheduler unchanged. Its five rounds still failed all ten
healthy query probes, while connection retirement returned all Store/RPC/queued
credits in 13.6–17.1 ms without a GC cycle. This distinguishes the transport
accounting correction from the separate scheduling correction.

## Validation status

The first combined exploratory binary passed the stricter single-Store Linux
arm64 scenario: 32 simultaneous 8 MiB replies all reached their paused consumers,
with zero active/pending backend work. The Node owned 272.8 MB within its roughly
496 MB budget. Typed Read and short Native completed in 2.99 and 3.06 ms;
cancellation returned every Store/RPC/wire/Native account in 8.70 ms without a new
GC cycle. All slow calls remained alive during probes, with no overload rejection
or watchdog expiry. See the [raw candidate result](native-memory-results-2026-10-16/native-memory-candidate-output.txt).

Clean-source repetition, low-memory recovery controls, independent review and
CI are pending. Full process sustained-load and fault qualification remains a
separate milestone. The Native account bounds Native ownership, not every typed
pipeline allocation, process RSS or Go runtime memory. Actual resident admission
continues to protect the whole process. Infinite slow streams remain finite
resource consumers and can exhaust the actual budget.

The formal Linux artifact was built from clean commit
`adc4daab1dff2d369246801e416bb64183af63ae`, SHA-256
`789e15b5688725ed313abc9e9fea239e8adc8b8d37173514feb32eac13ccde2b`.
The [build identity](native-memory-results-2026-10-16/native-memory-clean-build.json)
records compiler flags and target. Full default race and vet passed before this
build; subsequent evidence-only commits do not change its production source.

Five formal clean-artifact rounds passed with race detection: all thirty-two
large replies reached their blocked consumers with zero active/pending IO.
Typed and short Native probes took 2.16–4.46 ms. Cancellation returned every
Store/RPC/wire/Native account in 8.63–14.88 ms, with GC cycles unchanged at six
in every round; no watchdog or overload/budget rejection occurred. All five
processes exited zero without OOM or owned backend connections remaining.
The [repeat log](native-memory-results-2026-10-16/native-memory-clean-output-repeat.txt)
and [summary](native-memory-results-2026-10-16/native-memory-clean-output-summary.json)
retain the values. Real MongoDB diagnostics confirmed exactly 1086 fixed Weir
series at sixteen local Stores. Small-response MongoDB, malformed-command and
Elasticsearch progress regressions also passed three rounds with default policy.

The first clean artifact also passed the real typed-Read pressure matrix:
256/512 MiB, automatic/off, three rounds each. Automatic recovery took
141.7–154.2 ms; all six off controls remained latched at five seconds. Shared
160/192 MiB pressure recovered in 155.3/160.3 ms. Low-allowance 344/368 MiB
controls completed every healthy read with one GC and 0.11/0.10 CPU seconds.
All sixteen processes had matching clean identities, exited zero without OOM
and left no owned CLI containers. The
[first-candidate summary](native-memory-results-2026-10-16/native-memory-clean-summary.json)
keeps these observations separate from the subsequent final candidate.

A further offline lifecycle regression found a full-budget continuation wait:
when no initial or body IO can converge, two Search-like Native invocations
could publish Head to fast consumers but wait indefinitely for their next
2 MiB body-read workspace. Typed queries still progressed, and the account was
honestly full; the missing behavior was a timely explicit incomplete result.
This exact Runtime lifecycle regression is synthetic, not a claim that a real
HTTP header reached the same estimate. Mixed retained replies or capacity
shrink can also create the full state. Final qualification must include the
no-converging-IO correction and a new clean artifact.

The final continuation correction distinguishes a converging initial/read IO
reservation from a full account owned only by waiting output. Only the former
waits in FIFO. The latter wakes the body-read continuation with a memory error,
without granting an IO slot, and Search returns INCOMPLETE/RESOURCE_EXHAUSTED.
Its [before failure](native-memory-results-2026-10-16/native-read-capacity-before.txt),
full affected-package race results and focused twenty-round regressions are
retained. New coverage includes real Runtime fast-consumer publication, both
initial and ongoing-read convergence, and an actual HTTP exchange that preserves
Head, one backend attempt and incomplete terminal evidence without body reads.
