# Ingress memory qualification, 2026-10-17

Status: **incomplete**. The original qualification failed. Two subsequent
instrumented runs localized the failure; neither is qualification or a passing
production result. The first clean repair candidate also failed A512; further
implementation and unchanged-profile acceptance are required.

## Preserved original failure

The clean Linux arm64 CLI was built from
`0288974d8b64c199da4f0d7fb7f03d8a33978eee`, SHA256
`58723c2fc4db3fb98840f46513be198d16c2670412cf2e0694f28c3adec886ad`.
The committed qualification driver was
`f2f69bf907ad70afd1cf2dfe583842fa2b50d559`; its ingress test SHA256 was
`941d8dbb37b1449dfdb0a8f6c33e5402eafc70b6ca97ab2c100b8c8f8df35c07`.
The driver includes test-only fixture changes absent from the runtime build.
The recorded dependency proof verifies those helpers are not CLI dependencies.

The first actual profile used an owned MongoDB backend, the public SDK
v0.16.0, a default CLI limited to 512 MiB and two CPUs, 32 concurrent distinct
documents of exactly 12 MiB BSON each, and one common five-second deadline.
There were no retries or application policy overrides.

All 32 calls returned `DeadlineExceeded` without a mutation outcome. Independent
per-ID backend reads found every document absent and total cardinality zero.
The original health client used a separate connection: Read took 15.327 ms and
Native took 15.766 ms. This alone did not establish health on the upload
connection. Successful metric samples showed no admitted execution inputs;
they did not prove full frames had arrived.

There was no OOM, the CLI exited normally, and final ownership reached zero
in 29.471 ms. One Docker observation timed out and remains unknown in the raw
evidence. Recovery using the original SDK was not run because the original
driver skipped it after a failure. The 1024 MiB case and partial-upload case
were not run after the first failure.

The [original manifest](ingress-memory-results-2026-10-17/ingress-memory-main-A-manifest.json)
and [raw output](ingress-memory-results-2026-10-17/ingress-memory-main-A-process.txt)
are preserved without editing.

## Diagnostic comparison

Both diagnostics kept the 512 MiB, two-CPU, 32-document, 12 MiB, five-second
profile. They added logs at existing gRPC read/marshal and Weir decode/submit
points, a host CPU profile, GC counters, health through the original SDK at
t0+1s, and recovery even after failure. These observations and extra operations
can perturb timing. The binaries identify themselves as `DIAGNOSTIC-overlay`
and `dirty=true`; they must not be presented as clean release artifacts.

The Go toolchain refused overlays directly targeting the module cache. The
diagnostic build therefore used a private same-version grpc v1.83.2 copy.
All 930 copied Go/module files were verified against the original cache.
The temporary modfile added only a local same-version replacement. Production
sources and the global module cache were unchanged. The initial tooling failure
and driver compile failure are retained alongside the repaired build receipts.

| Observation | C0: original fixed windows | C1: original gRPC automatic windows |
| --- | --- | --- |
| Diagnostic binary SHA256 | `e5116920123a3c5b76ba4589b6cc2df4b772a9b593f472514f4f353307a00fae` | `8810fdd392cac309d3aeaac33c1b688be2b94932066ff7630b3aa36c4ffc5db3` |
| Complete large bodies | 0/32 | 32/32 |
| Actual large body bytes read | 318,177,120/402,655,968 | 402,655,968/402,655,968 |
| Large body completion | All cancelled at about five seconds | t0+2.871 to t0+3.814 seconds |
| Large requests reaching decode and submit | 0 | 32 |
| Outcomes | 32 deadline errors, no mutation outcomes | 2 APPLIED; 30 ResourceExhausted errors without mutation outcomes |
| Independent large-document oracle | All absent, cardinality 0 | Exactly the 2 APPLIED documents present with full hashes |
| Health on upload connection | Read 15.957 ms; Native 16.649 ms | Read 12.184 ms; Native 12.187 ms |
| Same-SDK small-write/read recovery after failure | Passed | Passed |
| OOM and exit | No OOM; exit 0 | No OOM; exit 0 |

In grpc v1.83.2, both `InitialWindowSize` and `InitialConnWindowSize` set
`staticWindowSize=true`. Weir supplied 64 KiB for both, disabling the BDP
estimator. Relative to C0, C1 only removed these two options and changed the
diagnostic version label. This changed the large messages from incomplete to
complete reception. The measurements establish a flow-control contribution;
they do not measure individual WINDOW_UPDATE round-trip time.

C0 client encoding and queuing finished by t0+454 ms. Actual server reads show
that large messages, health requests, and recovery all used the same TCP peer:
large streams 1 through 63, health streams 65 and 67, recovery streams 69 and
71. The independent original health client used another peer. The
[stage comparison](ingress-memory-results-2026-10-17/ingress-BDP-DIAGNOSTIC-stage-comparison.json)
preserves peer identities, source lines, timestamps, and units.

Restoring automatic windows alone is insufficient. In C1, 17 errors reported
execution working memory capacity exhaustion and 13 reported process overload.
The RSS sample peaked at 502,255,616 bytes and the overload latch became set.
Cgroup `memory.events max=370` and `sock_throttled=239` show pressure events;
`oom=0` and `oom_kill=0` do not mean there was no pressure. Metric peaks are
asynchronous samples, not a simultaneous accounting snapshot. These rejections
must not be described as proven false overload at low actual memory.

GC counters do not establish GC as the sole latency cause. C0 total GC CPU was
about 1.000 s, including 0.755 s of marking performed on idle processors and
0.015 s of assist. Idle-mark CPU is not total process idle time. Host profiles
contain substantial unclassified external-code samples under macOS race
instrumentation and cannot establish that the host had no bottleneck.

## Repair under validation

The repair uses the existing native gRPC HTTP/2 parser and the Node's existing
execution-memory ledger. The same-version public grpc v1.83.2 source is pinned
under `third_party/grpc`; its original and changed file hashes, upstream module
sum, license, and explicit patch are preserved there. This adds a maintenance
obligation: future gRPC updates must reapply and independently review the narrow
receive-ownership patch. No second HTTP/2 implementation is introduced.

The message header reserves the receive/decode peak before its body can expand
the stream window. Actual DATA backing buffers retain their charge until their
last native reference is freed. Individual RPC readers may wait for capacity;
the shared connection reader does not wait. The original 64 KiB connection
window bottleneck is removed while unadmitted stream windows remain bounded.
Decoded input transfers atomically into the existing execution account, and
cancelled streams drain their transport references before credit is returned.

Per-connection parser/workspace ownership has a conservative 512 KiB promise
and a finite 64 KiB header-list bound. That promise is not measured RSS. Live
connection ownership remains visible after RPCs finish and returns only when
the connection's native reader and writer have ended. The seven ingress metric
families distinguish actual buffer bytes, message promises, waiters, and live
connection promises.

The first full offline race run of this implementation failed the original
64-session, 32-input continuous-read test: only 61 sessions remained active.
A diagnostic repetition reproduced immediate queue-capacity errors for input
already held by an ingress owner. The repair permits a live source on the same
ledger to wait on the existing queue-vacancy notification; unowned, released,
or foreign-ledger sources retain immediate rejection. Cancellation leaves the
source charged until it is actually discarded. The unchanged 64-session,
32-input, four-iteration, ten-second regression passed 20 consecutive runs
after this change. The original race failure and diagnostic failures remain
preserved. The final metadata-accounting version passed the complete offline
race suite, nested receive tests under race five times, ordinary and integration
vet, and Linux arm64 compilation. The 926 recorded source/provenance file hashes
were unchanged across these checks and independently matched the frozen files.
The native transport accounts header-map copies before their allocation, with
early refusal/cancellation retiring both handler and native stream ownership.
Real-backend acceptance remains pending.

CI and the release packager must carry the local patch. Their dependency guard
permits only the exact grpc v1.83.2 replacement inside the current source tree;
SDK/protocol or unrelated replacements remain errors. Offline bootstrap prepares
both module graphs, and CI runs the nested receive-ownership race tests. Release
exports include the public gRPC Go packages, patch and attribution; build-info
receipts record the exact local replacement without temporary export paths.
The 45 script tests passed in normal and optimized Python modes. Clean release
export/build reproducibility and final runtime qualification are still pending.

## First clean repair candidate: A512 failed

Candidate `ff4b3dbf588ba3d0c621472186126cad2a2479da` was clean and supplied both
the Linux arm64 CLI and the race driver. The CLI SHA256 was
`690b57b5a28357ce940e4e331356defc228e0782a53ff1f4fa933b0285921456`;
the actual race driver SHA256 was
`adad8bf3ed444fb9dd2ce1574853892524ed95b03fd43e74a366b69c1ac62872`.
All 926 source hashes and the binary were unchanged before and after the run.
A preliminary metadata pathname guard mistook the public gRPC package directory
`credentials` for a secret asset. That preflight failure happened before build
or runtime testing, was preserved separately, and was narrowed to the already
reviewed, hash-verified public Go files. No service source or test profile changed.

The first actual candidate run retained 32 exact 12 MiB BSON puts, default
512 MiB/two CPUs, the common five-second caller budget, and no retries:

| Client evidence | Count | Independent backend oracle |
| --- | ---: | --- |
| APPLIED with successful RPC | 10 | All ten IDs, contents, lengths and full hashes correct |
| Explicit NOT_STARTED/RESOURCE_EXHAUSTED with successful RPC | 15 | All absent |
| ResourceExhausted without mutation outcome | 5 | All absent; still unconfirmed for the client |
| ENHANCE_YOUR_CALM reset without mutation outcome | 1 | Absent; still unconfirmed |
| Canceled without mutation outcome | 1 | Absent; still unconfirmed |

These seven unconfirmed calls are blocking failures; absence in this controlled
oracle cannot create an acknowledgement. All calls finished before the common
deadline. The first test error was a 300 ms Docker-observer timeout, also retained
as an observation failure. It does not explain or excuse the seven business
failures. Successful metric samples never showed the overload latch set.

There were ten documents in the backend and no unacknowledged applied write.
Independent-client Read/Native health took 64.062/69.460 ms. Health through the
original SDK object took 5.927/7.136 ms; its exact TCP identity was not observed.
After failure, that original SDK successfully performed a small APPLIED put and
verified its read. Temporary ownership drained in 8.639 ms while two connection
promises remained; closing the clients returned every owner to zero in 9.935 ms.
The CLI had no OOM, cgroup final max/oom/kill counters were zero, exited normally,
and was removed. These recovery results do not turn A512 into a pass.

A1024, B512/B1024 and the Native burst were stopped without running. The
[candidate receipt](ingress-memory-results-2026-10-17/ingress-production-validation.json)
and [actual raw output](ingress-memory-results-2026-10-17/ingress-production-A-process.txt)
preserve this first failure. The next repair must address rejection after receive
capacity has been promised and must be tested as a new clean candidate.

The independent [ledger audit](ingress-memory-results-2026-10-17/ingress-production-A-ledger-audit.json)
identified two reachable rejection paths. The original message promise covered
only payload length for raw storage, while every DATA buffer also charges its
actual backing capacity and 512 bytes of owner metadata. A normally fragmented
large body could need new credit near its tail. Separately, an observed 32 MiB
input promise with two inputs held only 8,382,026 free bytes, less than the next
12,586,203-byte input. That temporary exhaustion returned no wake channel even
when the source was already owned on the same ledger. Both paths can reject
before execution without an acknowledged mutation outcome. Asynchronous samples
cannot assign these paths to every failed ID. Budget shrink alone does not
invalidate a prepaid receive operation: it is checked only when a charge grows.

The follow-up repair prepays payload plus an equal bounded fragmentation
allowance, one maximum native DATA backing and its owner; materialization and
decoded copies retain separate promises. A small independently delivered gRPC
header followed by a larger backing is covered. An initial follow-up added an
independent raw limit, but the full race suite rejected the unchanged 1200-record
relative-metadata stream: legitimate subsequent messages can already be inside
the native stream window. That attempt failed before clean-candidate runtime
qualification, and subsequent checks were stopped. The simpler correction uses
the greater of the prepaid raw promise and actual raw ownership. Additional real
DATA owners must still pay the same finite Node ledger before queuing; they
cannot consume the separate decode promise. No additional forward-buffer pool or
per-RPC maximum-window promise is introduced. Live same-ledger decoded inputs
may wait for existing leases to return temporary input credit, using the existing
runtime notification loop. Unowned, foreign, closed and intrinsically impossible
inputs retain immediate refusal. A completed receiver preserves its terminal RPC
status; an outstanding receiver is still cancelled and joined for cleanup.

Focused race tests passed five consecutive runs, including the unchanged
64-session small-read boundary. The existing two-message wait unit fixture used
24 MiB messages under a 192 MiB observed allowance. With the complete receive
peak, 24 MiB no longer fits alone. It now uses 18 MiB under the same allowance to
preserve the original property: either message fits alone, but both cannot fit
together. The actual 32-by-12-MiB A profile and all runtime deadlines are unchanged.
The focused successes did not override the full-suite failure above. The corrected
R3 source then passed the complete offline race suite, nested gRPC receive race
tests five times, Docker-observer integration race tests, ordinary and integration
vet, and a Linux amd64 build. All 931 source/provenance hashes matched before and
after, and an independent audit matched every current file and the pinned gRPC
manifest. The actual clean Linux arm64 CLI acceptance is still pending.

The Docker observer now queries the owned local Unix-socket Docker Engine API
directly instead of spawning a CLI every sample. Its 300 ms timeout and sampling
cadence are unchanged. Missing, malformed, oversized, redirected, incomplete or
timed-out observations remain failures; this changes no business load, capacity,
caller deadline, success criteria or retry policy. The original observer failure
remains preserved.

## Second clean repair candidate: ordinary puts passed; partial-upload health failed

Candidate `262f3f37b669ebd751a43269c7a3bfd90086857f` supplied the clean Linux
arm64 CLI, SHA256
`466dc0730c9f81143cad8125745d5e6e915604f25864e865abc8adc4b8b9d10b`.
The actual arm64 build and both race drivers used that commit without an overlay
or application overrides. The qualification agent froze all 931 source files,
564 pinned gRPC files and the CLI unchanged after testing.

The original ordinary-put profile passed at both capacities. At 512 MiB,
12 writes were APPLIED with complete hashes and 20 explicitly returned
NOT_STARTED/capacity. At 1024 MiB, 21 were APPLIED and 11 explicitly returned
NOT_STARTED/capacity. Every call had a successful RPC and a confirmed outcome;
no call was ambiguous. Independent ID/content/hash and cardinality checks matched
all applied effects and all rejected absences. This is bounded-capacity behavior,
not proof that all 32 large writes can succeed at these capacities.

The first actual B512 run failed small-request health. At t0+231 ms, all
32 incomplete uploads remained pending, with 34 live connections, three message
promises, 29 waiters and zero execution inputs or backend effects. Independent
Read and Native calls and Native on one of the same upload HTTP/2 connections
returned ResourceExhausted in about 1.7 ms. Read on that same connection succeeded
in 3.189 ms. These are capacity errors, not caller or Docker-observer timeouts.
A subsequent asynchronous sample showed one short execution lease with the
automatic queue promise occupying every remaining resident byte, although RSS
was about 65.7 MB and the overload latch was clear. This supports a queue-promise
contention path; it does not identify the owner of each error. The
[root ledger audit](ingress-memory-results-2026-10-17/ingress-production-v2-B-root-ledger-audit.json)
records the exact sample and the pending causal check.

All uploads were intentionally cancelled at the original three-second boundary;
no complete request frame or mutation acknowledgement was sent, and every large
ID was absent. Temporary owners drained in 14.372 ms while all 34 connections
remained open. Closing the upload connections, same-SDK small-write/read recovery,
and final ownership zero passed. There was no OOM or cgroup max event, and every
owned CLI exited normally and was removed. The A512/A1024 successes are retained;
B1024 and the Native burst were stopped without running, and no failed profile
was retried. This candidate remains unqualified.

## Required acceptance

The repair must account for raw transport buffers before business admission,
bound decoding peaks, and retain ownership until actual buffer release or
safe handoff to the existing execution ledger. It must support cancellation,
unary and streaming requests, and healthy traffic on an upload connection.
Small inputs must be charged according to their actual size; reserving a
maximum-sized message for every RPC would introduce false capacity failures.

The ordinary-write profile retains its original document count, size,
512/1024 MiB capacities, CPU limit, common deadline, independent backend hash
oracle, and explicit-outcome requirements. A transport error without a mutation
outcome remains unconfirmed by the client, even when the backend oracle proves
absence in this particular controlled run.

The original partial-upload probe required all 32 almost-complete prefixes to
be written before health testing. Correct early backpressure can prevent this
condition. Before any actual B run, the partial-upload acceptance was changed
to isolate incomplete-input cancellation. It retains 32 legal 12 MiB requests,
512/1024 MiB, two CPUs, a fixed three-second missing-tail hold, and a common
five-second caller upper bound. The final 16 KiB are never released. At t0+3s
the driver intentionally cancels every upload, joins within one second, checks
all 32 IDs and cardinality for zero effects, requires all ingress and execution
ownership to reach zero within five seconds, and checks fresh SDK recovery.
Observed caller cancellation is expected for this profile; successful writes,
unexplained errors, or deadlines are failures. This does not invent a mutation
outcome for a request whose complete message was never transmitted.

During the missing-tail hold, real server ingress-buffer, message, and waiting
metrics must establish bounded ownership and backpressure, with no business
effects. Small Read and Native operations use one of the same official HTTP/2
ClientConn objects as an incomplete large request. They must finish within
their original three-second caller budget and before the hold ends. Local
socket writes establish only which connection has an incomplete upload, not
how much data the server retained or which global message lease belongs to it.
The full ordinary-write throughput and acknowledgement requirements remain in
A. No partial-upload runtime result existed when this change was selected.

Original failure and diagnostics have not run the remaining Lua allocation,
final 600-second sustained-load, or final fault-recovery acceptance. Their
results must be obtained from a clean final binary before a production-readiness
claim.

## Evidence inventory

[evidence-index.json](ingress-memory-results-2026-10-17/evidence-index.json)
records exact file hashes and original locations. Original receipts retain their
absolute paths; the inventory maps each basename into this directory. Sources
ending in `.go.txt` are inert evidence. Executables, host profile binaries, and
redundant public dependency copies are excluded; their hashes and reconstruction
inputs remain recorded. Raw files and saved patches are not whitespace-normalized.
