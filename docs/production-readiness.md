# Weir production reliability milestones

Production readiness is being qualified through separately reviewed changes.
Passing one milestone does not approve the remaining resource-recovery and
Linux sustained-load boundaries.

| Milestone | Result |
| --- | --- |
| Reclaimable file cache and baseline verification | [PR 75](https://github.com/batchstream/weir/pull/75) merged after independent review and Linux CI |
| Native consumer progress and output ownership | [PR 76](https://github.com/batchstream/weir/pull/76) merged after new independent review, repeated race checks and Linux CI |
| Automatic recovery after Go memory bursts with default configuration | [PR 77](https://github.com/batchstream/weir/pull/77) merged after new independent review and Linux CI; initial isolated-cgroup evidence below |
| Recovery with shared cgroup memory outside the process | [PR 78](https://github.com/batchstream/weir/pull/78) merged after new independent review and Linux CI |
| Healthy queries with a full Native output reservation | [Shared Native memory ownership](native-memory-qualification-2026-10-16.md) implemented; clean qualification and review pending |
| Linux complete-process sustained mixed workload and failure recovery | Pending |

The [initial reliability study](production-reliability-2026-10-16.md) is a
historical baseline at revision `63bafdd`; its Native scheduling and cancellation
findings are addressed by PR 76.

## Earlier PR 76 output reservation and its boundary

The initial scheduler charged a backend execution slot while Native output
waited for the client. Thirty-two small MongoDB requests with zero HTTP/2 output
windows left sixteen running and sixteen queued, reaching the three-second
probe deadline despite the backend having finished its replies.

PR 76 made Native publication borrow a bounded output reservation and release its
execution slot while waiting on the consumer. Further backend work joins FIFO
with new requests before regaining the slot. The original batch remains owned
by the Runtime throughout cancellation, draining and shutdown. A delivered
terminal event completes without waiting to reacquire a backend slot; it must
be the final emission and cannot be followed by backend IO. Nothing replays the
opaque command or changes its completion evidence.

The independent output reservation uses the existing queue operation and byte
limits. It includes retained input, actual response working memory and the
current emission. MongoDB charges its retained BSON response; Search charges
headers, metadata and bounded IO buffers. Early errors use their actual small
working sets rather than reserving the largest legal reply. Response working
memory is released from the charge after backend cleanup and before terminal
publication, so a slow terminal event does not retain an old response peak.
One oversized
response can occupy an empty output budget alone. A full real output budget
retains the original execution credit and backpressure. With finite execution
limits this preserves bounded executing and paused work.

PR 76 added four fixed-vocabulary gauges for paused output count, bytes and
their limits. The current shared account removes the two misleading per-Store
output-limit gauges and exports thirteen fixed Node gauges/counters instead. Existing active-execution metrics exclude paused output, while
retained working memory and backend connection ownership remain observable.
Native invocation elapsed-time histograms still include consumer waits; inspect
the pause gauges when distinguishing backend execution from output delay.
Paused HTTP responses can retain sockets. The process/cgroup Guard still
controls overload admission, and OS/container limits remain the hard memory
boundary. Output accounting does not replace actual resident-memory observation.

## Earlier PR 76 cancellation correction

The new real-backend regression also exposed a cancellation retention chain:
an output watchdog timer retained the RPC context, which could retain orphaned
transport buffers. Active RPCs and Store resources could reach zero while the
encoded bytes remained reachable until the long output timer expired.

Cancellation now detaches that watchdog reference. A late stats notification
cannot install it again. Encoded credit still belongs to the actual transport
buffer until Free or GC cleanup; cancellation never returns still-owned bytes
early. The deterministic regression places the buffer in the context itself to
verify the reference cycle is broken. GC is triggered by that test to establish
reachability, not by production transport code.

## Verified scenarios

- Real MongoDB 8.0.32 and Elasticsearch 8.19.22: thirty-two zero-window Native
  responses remain alive while healthy typed reads and short Native queries
  complete. Status and full expected counter/padding contents are checked.
  MongoDB repeats the case with early malformed-BSON failures to catch amplified
  error-response accounting. Each case passes three rounds with race detection,
  a three-second probe budget and the default thirty-second output stall.
- Real HTTP/2 flow control: thirty-two four-MiB streams receive small periodic
  window updates across more than one shortened watchdog interval. Healthy
  typed and Native requests on that same connection complete without a watchdog
  close. A separate zero-window case verifies eventual watchdog cleanup.
- Store tests cover concurrency limits of one and sixteen, thirty-two paused
  small replies, oversized single-reservation behavior, true output-capacity
  backpressure, FIFO continuation ordering, cancellation and terminal completion
  while a different backend call occupies the only slot.
- A native Linux arm64 container independently runs flow-control and orphan
  watchdog regressions. Full local default/race suites, backend integration,
  server/application integration, vet and script checks accompany this change.

The [before](native-progress-results-2026-10-16/mongo-before.txt),
[real-backend after](native-progress-results-2026-10-16/real-after.txt),
[Linux](native-progress-results-2026-10-16/linux.txt), and
[cancellation failure](native-progress-results-2026-10-16/output-reclamation-before.txt)
logs retain the relevant observations. Timings in these functional probes are
local observations, not a general latency or sustainable-capacity guarantee.

## Reproduction

Prepare the owned MongoDB and normal-capacity Elasticsearch fixtures and a
matching Go 1.27.1 toolchain, then run:

```sh
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch GOWORK=off GOPROXY=off \
  go test -tags integration -race -count=3 -v \
  -run '^Test(Mongo|Search)SlowNativeOutputAllowsHealthyQueries$' ./internal/server
GOWORK=off GOPROXY=off go test -race -count=5 \
  -run 'TestManySlow|TestOversizedNative|TestNativeResume|TestDeliveredNativeEnd|TestNativeContinuous|TestNativeOutputStalls|TestCanceledOutputWatchdog' \
  ./internal/store ./internal/server
```

Whole backend fault suites run serially when sharing the same MongoDB fixture.
The test only owns its loopback connections, records and indexes; it does not
contact production services.

## Automatic recovery after Go heap bursts

The baseline found a different failure after a large allocation was released:
Go live heap dropped to about 1.3 MiB, but resident memory remained about 243 MiB
in a 256 MiB cgroup. Admission correctly remained closed because actual resident
pressure was still high. The missing piece was a runtime budget that encouraged
GC and page release before the process reached its recovery threshold.

PR 77 introduced explicit CLI management of the standard Go soft memory limit,
using 80% of the recovery-watermark budget, or 56% of detected capacity with the
default 80/70 watermarks. The remaining space below recovery is for the binary,
kernel buffers and other resident memory outside Go. The existing Guard sampler
updates this limit after complete trusted capacity observations; there is no
second timer and no periodic forced GC. Real process and cgroup observations
continue to decide overload. Holding a genuinely large live working set must
still close admission.

The process policy is optional for embedded Nodes and explicit for the CLI.
Any supplied `GOMEMLIMIT`, or an already finite runtime setting, takes priority.
After observing an external change, automatic management yields permanently.
Cleanup restores the prior value only when it still owns the current setting,
after node shutdown. Unknown capacity samples cannot loosen a previous limit.
Zero recovery watermarks are rejected because a running process cannot return
to zero resident bytes to leave its overload latch.

### Observed recovery and limits

The native Linux arm64 service ran on Docker 29.7.2, Linux 7.0.12-linuxkit,
Go 1.27.1, cgroup v2, two CPUs, and the cached image
`sha256:ad4f11f7c85f87c0a6d6c0fe492ad53f9cdcff6e8c88b881360c70eff701c67b`.
The host driver and real MongoDB 8.0.32 were outside the service cgroup.
The qualified CLI SHA-256 was
`dc48ac91a42903e289ef0b85ee64a496a50f6f0d26c4e1f8ec7e85a94465ffc6`.

| Experiment | Automatic policy | Explicit off control |
| --- | --- | --- |
| 256 MiB, 224 MiB touched Go allocation, three fresh processes | Recovered in 210.6–217.2 ms after release | Still latched after 5.004 s in the control process |
| 256 MiB CLI, one default Store, 14–15 live 12 MiB Native replies, three rounds | Recovered in 114.7, 123.3 and 774.1 ms | All three rounds still latched after 5 s |
| 512 MiB CLI, three default Stores, 30–32 live replies in automatic rounds | Recovered in 124.7–134.5 ms, three rounds | One round naturally recovered in 2.848 s; two remained latched after 5 s |

The live-allocation and live-reply stages remained overloaded while held; fresh
point queries returned `RESOURCE_EXHAUSTED`. Releasing replies canceled their
real SDK consumers. Recovery required a complete memory sample, working set at
or below 70%, a successful healthy read, and zero queued work, active work,
paused output, retained results, working bytes, RPC sessions and encoded output
bytes. All six automatic CLI rounds then completed another three seconds of
successful writes and value-checked reads. No test or production hook forced GC
or `FreeOSMemory` inside these service processes. All containers exited zero,
reported no OOM, and were removed.

The off control is observational: spontaneous runtime GC is permitted. In one
256 MiB control, live heap had fallen to 1.43 MB while RSS was 200.1 MB and cgroup
working set 221.3 MB, still over recovery. This reproduces resident idle memory
as a distinct issue from live object retention.

The CLI also followed actual in-place container limit changes of
512→256→512 MiB without restart. Its automatic Go limit changed
300647710→150323855→300647710 bytes within 75 ms of each update; readiness
remained healthy and shutdown exited zero. Unit/profile tests cover smallest
visible capacity, unknown samples, external ownership, repeated cleanup, and
successful or failed embedded assembly without global runtime changes.

Across the CLI matrix, nineteen three-second steady stages offered 20 operations
per second and completed 1140 successful puts/reads. Automatic stages used
0.13–0.18 process CPU seconds per stage, compared with 0.13–0.19 for off.
Combined put/read p99 observations ranged 35.8–86.9 ms before automatic bursts
and 36.4–53.8 ms afterward. Sixty samples make nearest-rank p99 equal the maximum;
the defaults include mutation batching waits. These short, deliberately low-load
probes establish recovery and functional continuity, not maximum throughput or
a statistical performance-regression claim. Sustained per-operation latency and
capacity qualification remains the next milestone.

Raw [CLI matrix](memory-recovery-results-2026-10-16/cli-final.txt),
[machine-readable observations](memory-recovery-results-2026-10-16/summary.json),
[heap auto](memory-recovery-results-2026-10-16/guard-auto.txt),
[heap off](memory-recovery-results-2026-10-16/guard-off.txt),
[dynamic CLI](memory-recovery-results-2026-10-16/dynamic-cli.json), and
[heap fixture cleanup](memory-recovery-results-2026-10-16/guard-cleanup.json)
retain the evidence. The initial CLI harness used TCP acceptance as readiness
and hit an HTTP EOF before workload started; its
[failed preflight](memory-recovery-results-2026-10-16/cli-first.txt) is preserved.
It now requires `/readyz=200` before collecting metrics or starting workload.
Full default/race suites, integration-tag vet, 38 script tests and the real
1103-series diagnostics test also passed.

To repeat the CLI matrix on the same owned Darwin arm64 + native Linux setup:

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 GOWORK=off GOPROXY=off \
  go build -o "$PWD/.testdata/weir-memory-recovery-linux" ./cmd/weir
WEIR_INTEGRATION=1 WEIR_MEMORY_RECOVERY_INTEGRATION=1 \
  WEIR_MEMORY_RECOVERY_BINARY="$PWD/.testdata/weir-memory-recovery-linux" \
  GOWORK=off GOPROXY=off go test -tags integration -count=1 -v -timeout=300s \
  -run '^TestCLIBusinessHeapRecovery$' ./internal/app
```

The owned Mongo fixture must already be available on `127.0.0.1:27028`; the
service uses `host.docker.internal` with direct connection. The test creates
unique records and containers and verifies their cleanup. It is skipped without
the explicit opt-in and does not pull images or access production.

## Recovery while the cgroup contains memory outside the process

Further qualification retained tmpfs shared memory while releasing real Native
business replies. With 128 MiB retained in a 512 MiB container, PR 77 recovered
in 1.887 s. With 160 and 192 MiB retained, all Store/RPC/output ownership returned
to zero, but the default policy remained overloaded beyond five seconds. Go
itself stayed below its full-capacity soft limit, so low-frequency business
probes did not trigger timely reclamation. The
[160 MiB](shared-recovery-results-2026-10-16/shared-pressure-160-old77.txt) and
[192 MiB](shared-recovery-results-2026-10-16/shared-pressure-192-old77.txt)
failures preserve this boundary. These exploratory artifacts report their old,
dirty build identity explicitly; they are not qualified exact-source releases.

The automatic policy now uses the process's remaining recovery allowance at
**every** trusted finite cgroup level:

```
external = max(working_set - process_RSS, 0)
process_allowance = max(low_watermark(limit) - external, 0)
```

The smallest allowance is combined with the host/process recovery budget, then
80% is assigned to Go. The actual admission thresholds remain unchanged. This
lets ordinary runtime GC respond when shared users consume part of the recovery
budget, even if Go remains below the old full-capacity limit. The reported
memory capacity retains its original meaning; separate fixed gauges expose the
remaining allowance and whether that observation is known.

Experiments with only a few MiB of usable allowance also exposed unproductive GC:
a complete CLI did 296 collections and used 0.57 process CPU seconds over about
three seconds of very low business activity. Automatic limits therefore have a
16 MiB runtime floor, capped by the original total-capacity target for smaller
process budgets. This prevents an unattainably small automatic limit from
continually collecting necessary runtime memory. Explicit runtime settings
still take precedence. A known zero allowance preserves the prior owned limit;
it never installs zero. Actual outside pressure can keep the Guard latched
until sufficient memory is released.

The [clean build receipt](shared-recovery-results-2026-10-16/shared-recovery-clean-build.json)
identifies source `b3672473309638fb60358d95ec828221d7fd9fc6`, Go 1.27.1 and
Linux arm64 binary SHA256
`e7fa6e60820af1cb93c4262cdf659b07a73ce9a5ba2b2e8845621bf5fa7ca33a`.
The worktree was clean before and after compilation. Every complete-process
case checked the mounted binary's hash and `version` output, including
`state=clean-commit` and `dirty=false`.

| Retained shared memory / 512 MiB container | Complete-process result |
| --- | --- |
| [160 MiB](shared-recovery-results-2026-10-16/shared-pressure-160-clean-after.txt) | 18 live 12 MiB replies crossed admission pressure; after cancellation recovered in 124.4 ms, final working set 352.1 MB |
| [192 MiB](shared-recovery-results-2026-10-16/shared-pressure-192-clean-after.txt) | 15 live replies crossed admission pressure; after cancellation recovered in 128.6 ms, final working set 339.3 MB |
| [344 MiB](shared-recovery-results-2026-10-16/shared-pressure-344-clean-after.txt) | 29 low-rate reads, no rejection; 16 MiB Go floor, one GC and 0.11 process CPU seconds over 3.09 s |

All three cases use default admission, batching, queue and transport settings,
retain their shared memory through recovery, verify all Store/RPC/output bytes
return to zero and exit normally without OOM. During the deliberate live-heap
pressure and recovery phases, the 160/192 MiB cases respectively used 0.77/0.64
process CPU seconds and 102/105 GC cycles; these are not steady-load capacity
measurements. Per-GC CPU is unavailable from this CLI's metrics.

The earlier floor experiments are labelled exploratory dirty builds in their
raw logs. An independent
[368 MiB Linux control](shared-recovery-results-2026-10-16/shared-allowance-368-floor-after.txt)
keeps outside memory above the recovery threshold: it stays latched with a known
zero allowance for five seconds, performs no GC in that observation window and
does not manufacture recovery. Unit coverage also distinguishes every visible
ancestor, capacity changes, missing observations and explicit runtime ownership.
The affected default race suites and vet pass; the
[real Mongo diagnostics check](shared-recovery-results-2026-10-16/shared-recovery-diagnostics-clean.txt)
passes with exactly 1105 fixed Weir series at the maximum local Store count.

The sustained mixed workload, full Native output boundary and backend-fault
milestones remain pending.
