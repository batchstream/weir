# Weir production reliability milestones

Production readiness is being qualified through separately reviewed changes.
Passing one milestone does not approve the remaining resource-recovery and
Linux sustained-load boundaries.

| Milestone | Result |
| --- | --- |
| Reclaimable file cache and baseline verification | [PR 75](https://github.com/batchstream/weir/pull/75) merged after independent review and Linux CI |
| Native consumer progress and output ownership | Implemented in this change; evidence and acceptance below |
| Automatic recovery after Go memory bursts with default configuration | Pending |
| Linux complete-process sustained mixed workload and failure recovery | Pending |

The [initial reliability study](production-reliability-2026-10-16.md) is a
historical baseline at revision `63bafdd`; its Native scheduling and cancellation
findings are addressed by the current change.

## Native output progress and resource boundaries

The initial scheduler charged a backend execution slot while Native output
waited for the client. Thirty-two small MongoDB requests with zero HTTP/2 output
windows left sixteen running and sixteen queued, reaching the three-second
probe deadline despite the backend having finished its replies.

Native publication now borrows a bounded output reservation and releases its
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

The four new fixed-vocabulary gauges report paused output count, bytes and
their limits. Existing active-execution metrics exclude paused output, while
retained working memory and backend connection ownership remain observable.
Native invocation elapsed-time histograms still include consumer waits; inspect
the pause gauges when distinguishing backend execution from output delay.
Paused HTTP responses can retain sockets. The process/cgroup Guard still
controls overload admission, and OS/container limits remain the hard memory
boundary. Output accounting does not replace actual resident-memory observation.

## Cancellation and encoded output ownership

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
