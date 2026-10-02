# Resolve/direct-routing validation

This report covers the breaking discovery refactor on
`randy/resolve-direct-routing`, using Darwin/arm64 Go 1.27.1 and task-owned
loopback fixtures. It preserves a single Weir process with in-process Lua.
Production services and Kubernetes clusters were not contacted.

## Default and static validation

The complete default Go suite, complete race suite and CGO-disabled suite passed.
Ordinary and integration-tagged vet passed. All integration-tagged packages
compiled. Integration-tagged capacity-helper race tests passed. Repeating
`scripts/generate.sh` preserved every generated Go file's SHA-256 hash.
Python helper tests passed with the compiled completion fixture: 123 tests in
ordinary mode with no skips, and 123 tests under optimization with 29 intentional
assertion-related skips. The CI workflow repeats these checks on native Linux
with fixed offline tools.

Focused tests exercise arbitrary seed initialization, multiple Stores, round-robin
direct traffic, DNS changes while a previous connection is healthy, DNS refresh
while the seed is unavailable, ownership changes, expiry/conflict invalidation,
cancellation, cleanup and non-replay after an acknowledged write loses its final
transport reply. Seed business-request counters remain zero.

Directory tests verify periodic convergence over real loopback gRPC, bootstrap
self-selection with fresh connections, same-group unions and different-group
conflicts, shutdown withdrawal, incarnation changes, no same-sequence lease
extension or resurrection, finite replay watermarks and live-node bounds, and
atomic rejection of invalid request/response state. Listener tests verify control
admission remains available when business sessions/application connections are
full, and only the intended RPC methods are exposed on each listener.

## Real processes and backend validation

Task-owned MongoDB 8.0.32 replica-set and Elasticsearch 8.19.22 fixtures were used.
Three independent executable processes exercise A learning Store ownership from
B, resolving the addresses of C, and high-level client writes/reads persisted at C.
Raw business requests at A are rejected before backend execution. Replica
withdrawal/restart, direct distribution and A/AAAA address changes are verified
with independent processes. SIGTERM readiness and shutdown remain covered.

A blackhole peer holds an Exchange during shutdown while an admitted MongoDB
mutation completes. Runtime draining starts promptly and directory withdrawal
runs alongside business draining within the shared shutdown deadline.

Real Route backend acceptance verifies exact 2 MiB records, Lua/partial outcomes,
acknowledged/reply-loss evidence, finite native reply loss without replay, and scan
continuation after the original process is replaced, on both MongoDB and Search.
Large-record fixture adapters explicitly declare the required 2 MiB read limit.

Commands:

```sh
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
go test -race -tags=integration -count=1 -timeout=180s ./internal/app \
-run '^Test(IndependentWeirProcesses|EndpointIndependentProcessesDistributionReplacement|EndpointDNSAcrossProcesses|DirectoryWithdrawalDoesNotDelayAdmittedWriteDrain|DiagnosticProcessSIGTERMReadinessBeforeExit)$'

WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
go test -race -tags=integration -count=1 -timeout=180s ./internal/server \
-run '^TestRoute(Mongo2MiBRecordLuaScanAndPartialBatch|MongoAppliedWriteAndNativeReplyLossAreNotReplayed|Search2MiBRecordAndAppliedReplyLoss|MongoScanContinuesOnNewInstanceAfterOriginShutdown|SearchScanContinuesOnNewInstanceAfterOriginShutdown)$'
```

## Direct slow-consumer resources

`WEIR_ROUTE_MEMORY=1 go test -count=1 -timeout=180s ./internal/server
-run '^TestRouteMemory200MiBDirect$' -v` uses one independent executor process,
real TCP/HTTP2/gRPC and a deterministic adapter producing legal 2 MiB Events.
Consumption pauses for 250 ms then paces every frame by 1 ms. This measures
transport/runtime memory, separately from the real-backend acceptance above.

| Measurement | 100 records / 200 MiB | 400 records / 800 MiB |
| --- | ---: | ---: |
| Time | 5.370 s | 20.563 s |
| Complete response frames | 3,400 | 13,600 |
| Failures | 0 | 0 |
| Executor peak HeapAlloc | 39.93 MiB | 41.89 MiB |
| Executor peak RSS | 72.47 MiB | 81.45 MiB |
| Post-GC HeapAlloc | 0.90 MiB | 0.96 MiB |
| Peak unfinished IDs | 8 | 8 |

All live RPC/input/result/workspace and publisher occupancy returns to zero.
Four times the transferred volume does not produce proportional retained heap.
Peaks are sampled every 50 ms and aggregated by field. RSS keeps allocator
high-water pages; this is bounded-workload evidence, not an OS memory ceiling or
a production throughput benchmark.

## Scope of evidence

URI affinity is absent. The protocol is independent of Kubernetes; sample manifests
use a normal initialization Service, a group headless business Service and an
explicit generic environment source for each instance's full peer address.
No live Kubernetes/HPA, OpenSearch, generated TLS/authentication, production,
packaged-image or Linux-cgroup profile was executed for this change. Tagged
compilation and CI do not claim those profiles ran. Backend security behavior
remains covered by the retained default tests and prior historical reports.
