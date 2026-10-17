# Final complete-process qualification, 2026-10-17

The default service passed the complete 600-second mixed workload and the final
owned-proxy fault qualification against real MongoDB 8.0.32 and Elasticsearch
8.19.22. The earlier formal failure remains preserved. These results cover the
controlled local profiles below; the user selected local validation and code
repair, so running production clusters were not inspected or changed.

The [machine-readable summary](production-process-results-2026-10-17/summary.json)
and [evidence inventory](production-process-results-2026-10-17/evidence-index.json)
retain exact commands, clean source and binary identities, raw results, failures,
source projections and independent cleanup observations.

## Sustained load

The actual Linux arm64 CLI ran in native cgroup v2 with 1 GiB memory and two
CPUs. The host driver used Go 1.27.1 race detection and the real SDK Resolve
path. Store concurrency, batching, queues, output stall and admission settings
kept their defaults. The pinned Elasticsearch service used its verified private
Linux bridge; independent data oracles used host loopback.

Clean source: `69349eac568af9993c837fcd05c08337f5743a30`.
CLI SHA256: `5310682e9ca6b072ff602b20708965f848ec8b6d739c61033f260324bc2ca395`.

The driver completed 6,000 warmup calls, 120,000 measurement calls at fixed
200 RPC/s for the full 600 seconds, and all 256 additional burst calls. It did
not suppress offers or retry a failed formal run. The 512-call driver bound
was not exhausted. All ordinary operations succeeded; the execution memory
budget rejection counter stayed zero. Every warmup, steady and burst call was
also checked against the original three-second end-to-end bound.

| Method | MongoDB steady p99 | Elasticsearch steady p99 | Predeclared limit |
| --- | --- | --- | --- |
| Read | 11.114 ms | 27.668 ms | 100 ms |
| Put | 35.263 ms | 40.035 ms | 250 ms |
| Native | 9.848 ms | 21.702 ms | 250 ms |
| Lua | 37.345 ms | 33.230 ms | 500 ms |
| Four-record batch | 45.756 ms | 64.235 ms | 500 ms |

The largest steady end-to-end observation was 588.935 ms. The 256-request burst
at measurement second 300 completed with a maximum of 82.114 ms, including
driver scheduling lag. Its six backend/method distributions are retained
separately; steady p99 limits are not applied to a different burst distribution.
The first historical formal run failed at this same burst with a short Native
resource rejection; it is not counted as a successful 600-second run.

At second 150, 32 Native consumers began receiving 8 MiB results in periodic
chunks for 120 seconds. Every stream demonstrated at least 237 callbacks and
11,316,243 bytes; the largest observed gap was 1.791397 seconds, below the
two-second continuous-progress gate. Thirty-two responses completed and their
32 successors were intentionally canceled at phase end. This crossed the
unchanged default 30-second output-stall interval while normal traffic continued.

Independent backend scans verified all 25,374 MongoDB and 25,375 Elasticsearch
documents, including every acknowledged Put, batch and Lua effect, and rejected
unacknowledged or missing records. Closing the SDK returned all temporary Store,
RPC, wire, ingress and execution ownership to zero inside five seconds. The CLI
exited zero with OOM=false; both owned backend connection counts closed at zero.
Independent checks confirmed the exact test database, index and CLI container
were absent afterward.

There were 630 one-second samples including warmup. Sampled maxima were
356,216,832 bytes RSS and 477,650,944 bytes cgroup working set, at potentially
different instants. These are sampled observations, not a proof of the physical
transient peak. Admission never latched. Process CPU increased by 80.15 seconds
over the roughly 630-second offered interval. The raw per-minute distributions,
driver lag and GC observations remain available; this is a fixed-workload
qualification, not a maximum-capacity or long-duration soak claim.

The initial outer summary script expected `oracle backend=<name>` while the
test logs `independent oracle <name>`. It therefore reported two missing labels
after the actual test had passed and independently cleaned up. Both that parser
failure and the corrected exact-label validation are retained. No test was
rerun, no raw result overwritten, and no acceptance gate weakened.

## Fault and recovery

The final Linux CLI used 512 MiB, two CPUs and the same default service policies.
Clean source: `032fd8b8e2cfa0acf0e65c166d6265c3c30ad87b`.
CLI SHA256: `ea8d8617052a53db7fa676df9b614fd8cfcc973004f1f0451e79539352a15687`.

Only the opt-in fault driver and test-only Mongo observer changed after the
sustained run. The [source projection](production-process-results-2026-10-17/final-fault-r2/sustained-to-fault-source-projection.json)
checks every protected source hash and enumerates those two files. The shipping
runtime, dependency graph and packaging code did not change. Consequently the
600-second result remains applicable without repeating it for a fixture change.

Before running final faults, independent preflight reviewers strengthened these
assertions:

- Check the recovery deadline before accepting the last zero-resource sample.
- Check successful healthy calls against measured three-second elapsed time.
- Observe cancellation credit recovery before manually releasing the proxy gate.
- Keep proxy gates and network deadlines finite at 20 seconds, beyond the
  original three/five/eight-second business gates, so automatic fixture EOF
  cannot help the service pass cancellation recovery.

| Check | MongoDB | Elasticsearch |
| --- | --- | --- |
| Healthy Read while another IO is held | 10.737 ms | 9.630 ms |
| Healthy Native while another IO is held | 2.597 ms | 5.836 ms |
| Held IO cancellation and credit recovery before proxy release | 19.403 ms | 20.681 ms |
| Same SDK CRUD/Native after proxy restoration | Within 5 s; full observation 797.783 ms | Within 5 s; full observation 117.411 ms |
| Native lost acknowledgement | INCOMPLETE; exactly one write/ACK/drop and oracle increment | INCOMPLETE; exactly one write/ACK/drop and oracle increment/version |
| Typed lost acknowledgement | UNKNOWN; exactly one write/ACK/drop and oracle increment | UNKNOWN; exactly one write/ACK/drop and oracle increment/version |

Canceling after a backend acknowledgement also preserved exactly one mutation
and returned all credits before proxy release. No ambiguous mutation was
replayed. Between faults, the same SDK retained its two legitimate Store
channels and their 1 MiB ingress connection floor; other execution/ingress/wire
owners were zero. Recovery retained the same service PID. A mutation with a
held acknowledgement overlapped SIGTERM; shutdown finished in 188.447 ms,
exit zero, OOM=false, with exactly one backend increment and zero observer
sockets. These are local observations under the fixed hard gates.

The suite completed with race detection, followed by independent exact database,
index and container absence checks. It used owned per-request proxies, no
database-global failpoints, forced service GC, production endpoints or secrets.

## Prior repairs and delivery evidence

PRs [75](https://github.com/batchstream/weir/pull/75),
[76](https://github.com/batchstream/weir/pull/76),
[77](https://github.com/batchstream/weir/pull/77),
[78](https://github.com/batchstream/weir/pull/78),
[79](https://github.com/batchstream/weir/pull/79),
[80](https://github.com/batchstream/weir/pull/80),
[81](https://github.com/batchstream/weir/pull/81),
[82](https://github.com/batchstream/weir/pull/82) and
[83](https://github.com/batchstream/weir/pull/83) were independently reviewed,
passed their own final-head CI and merged before this qualification. The
[readiness milestones](production-readiness.md) link their separate causal
regressions, recovery profiles and memory bounds.

PR83 additionally qualified the exact shipping Linux arm64 binary against the
original Lua OOM probe and sixteen allocation paths, and built two clean exports
for four targets with identical binary and archive pairs. This final branch
changes only opt-in tests, test fixtures and evidence/documentation; its shipping
runtime is unchanged. The source-projection receipts and prior final PR83 review
and CI receipts are retained in this inventory.

Default tests stay offline; these real-backend complete-process suites require
explicit integration and qualification opt-ins. To reproduce, build a clean
Linux arm64 CLI with Go 1.27.1, retain its build identity/SHA256, and compile the
integration app test driver with race detection. Set `WEIR_INTEGRATION=1`,
`WEIR_SEARCH_INTEGRATION=elasticsearch`, `WEIR_QUALIFICATION_BINARY`,
`WEIR_QUALIFICATION_SOURCE` and `WEIR_QUALIFICATION_SHA256`; run the two named
tests separately with `WEIR_PRODUCTION_QUALIFICATION=1` or
`WEIR_PRODUCTION_FAULT_QUALIFICATION=1`. Leave both
`WEIR_PRODUCTION_QUALIFICATION_SMOKE` and `WEIR_FAULT_EXPLORATORY` unset. Exact
commands, sanitized environments, fixture identities and exit codes are in the
inventory. Failures remain evidence and are investigated before a new run.
