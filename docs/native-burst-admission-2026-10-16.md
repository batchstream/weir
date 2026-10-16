# Native admission during short mixed bursts

Status: the causal correction passes clean-artifact real-backend acceptance.
Independent final-head PR review and CI remain merge gates.
The overall production qualification has not passed.

## Reproduced failure

The first formal Linux run used clean source
`fde3b1775b0a36dbb4f4044be1299f731db43314`, binary SHA256
`795601c60e8f4398a8108843939361032f88249acc8f75ee42ae322ca7269c79`.
At the 300-second, 256-request mixed burst, a 1 KiB Mongo Native query returned
NOT_STARTED/RESOURCE_EXHAUSTED after about 60 ms. The preceding 60,006 steady
calls, 6,000 warmup calls and continuous 120-second slow-consumer phase passed.
This is a failed qualification: it did not reach 600 seconds, final oracles or
the final recovery gate. Its [raw log](native-burst-results-2026-10-16/production-qualification-formal600-first.txt)
and [manifest](native-burst-results-2026-10-16/production-qualification-formal600-first-manifest.json)
are preserved. The last one-second sample was before the burst and cannot
establish the exact denial-time RSS or ledger state.

A separate bounded baseline repeated ten bursts, collected all 2,560 outcomes,
and reproduced one short Search Native denial. All other calls and independent
persisted-data oracles passed. Both diagnostic CLI runs exited cleanly without
OOM or owned-container residuals; all logical credits returned within five seconds.

A diagnostic-only binary added mutex-held numerical logs before each existing
denial branch, without changing policy, timeouts or retries. It reproduced six
denials, all in input admission. Total reserved Used was 491.8–495.5 MB, leaving
28,863–112,590 bytes; the new input/window asked for about 135,900 bytes.
Resident credit was only 5.0–6.1 MB, below its 443.7–447.5 MB limit, while twelve
to fourteen invocations still held initial working estimates. This establishes
transient IO reservation contention, rather than a full retained-output pool.
This binary is explicitly diagnostic, not clean qualification, regardless of
its displayed version. See the [causal manifest](native-burst-results-2026-10-16/native-burst-diagnostic-manifest.json)
for commands, source/overlay/binary/raw hashes and sampling limits.

The deterministic [before test](native-burst-results-2026-10-16/native-burst-admission-unit-before-test.go.txt)
also isolated two initial IO reservations, only 265,216 resident bytes and ample
resident space. A third small request became a hard denial while those IOs
could converge. Raw before failures remain alongside the later passing checks.

## Corrected ownership

The total account remains B = floor(2 * trusted recovery allowance / 3), with
1024 leases. A finite Node input promise Q is charged once to Used and Resident
while leases exist. Its automatic target is
min(default queue bytes, (B - min(B, IO reserve)) / 2), at most 32 MiB.
Actual raw input plus a fixed 1024-byte terminal credit draws from that promise;
I is never larger than Q. Initial IO cannot spend this promised input capacity.
Queued requests no longer reserve a future double-chunk publication window.
That window is charged immediately before dispatch, alongside initial work.

A legal input exceeding the automatic promise may proceed alone when its full
initial peak fits the same total account. Trusted shrink revokes unused promise
only, preserving actual inputs and work. Unknown observations keep the previous
trusted bound. Known zero leaves no room for new inputs. The last Ack releases
Q and every byte/state count returns to zero.

Native input-queue saturation now fails explicitly with NOT_STARTED and no
external Submit waiter. Temporary IO waits belong only to admitted, accounted,
bounded Runtime queue entries. Other Stores wake them through the existing
shared-memory signal. A body-read completion also signals when byte totals are
unchanged but transient IO has ended. Typed behavior is unchanged in this PR.

Two additional fixed Node gauges expose input_reserved_bytes and input_used_bytes.
Used/Resident include the unused input promise, so they describe reservations,
not physical RSS. Maximum static Weir series becomes 1088. Actual RSS/cgroup
admission remains independent. True byte/count/input-queue exhaustion still
returns an explicit capacity failure. Opaque operations are never replayed.

## Focused clean-artifact acceptance

Affected Store, execution, Mongo, Search, server, app and CLI race suites pass;
focused causal/input/queue/count regressions pass twenty times and vet passes.
The permanent real CLI test keeps the ten-by-256 burst, three-second per-call
and five-second cleanup bounds. It checks every result and independent backend
ID/value/padding/cardinality oracles. The implementation agent also ran the full
affected race package list successfully; that output exists in the original tool
session rather than a committed raw file. CI will independently run the full
repository default, race and vet checks at the final PR head.

The first standalone attempt used clean `7b091080f6d11f137e776d30bdf01360b9075220`
and failed all 420 Search Native calls: the new fixture supplied the index in
both Resource and the absolute HTTP path. This duplicated the index prefix.
That [failed raw run](native-burst-results-2026-10-16/native-burst-fixed-burst.txt)
and [manifest](native-burst-results-2026-10-16/native-burst-fixed-validation-first.json)
remain failed; dependent checks stopped. Only test construction and its error
message changed before the following run. Production code did not change.

The corrected fixture and production were frozen at clean source
`94523988c6fdf0bfc15e117d2bcc23ed5e10785a`. Go 1.27.1 produced a clean
Linux/arm64 CLI, SHA256
`d40e8a0aaaee8a2720ac54c154266b71fc67ae6dc8609dd62446fa93700b49bf`.
The [build receipt](native-burst-results-2026-10-16/native-burst-fixed-r2-build.json)
and [validation manifest](native-burst-results-2026-10-16/native-burst-fixed-r2-validation.json)
record exact source-file hashes, commands, runtime identities, raw-file hashes,
actual exit codes, results and cleanup. These checks ran sequentially with an
exclusive backend/load window, using the original deadlines and no retries.

| Check | Result |
| --- | --- |
| Clean CLI ten mixed 256-call bursts | 2560/2560 success; 20 persisted-data oracles; round p99 38.21–58.69 ms, maximum 58.94 ms |
| Node account | 15 fixed metrics; Q >= I, Used <= B; zero budget denials or Guard latch; all logical ownership zero within 5 seconds |
| Clean CLI 32 slow 8 MiB responses, five repetitions | Healthy Read/short Native 2.39–3.61 ms; all ownership zero 8.42–12.97 ms after cancellation |
| Maximum diagnostics | Exactly 1088 Weir series, 16 Stores, zero synthetic executions |
| Mongo valid/malformed and Search small-output progress, race x3 | All pass from the same clean source; these are in-process server tests rather than Linux CLI runs |
| Cleanup | All six CLI runs exit 0 without OOM; backend owned connections and owned CLI container residuals zero |

The focused burst is an admission regression, not sustained capacity or
throughput certification. The prior twelve typed memory-policy and four shared
pressure runs were not repeated here; their earlier source identities and scope
remain unchanged. The original formal 600-second failure remains a failure.
Independent final-head approval and green CI will gate merging.

The separate typed large-document probe also found a real 512 MiB OOM outside
the Native ledger. That is a subsequent milestone, not repaired or approved by
this Native-only correction. Complete-process production qualification remains
blocked until its confirmed boundaries are resolved and the original sustained
and fault profiles pass.
