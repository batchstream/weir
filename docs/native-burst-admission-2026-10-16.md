# Native admission during short mixed bursts

Status: the causal correction is frozen and passes affected race/vet checks.
Clean-artifact backend acceptance and independent PR review remain pending.
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
persisted-data oracles passed. Both backends exited cleanly without OOM or
owned-container residuals; all logical credits returned within five seconds.

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

## Validation pending

Affected Store, execution, Mongo, Search, server, app and CLI race suites pass;
focused causal/input/queue/count regressions pass twenty times and vet passes.
The permanent real CLI test keeps the ten-by-256 burst, three-second per-call
and five-second cleanup bounds. It checks every result and independent backend
ID/value/padding/cardinality oracles. Clean source/binary receipts, new results,
independent final-head approval and CI will gate merging.

The separate typed large-document probe also found a real 512 MiB OOM outside
the Native ledger. That is a subsequent milestone, not repaired or approved by
this Native-only correction. Complete-process production qualification remains
blocked until its confirmed boundaries are resolved and the original sustained
and fault profiles pass.
