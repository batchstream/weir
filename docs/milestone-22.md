# M22: first reference capacity calibration

Historical M22 was rejected by independent review. Its plan/results below remain
unchanged; the maintained entry now uses [M22R](milestone-22-remediation.md) and
requires a matching native generator qualification receipt.

This bounded stage adds only an internal load command and an explicitly opted-in
standalone OCI fixture. Product inputs remain those of M20 source
`315819fcd2c0cae1c22604e85ccdb5bb9291f585`. The bounded measured attempt completed with no qualifying capacity candidate,
because the generator dropped late arrivals even at50ops/s. Independent review
is pending; this is not a 24h soak or production qualification.
M21R was independently accepted only for its ownership corrections and small
single-VM multiworker correctness profile. Its outer all-network-ID assertion
failed; the default bridge changed before the first fixture Docker mutation.
The environment cause remains unconfirmed, and historical failures remain intact.

## Frozen experiment and reproduction

The immutable contract is [capacity-plan.json](../scripts/capacity-plan.json).
Before any Docker mutation, the entry records source/tool/input hashes, verifies
all 70 product inputs and OCI layers/embedded binary, validates effective defaults,
records initial inventory and host/VM metadata, and writes a read-only expanded
`calibration-plan.json`. Its SHA256 is copied into the result. Inventory is recorded
again immediately before the first Docker mutation. No existing evidence root,
owner, exact image tag, container name or network is reused.

```sh
WEIR_CAPACITY_INTEGRATION=1 python3 scripts/test-capacity.py \
  --owner weir-m22-UNIQUE --evidence /absolute/new/evidence/root
```

The parent directory must exist. Use lowercase letters/digits/hyphens for UNIQUE.
The entry currently targets this native Darwin arm64 host's existing Go1.27.1
runtime/caches and native aarch64 Docker VM8CPU/8,319,770,624B/Linux7.0.12-linuxkit.
It requires the exact ES image config in the plan; the exact untagged product OCI
may be loaded after read-only collision checks (retained as an artifact cache).
Docker's legacy image store requires a manifest.json archive: the fixture preserves
exact config bytes and verifies every uncompressed layer diff_id during format
translation; it does not rebuild or retag the product.
it never pulls images, changes the daemon, or reads developer credentials.
No current Kubernetes context, existing access files, or paid resources are used.
All commands have time/output limits and start/end/exit records. A failure retains
its plan, command output, mutation ledger, and manifest. Cleanup tries every owned
resource independently, checking label/name/ID before removal. No global prune.

中文运行要点：只接受全新证据目录与唯一 owner；显式 opt-in 才运行 Docker。
先冻结计划和 hash，再进行原生测量；任一失败均保留，不换配置或降门槛重试。
只有预设的一次候选降档确认可运行。停止后清理自有容器/网络/tag并核对库存。
默认 bridge 差异单列；活动期间未知差异或非自有对象变化会使入口失败。
这里是共享物理宿主上的单VM校准，不能称物理核独占、Weir极限或24小时资格。

## Measurement contract

Every ten planned calls contain nine uniform corpus Reads and one unique-ID Put.
There are 64 fixed workers and no buffered work queue. Unavailable workers cause
explicit client drops; scheduling delays over5ms are late drops. Catch-up is
bounded by that5ms horizon, not an unlimited burst. The one-second deadline starts
at planned arrival. Completed failure/timeout latencies remain in histograms;
drops have no fabricated zero sample and remain in the planned denominator.
Reported planned, started, success, read and write rates are separate. Histogram
upper bounds are100us to10ms,1ms to1s,10ms to2s, then explicit overflow; count
merges, not averaged percentiles, determine all quantiles. Max retains nanoseconds.
Warmup, measurement and10s recovery cohorts are separate; all are retained.

The JSON document is exactly1024 raw bytes. Its `data` value is deterministic
SHA256 hex, about4bits/character and roughly2:1 compressible, not random entropy
claims or zero padding. Mapping is disabled: this measures complete source reads
and writes, not search queries. Before each trial, only the owned index is reset
and1000 corpus records are seeded. ES and Weir processes persist, followed by20s
warmup. Overload and immediate recovery share data and processes without a reset.

The client has4 gRPC connections; HTTP max62 plus2 sequential observer sockets
keeps the overall client/observer HTTP ceiling64. No proxy, redirect, retry,
hedging, or idempotency header is used. Non-rewindable HTTP bodies, including an
empty chunked GET body, disable Go transport transparent replay. Direct writes
are single-record bulk POSTs; Weir may microbatch. This request-shape difference
and protobuf versus JSON envelopes are part of the comparison.

Each write's outcome occupies one byte in a bounded ledger (<=100000/trial).
After the timed phase, realtime mget pages of100 audit every planned write ID,
including drops and UNKNOWN. Every found record must have version1 and the exact
payload; every APPLIED must exist and every definite non-application must be absent.
No audit triggers a resend. The base64 ledger plus deterministic IDs preserves
all outcomes without retaining payloads. Responses are fully read within256KiB
(16KiB for gRPC); excess/truncated/malformed responses never become success.

Two namespace observers have total0.25CPU/128MiB. They sample only their owned
Weir/ES PID, FD, socket and cgroup files every2s; the Weir observer also reads
loopback metrics and bounded ES stats. Client self-sampling uses the same interval.
These are sampled bounds, not atomic peaks. Exact Weir goroutine/Go heap census
is unavailable and remains unverified. Existing Guard RSS and cgroup signals,
connection owner and execution ledgers are retained separately. Idle evidence is
captured12s after all trials and final backend owner logs after graceful shutdown.

## Tests and evidence

Default offline Go test/race/vet must stay free of Docker/real backend starts.
The pure histogram, document, HTTP completeness/no-replay and open-loop boundary
tests also run by default. Integration vet compiles the opt-in command. Python
negative tests use synthetic subprocess metadata and temporary owned paths:

```sh
python3 -m unittest discover -s scripts -p test_capacity_test.py
```

Native results are frozen below. Failed preparations and measured phases remain
in their evidence manifests.
Other sizes/specifications, mixed/Hot/Bulk/Scan/Native workloads, TLS performance,
other backends/native platforms, Kubernetes capacity and24h remain required.


## Measured result: no capacity candidate

Executor chat `01a0e510-c8cd-7240-8064-d158d8a8f50b`; checkout baseline
`97a26da886140440fd4000a8aa5ee3624190e176`; initial implementation `b69d7cc`;
actual measured tool source `0a0951363f3a7b21529e7a648541094baaac163b`.
The exact [expanded plan](calibration-plan.json) SHA256 is
`907dde8753ab89807d006ec41023c00456518615157b19ecc9006611a40001a6`.
[capacity-baseline.json](capacity-baseline.json) preserves the full profile,
thresholds, effective settings, measured counts/quantiles/resources, source/tool
hashes and retained failure manifests. Its candidate is **null**, not zero RPS.
The three confirmations, direct comparison,2x overload and70% recovery were not
run because the minimum search point failed, as required by the original rule.
No resources, deadlines, arrival algorithm or gates were adjusted after load.

| Window | Planned | Completed / success | Client late drops | UNKNOWN |
| --- | ---: | ---: | ---: | ---: |
|20s warmup at50ops/s|1000|808 /808|192|0|
|60s measurement at50ops/s|3000|2569 /2569|431|0|

All623 drops were arrivals already more than5ms late, not a full worker queue.
Measurement failure/drop ratio is431/3000=14.3667%. Started and success rates are
42.8167/s; successful reads39.1833/s and writes3.6333/s. Completed-call aggregate
arrival p50/p95/p99=5.1/9.1/12ms (histogram upper bounds; see raw counts for exact
recalculation). These include all started outcomes, but **do not describe the
latency of the431 unsent arrivals**, whose latency was not fabricated. Therefore
the completed-call latency figures do not qualify50ops/s. Read/Put and dispatch/
scheduling quantiles remain separately machine-readable.

Client CPU used2.3637% of its1CPU quota; ES used5.8984% of its3CPU quota in the
measurement samples. Both had zero quota throttled time. No sampled DB queue/
rejection, product protocol/transport error, UNKNOWN or incorrect payload was
observed. The evidence identifies generator scheduling lateness; its root cause
(timer/guest/physical-host scheduling or another cause) remains unverified.
**This is not evidence that Weir's or ES's capacity is below50ops/s.** No alternative
arrival algorithm, changed timeout or tuned VM rerun was used to obtain a pass.

All400 planned unique write IDs (warmup plus measurement) were audited in4 bounded
realtime mget pages:296 APPLIED were present with version1 and exact1024-byte
payload;104 dropped IDs were absent. No retries, false APPLIED, false
non-application, correlation failure or replay was observed. Bootstrap seeded
1000 records, and the search trial reset the index and seeded another1000: the
raw runner's `mutations_with_seed=1400` omits that initial bootstrap count; the
compact receipt explicitly accounts for2400 planned mutations including both
seed phases. Actual timed writes were296. All are far below the fixed budget.

Across49 Weir samples, RSS max25,714,688B, cgroup current max13,471,744B, FD max14,
backend owner peak1/limit5, ingress connections max4. ES server JVM (not tini or
launcher) across43 samples: RSS max1,468,440,576B, cgroup current max1,519,935,488B,
FD max573. Both cgroup memory.events max/oom/oom_kill were0. RSS and cgroup are
separate accounting views; file-page charges need not belong to this cgroup, so
the two values need not be ordered. They are not added together.
Sampled bounds do not prove atomic peaks, long-term stability or24h freedom from
leaks. Exact Weir goroutine and Go heap census remain unverified, represented by
null rather than measured zero.

After12s idle, Weir FD returned to10 (startup10); ingress connections/sessions,
pending/result entries, active executions and live sessions were0. One backend
idle pooled owner remained, within limit5; graceful shutdown logged owned0,
peak1, acquired1=released1. All five containers had restartCount0 and were
removed; own network and helper tag were removed; no volumes were created.
The exact untagged product image loaded from the verified OCI remains as an
artifact cache. Generated helper binaries/tars were reclaimed with hashes; their
removal records explain absent intermediates in the original failure manifests.
No failed command output, plan or measurement was removed.

The measured invocation lasted117.480s including preparation and cleanup, with
80s planned load,4000 planned calls and400 planned timed mutations. It exited0
because the bounded search and cleanup completed; its capacity gate is **FAIL**.
All command exits, including expected unsuccessful readiness probes, are in
`commands.jsonl`; exit0 does not mean every subcommand succeeded.

## Retained preparation failures and environment

Five earlier invocations never entered timed load. Each stopped and confirmed
cleanup, and each remains under `.testdata/m22/native-{first..fifth}` with its
command log, exit1, plan where generated, original manifest and cleanup results:

1.3.117s: exact product image was not already loaded (read-only preparation).
2.6.606s: legacy Docker image store rejected an OCI archive lacking manifest.json.
3.5.862s: local log-driver compression cannot be enabled with max-file1.
4.23.726s: read-only observer could read RSS/cgroup but lacked FD directory access.
5.25.028s: ES has launcher and server JVMs; selecting unique `java` comm was ambiguous.

The maintained entry now imports an untagged legacy-format archive preserving
config and diff_ids, disables compression for the single bounded log file, and
selects the ES bootstrap main class inside its own PID namespace. Only owned
observers receive SYS_PTRACE/DAC_READ_SEARCH. These are preparation/observation
corrections, not product changes or discarded capacity curves.

Initial/first-mutation/final inventories for the measured invocation match for
all non-owned containers, volumes and networks including the default bridge.
The old four exited M2 containers and redis_default remain. This local observation
does not resolve the historical M21R bridge cause or reverse its failed outer
auditor result. No daemon settings, host configuration, user access file or
credentials were read or modified.

Only this failed minimum-rate generator profile has a timed measurement. Further
capacity search requires an independently reviewed generator/environment remedy
and a separately authorized invocation; this executor is stopped and does not
schedule a next phase. All broader production gates listed above remain required.


## Final offline validation and handoff

Fixed Go1.27.1, GOENV=off/GOTOOLCHAIN=local/GOWORK=off/GOPROXY=off/GOSUMDB=off,
explicit repository GOROOT. Final CGO0 default test62.065s, CGO1 full race65.311s,
vet0.303s, integration vet0.358s and Linux arm64 integration vet0.360s all exit0.
Linux vet is static evidence only. The9 Python safety/count/archive tests pass;
final responsibility-based Go test-file organization was followed by focused
capacity race (3.078s, exit0). No native fixture or test child remains running.
Earlier development had an ambient GOROOT/compiler version mismatch and a stale
cleanup-test expected list after adding intermediate reclamation; both failed
locally before the final checks. Neither was a product regression or a native
capacity measurement, and neither is described as an all-attempts pass.

The evidence index at `.testdata/m22/evidence-index.json` hashes every retained
file; the compact baseline pins its hash. Retained evidence is about13MiB, below
256MiB. The first-five original manifests also list reclaimed generated helper
binaries/tars; their explicit `intermediate-reclamation.json` records preserve
size/hash and explain those deliberate absences. The sixth final manifest lists
only retained evidence. All five preparation failures, failed50ops/s measurement,
raw histogram counts, bounded outcome ledger and actual command exits remain.

Local commits only; no push, PR, tag, release, host/daemon change or paid resource.
The executor stops after handing these results to the coordinator. A generator
or environment remedy and new capacity invocation require a separately planned
stage; no follow-on chat, automation or silent retest is created here.
