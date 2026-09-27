# M22: first reference capacity calibration

This bounded stage adds only an internal load command and an explicitly opted-in
standalone OCI fixture. Product inputs remain those of M20 source
`315819fcd2c0cae1c22604e85ccdb5bb9291f585`. Execution evidence is pending;
calibration is not independent acceptance, a 24h soak, or production qualification.
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
may be loaded after read-only collision checks (retained as an artifact cache);
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

Native invocation results and the machine-readable capacity baseline will be
recorded here after execution. Failed phases remain in the evidence manifest.
Other sizes/specifications, mixed/Hot/Bulk/Scan/Native workloads, TLS performance,
other backends/native platforms, Kubernetes capacity and24h remain required.
