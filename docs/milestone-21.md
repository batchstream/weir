# M21: bounded three-replica / two-worker lifecycle smoke

2026-09-28. Baseline `e4b8a432e2596b2fe681cd2d8c6629b8a631c734` on local main.
This report records executor evidence for coordinator acceptance. **It is not
reference capacity, a physical-host failure test, a version-compatibility upgrade,
a 24-hour soak, or whole-product production qualification.**

## Frozen scope and resources

One new owned kind v0.33.0 cluster, Kubernetes v1.36.4, native Linux arm64,
one control-plane and exactly two workers inside the same Docker VM. The host is
Darwin arm64; the Linux VM has 8 CPUs, 8,319,770,624 bytes and kernel
7.0.12-linuxkit. Four pre-existing exited M2 containers and `redis_default` network
are outside this fixture. No current kubeconfig/context, existing Secret or user
Docker configuration is used. Official tools consume the generated kubeconfig;
its content is never read or logged.

The candidate 512 MiB Weir size could not leave adequate per-worker headroom under
the conservative old/new overlap. **Before any cluster started**, this correctness
smoke froze 384 MiB/Pod with a 256 MiB process budget. The canonical reference
Deployment remains 2CPU/1GiB with 768 MiB process budget. No workload or resource
threshold is lowered after a failure.

| Owner | CPU hard limit | Memory hard limit | Reserved application memory / headroom |
| --- | ---: | ---: | --- |
| control-plane | 2 | 1280 MiB | No Weir, ES or client Pods |
| worker (healthy) | 3.5 | 3840 MiB | At most four old/new Weir 1536 + ES1536 + client256; 512 MiB system allowance |
| worker2 (fault target) | 2.5 | 2048 MiB | At most four old/new Weir 1536; 512 MiB system allowance |
| each Weir | 0.5 | 384 MiB | request100m/384Mi, process budget256Mi, one Local C2 or C1 |
| ES8.19.22 | 1 | 1536 MiB | request500m/1536Mi, fixed384Mi heap, bounded512Mi data |
| client Pod | 0.5 | 256 MiB | request100m/256Mi, at most five client processes / twelve gRPC connections |

Node hard limits sum to 7 GiB, leaving 803,577,856 bytes outside these nodes.
The 512 MiB worker allowance is not an OS reservation. CPU limits can contend;
no latency/throughput result is inferred from this setup. Bootstrap and each node's
actual cgroup memory/events are recorded separately, including reclaim pressure.

Workload maximum: 400 unique mutations, documents at most64 bytes, six new Service
connections after expansion, a 40-operation old Bulk stream, two 60-operation
rolling loads, three bounded active clients per revision, one short worker fault.
Unary RPCs are bounded to4s, HTTP observations3s, streams60s and active lifecycle
clients20s. The product retains default input stall30s, overall close5s and Pod
grace15s. Bootstrap≤20min; the complete functional fixture≤15min; a worker pause
≤90s, normal node recovery≤120s. Failure restoration and overall cleanup are
independent finally paths. No controller monitoring or eviction timer is shortened.

Only the test Deployment sets standard `GODEBUG=schedtrace=10000,scheddetail=1`
for a bounded runtime goroutine census. No production instrumentation is added.
These debug logs preclude treating this run as a capacity/latency benchmark.
The client and ES remain pinned to the healthy worker during the fault.

## Maintained implementation and source identity

`deploy/kubernetes/three-replicas.patch.json` extends the one canonical Deployment:
RollingUpdate, maxSurge0/maxUnavailable1, worker-only node affinity, hostname
maxSkew1/minDomains2. No chart, controller, service discovery or application replay
is introduced. The single-replica example remains available.

`internal/testutil/testkube` contains only an opt-in RPC/HTTP client, separated into
client entry, record/Bulk validation, backend observations and active lifecycle
calls. `scripts/test-kubernetes.py` orchestrates this one fixed experiment;
`kubernetes_fixture.py` owns explicit-context commands, node ownership and cleanup.
Offline Python tests inject cleanup failures without starting any real resource.

Product artifact source remains `315819fcd2c0cae1c22604e85ccdb5bb9291f585`:
all70 product/packaging inputs are hashed against its receipt. No product or
packaging input changed. This reuses the exact M20 image, not a test binary or a
reconstructed image layer:

- OCI index `sha256:ab777e7e00cc83f2f322f4324bca566fdfe9b7970b1dae458d39844cbe7142b7`.
- arm64 manifest `sha256:f2799aae8e5468ece29fa820cdc6546f6b409c86b48de05376277391c2365270`.
- runtime image ID `sha256:caa699e6ca172cbfa24ed4d311f4cb346817a354b05df52601abd954edcb4dab`.
- executable SHA256 `9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32`.

Every observed Pod UID/node/imageID/config reference, CRI PID and executable hash
is retained. The separate client binary/image and its exact source inputs have
independent receipts. No new six-target build or Java scan is claimed for unchanged
product inputs. Fixed node and ES digests, native tool hashes and resource values
are in `scripts/kubernetes-smoke.json` and the executed freeze receipt.

## Accounting and semantics

Three steady C2 Locals have ΣC=6, Σ(C+1)=9 local DB ownership slots and at most18
combined DB/DNS sockets. C1 gives3/6/12. A single full C2→C1 or C1→C2 revision
conservatively budgets three old plus three new:9/15/30. The starting, Ready,
draining and terminating PIDs all count. Standard RollingUpdate does not enforce
a hard maximum of three live executors. Before the next revision all old process
IDs must be gone, not merely absent from Ready endpoints.

The original M12R distinction remains: pool entries, local dial/raw/closing owner
credits, DB accepted connections and already-sent remote work are different
measurements. Search includes the separate Native socket in C+1; Native still
shares the Store execution window. Direct fixture administration/probes (at most
five concurrent HTTP observations) are separately counted. Owner `peak` is an
exact event high-water mark; sampled sums across Pods are not atomic global peaks.
DB remote tails do not acquire an invented empirical constant or unconditional
C-derived ceiling. Refresh listeners measure the particular held-write tail.

All writes use unique record/operation IDs. Client APPLIED, explicit non-execution
and UNKNOWN are recorded independently from ES `_version` and existence. A final
refresh plus bounded complete search verifies every surviving fixture record is
version1. UNKNOWN writes are never resent. This isolated readback is not a general
production deduplication or recovery protocol.

The old Bulk stream retains its original connection through scale-up, validates
continuous indexes0–39, one End with counts40/40 and final EOF. Independent new
connections can reach different Pods; no equal-distribution promise is made.
Rollout and rollback change validated immutable Secret references C2→C1→C2,
using the same exact image. This demonstrates static-config lifecycle only.

A Native `refresh=wait_for` write is observed persisted while its response remains
incomplete. Independent partial Read uploads exercise default30s input stall
against5s process shutdown. Incomplete Native responses mean UNKNOWN effects,
not NOT_STARTED. Node loss is a paused owned worker container, with normal Node
and EndpointSlice detection; it is not deletion, eviction or a permanently lost
physical machine. Backend replication/failover is outside this single ES fixture.

## Executed evidence and measured limits

Final successful, independently auditable fixture:
`.testdata/m21-20260928-r3/`, owner `weir-m21-20260928-r3`.
Implementation commit `7d3db74`; log-retention correction `0e79e24`; the executed
client/fixture source is `5e5406d1b8489d379996d50601cd7216f852abc0`.
The credential-free [receipt](milestone-21-receipt.json) separates these from the
unchanged product source. Final documentation/observation-only changes are later
local commits; they are not a new product artifact.

The final test-client binary SHA256 is
`5c4afb60a827fa2b30b39489ea2969127c731f1f600bcab1d7675b9920fda114`;
client image ID is
`sha256:7aee4dc69de67d072209218647c40c3af0c16e109cf5aed4b7a6f2892ae4b29d`.
It is not the product `/weir` binary.

| Check | Final observed result |
| --- | --- |
| Time bounds | bootstrap28.753s; complete functional exercise185.770s, then exact cleanup |
| Placement | initial Pod on worker2; three Ready replicas distributed worker2=2 / worker=1; all revisions retained two-worker placement, no Weir on control-plane |
| Long connection | old stream remained on UID2562a64e…/worker2, one connection and one stream; all40 unique writes APPLIED, indexes0–39, End40/40 and EOF |
| New connections | six independent Service connections completed Read/Mutate/Bulk; final per-Pod record counters49/6/6, including old40 and initial3 on the first Pod; all three Pods received work, without a balancing guarantee |
| Config rollout / rollback | immutable Secret refs weir-c2→weir-c1→weir-c2; full revisions13.293s /13.690s; prior old container IDs absent from Running CRI before next revision |
| Default-stall termination | first terminating partial upload in each revision ended5.151s /5.170s after client setup; other concurrently opened uploads ended8.24–11.26s as their Pods later entered drain; no30s stall wait or15s SIGKILL path |
| Pod exit evidence | all nine executor identities have final owner receipts and are absent from final Running CRI; one final-run CRI exit snapshot retained exit0/Completed, others were garbage-collected before a redundant exit-code read. No inference of an unrecorded exit code |
| Worker fault | exact owner/ID-checked worker2 paused50.735s; Node Ready=Unknown/NodeStatusUnknown first observed49.904s after pause; faulty endpoints ready=false, healthy worker Service call succeeded before unpause |
| Recovery | Ready and successful Service/direct calls with original post-rollback Pod UIDs; recovery sample61.473s after pause (about10.74s after unpause); no eviction, forced Pod deletion or parameter changes |
| Client outcomes |193 unique mutations:184 APPLIED,9 UNKNOWN; no NOT_STARTED/NOT_APPLIED received in this particular run. Seven persisted Native writes lost complete responses; two timed-out unary calls remained absent in final isolated readback |
| Persistence / replay |191 records, every `_version=1`; includes all184 acknowledged mutations and7 UNKNOWN Native effects. Unknown operations never resent |
| Live executors | sampled Running CRI high-water5, despite maxSurge0; conservative bound6 retained. At most four observed on either worker; each old/new Local accounted by its own immutable concurrency revision |
| Local ledgers | sampled aggregate active executions3, owned connections7, ingress connections4/sessions7; these are sampled sums, not atomic global peaks. All per-owner event peaks≤C+1 |
| Final recovery | last three Pods:18 live goroutines each in standard scheduler census (observed run peak34), pending/retained/session/active/ingress ledgers0, ordinary backend idle connection1 each; after scale0, all Weir Pods/processes/Local owners0 |
| OOM / restarts | node cgroup oom/oom_kill/oom_group_kill all0, every Weir restartCount0; client and ES restartCount0 |

Each full UID, node, PID, config reference, peak and acquired/released count is in
the receipt. Original C2 PIDs were816/1101 on worker2 and1343 on worker; C1
PIDs2212/2738 on worker2 and2617 on worker; final C2 PIDs4674/5175 on worker2
and4113 on worker. All70 product inputs and each running executable hash matched
the accurate artifact. Native evidence is Linux arm64, not cross-architecture
emulation. Debug `G` entries with idle/dead status0/6 are not live goroutines.
A paused node has no fresh local scrape; its frozen processes are retained in
accounting, never reported as observed zero.

### Database observations are separate

ES statistics now select only current/total HTTP connection counters, the fixed
write thread-pool fields and refresh listeners; every reply fits under350 bytes
and is complete JSON. Results:

| Phase | DB accepted HTTP current | Refresh listeners | Write active / queue / rejected |
| --- | ---: | ---: | --- |
| three steady C2 |4 |0 |0 /0 /0 |
| C2→C1 overlap |8 |3 |0 /0 /0 |
| C1→C2 overlap |8 |3 |0 /0 /0 |
| paused worker,32.802s after pause |4 |1 |0 /0 /0 |
| recovered |4 |0 |0 /0 /0 |
| Weir scaled0 and all executors exited |1 |0 |0 /0 /0 |

The final one connection is the direct observer itself, which disables keepalive.
Readiness/admin clients are not Local pool owners. The supplemental paused-worker
observation used the same client/ES, explicit fresh kubeconfig/context and an8s
command bound; it is recorded separately with its timestamp. The Native client
had already returned UNKNOWN at20s, yet a DB refresh-response listener remained
at32.802s. The write had already persisted; this is a retained visibility/reply
resource, not evidence of ongoing write-thread execution or replay. Recovery and
explicit fixture refresh released it. The maintained entry now captures this
same counter observation during the paused phase.

All nine `backend_connections_closed` lines report owned0, peak within limit and
acquired=released. Original and C1 owners each acquired/released3; final C2 owners
4/2/2. No sampled pool figure is substituted for these exact owner events. These
observations do not impose a hard remote connection/work bound under arbitrary
partitions or backend failure.

The control-plane is tight: final memory.current1,326,931,968/1,342,177,280 bytes
and memory.events.max574, with OOM counters0. The initial run's memory.stat showed
substantial reclaimable file cache, separately from anonymous memory. This is
recorded pressure, not spare capacity or evidence that this control-plane size is
suitable for production. Final healthy/faulted worker current was3,042,148,352 /
407,642,112 bytes. These are aggregate cgroup samples, not Weir RSS or peak memory.

## Regression, preserved failures and reproducibility

Pinned Go1.27.1, GOENV=off/GOTOOLCHAIN=local/GOWORK=off/GOPROXY=off/GOSUMDB=off.
No default test starts Docker, Kubernetes or a real backend.

| Check | Result |
| --- | --- |
| default CGO0 full test / CGO1 full race | PASS61.695s /65.482s |
| default vet / integration vet | PASS0.300s /0.343s; final client re-vetted after fixture-only changes |
| CLI/app/server close, drain, admission, diagnostics and transport three-round race |69 top-level PASS,100.072s |
| full Guard three-round race |42 top-level PASS,3.199s; independently selected because the earlier broad name filter matched no Guard tests |
| Search owner/connection/DNS/close three-round race |21 top-level PASS,17.604s |
| cleanup failure tests |4 offline tests: independent cleanup after injected failures, owner mismatch rejection, pre-existing owner rejection and partial-bootstrap resource discovery |
| bounded HTTP evidence reader |exact64KiB accepted;64KiB+1 rejected, three race rounds PASS |
| packaging Python tests |PASS; no new packaging build was necessary |

No failure is erased:

1. `.testdata/m21-20260928` completed functional scenarios and187 version1 records,
   but final receipt assertion failed for one fast-exiting Pod: kubelet removed
   its log before the next file poll. All owned resources were cleaned. The fix
   opens the official Pod log stream before termination, with at most nine streams
   bounded by fixture lifetime/finally cleanup. No workload/resource change.
2. `.testdata/m21-20260928-r2` reported functional PASS and retained every owner
   receipt, but offline review rejected its late DB statistics: requesting the
   whole HTTP client history exceeded64KiB and the old test helper silently
   truncated it. This is not complete DB retirement evidence. The fix requests
   only the required counters and rejects any response exceeding the explicit
   bound; regression tests preserve the truncation counterexample.
3. `.testdata/m21-20260928-r3` reran the same resource and operation profile with
   both observation fixes. Complete DB JSON, final one observer connection,
   zero refresh listeners, all owner receipts and independent version audit pass.
   No product correctness defect or resource/traffic reduction was used to obtain
   that result. Prior UNKNOWN distributions differ naturally across finite Service
   connection selections and remain recorded.

Reproduce from the fixed product artifact and verified native tools described in
M20; use a new lowercase owner and evidence directory:

```sh
WEIR_KUBE_INTEGRATION=1 python3 scripts/test-kubernetes.py \
  --owner weir-m21-unique \
  --evidence .testdata/m21-unique \
  --artifact dist/m20/first
python3 -m unittest discover -s scripts -p test_kubernetes_test.py
```

The exact executed commands, all three freezes, Pod/config/image/CRI identities,
metrics, scheduler/owner logs, incomplete-result logs, complete version audit,
statistics and cleanup receipts remain under the three evidence directories.
The evidence index hashes714 explicit files and excludes generated kubeconfigs.
Index SHA256 `c75fe36ac19033a791d040c36e9eb7d48542f3aaec9705962c989269f07e12eb`.
Final fixture maintenance commit `5ebd80f` adds the observed paused-tail read to
the entry, a statement-only Go style fix and offline-tested bootstrap ownership/
partial-cleanup guards; it changes no product input or functional workload. These
last failure-path guards were verified offline, not by deliberately failing a
fourth real cluster.

All three clusters, node containers, their three anonymous volumes each, newly
created empty kind networks, client/ES/Weir Pods and log/client processes were
stopped and cleaned by exact owner/ID. Generated kubeconfigs were unlinked without
reading them. No builder was created; local images/binaries remain as inert
artifacts. Existing four exited M2 containers and `redis_default` remain untouched.
No test or load remains running. Local main is committed and clean at handoff.

This executor issued no push/PR/tag/release/production deployment. During execution,
the origin/main reflog independently advanced to5e5406d at05:41:41 Asia/Shanghai
with `update by push`; no corresponding push command was issued by this chat and
no active local Git hook was present. The external source of that ref update was
not verified; it is not attributed to this executor or hidden from handoff.

Reference three-replica2CPU/1GiB and4CPU/2GiB capacity, physical-host failures,
permanent node replacement/eviction, backend replication, cross-version upgrades,
other native runners/OS memory and24h qualification remain required/unverified.
OpenSearch upstream security evidence and Linux Mongo/kernel startup remain
blocked. Weir authentication stays excluded; general ProgramTransform remains
user-approved deferred/UNSUPPORTED. This stage stops for coordinator acceptance;
no subsequent stage or recurring task is started.
