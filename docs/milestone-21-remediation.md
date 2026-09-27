# M21R: fixture ownership remediation

2026-09-28. Baseline `601b7c4be70043fc9afa9f180449a88336db7421`;
implementation and executed fixture `ff783e18fd2db0894e1c85c62865558784e7d1d9`.
Executor chat `01a0e4ec-3975-78e1-9cc9-c8ac80f6a649`.

**Original M21 remains pending remediation acceptance. M21R execution evidence
is pending independent coordinator acceptance.** This does not qualify the whole
product, reference capacity, cross-host recovery, version upgrades or a 24h soak.
The original [M21](milestone-21.md) three runs, failed observations and coordinator
ownership counterexamples remain preserved. No product or workload gate was relaxed.

## Root causes and bounded corrections

- `Fixture` now exclusively creates a new evidence root with mode0700, before
  opening files or calling external tools. Its parent must already exist. It does
  not resolve the final path component before mkdir: existing files, directories,
  root symlinks (including dangling links), and existing home/docker configurations
  all fail before they can become fixture paths. Errors report path state only.
  Newly created empty subdirectories unwind on partial directory creation failure;
  failures after initialization retain owned evidence. No resume/force/clear mode.
- The CLI rejects optimized Python at module entry, before fixture imports or
  side effects. Evidence assertions therefore cannot disappear under `-O` or
  `PYTHONOPTIMIZE=1`. Independently, owner/name/ID, opt-in, collision and empty-network
  safety guards use explicit conditions/exceptions. Offline direct cleanup tests
  also pass with optimization enabled, including a foreign owner's saved volume.
- Read-only preflight checks running containers, owner label, exact node names,
  the kind network and exact client image tag before `prepare`. Preparation and
  bootstrap require successful preflight. Existing tags cannot be overwritten
  merely because an owner collision is discovered later. Docker commands use the
  newly created empty configuration directory; no user Docker auth or existing
  kubeconfig is opened.
- Cleanup verifies each recorded node's owner/ID and volume list. A failed or
  unconfirmed node deletion retains its volumes and reports them; independent
  nodes, restoration, network and access-file cleanup continue. Network deletion
  requires the recorded ID and no attached containers. Inventory failures are
  recorded independently instead of hiding the other cleanup outcomes.

This is one serial fixture owner's boundary, not a concurrent daemon lease or
hostile same-user filesystem defense. No locks, controller, feature expansion,
product authentication, replay or compatibility layer was added.

## Offline failure evidence and regression

Evidence directory: `.testdata/m21r-20260928-validation/`. All negative cases use
new temporary paths, synthetic sentinels and mocked external commands; no negative
test starts Docker or kind. Existing-path tests reject before file-content reads,
retain inode/content/mtime, and verify no subprocess or prepare call. Collisions
cover running containers, owner, node, network and client tag. Other tests cover
normal preparation order, prepare failure, partial directory creation, retained
nodes after fast bootstrap failure, mismatched IDs/nonempty networks and independent
cleanup after injected errors.

| Check | Result |
| --- | --- |
| Fixture Python suite |11 tests PASS;0.206s |
| Optimized Python suite |6 tests PASS,5 ordinary CLI cases explicitly skipped because the CLI refuses optimization;0.196s. Both `-O` and `PYTHONOPTIMIZE=1` subprocess refusals check no fixture side effect |
| Coordinator counterexamples adapted to the corrected entry |3/3 PASS; originals and original failing logs/JSON copied unchanged into the validation directory |
| Default CGO0 full test, count1 |PASS62.366s |
| Default CGO1 full race, count1 |PASS65.097s |
| Default / integration / Linux arm64 integration vet |PASS0.295s /2.239s /2.333s |
| testkube64KiB/64KiB+1 HTTP evidence boundary |three race rounds PASS2.643s |

The original access-file probe now expects `FileExistsError`, because the old root
is rejected before its injected prepare failure. The ordering probe injects the
same collision into the new `preflight` boundary instead of `bootstrap`; its
assertion still forbids prepare/import before refusal. The original symlink
assertion is unchanged. These changes do not weaken original-resource preservation
or the absence of external writes. The maintained suite additionally uses actual
mocked Docker metadata responses through the complete main entry.

Pinned Go1.27.1; GOENV=off, GOTOOLCHAIN=local, GOWORK=off, GOPROXY=off,
GOSUMDB=off. All tests completed before the native run. Product70 inputs match the
M20 receipt and exact source `315819fcd2c0cae1c22604e85ccdb5bb9291f585`; archive
checksums and OCI blobs/config/manifest were verified. No new six-platform build
or unchanged Java scan is claimed.

## One real regression, unchanged frozen profile

Only one full real cluster run: owner `weir-m21-20260928-remediation`, evidence
`.testdata/m21r-20260928/`. Exit0, bootstrap27.425s, functional180.913s,
including preparation/cleanup total214.403s. No retry, traffic/resource reduction,
shortened timeout or system/daemon setting change was issued.

Before it, Docker reported8CPU,8,319,770,624B,7.0.12-linuxkit/aarch64 and no running
containers. A separately named, labeled, network-none, read-only64MiB/0.1CPU
short-lived container read `/proc/meminfo` and memory pressure: MemAvailable
7,607,248KiB and memory PSI0. It and its anonymous volume were removed by exact
owner/ID before the cluster. The fixture uses the unchanged `kubernetes-smoke.json`:
CP2CPU/1280MiB, worker3.5CPU/3840MiB, worker2 2.5CPU/2048MiB; Weir0.5CPU/384MiB
with256MiB process budget, ES1536MiB/384MiB heap, client256MiB. Total node hard
limits7GiB. Kind0.33.0, Kubernetes1.36.4 and ES8.19.22 remain fixed.
Default stall30s, close5s and grace15s remain unchanged, as do all operation limits.

The same exact M20 product image is used throughout:

- OCI index `sha256:ab777e7e00cc83f2f322f4324bca566fdfe9b7970b1dae458d39844cbe7142b7`.
- arm64 manifest `sha256:f2799aae8e5468ece29fa820cdc6546f6b409c86b48de05376277391c2365270`.
- image ID `sha256:caa699e6ca172cbfa24ed4d311f4cb346817a354b05df52601abd954edcb4dab`.
- executable SHA256 `9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32`, checked for all9 executors.

The separately rebuilt integration client SHA256 is
`f916b5bd4f075f85ba319b160fe9c0faca4b249128f4f5c95184f89bd076dd0f`, image
`sha256:af49237e1db47c8ab51524862947cfa65fb49aa5f5c9db36f7c2d022e193163e`.
Its source receipt identifies the executed implementation; it is not `/weir`.
M21's earlier client identity is preserved separately, not reused as this identity.

| Observation | Result |
| --- | --- |
| Three revisions |3 Ready on two workers each, no Weir on CP; same image and immutable C2→C1→C2 config references |
| Old Bulk stream / new connections |one original stream, indexes0–39, End40/40 and EOF;6 independent new Service connections succeed. Record counters at hold completion43/18/0: this run reached2 of3 Pods, no equal-distribution guarantee |
| Roll / rollback |14.155s /13.565s; previous revision process IDs absent from Running CRI before the next revision |
| Default-stall drain |first terminating partial uploads ended5.143s /5.093s after setup; other concurrent uploads8.35–11.41s as later Pods drained; no30s stall wait |
| Worker fault |exact owner/ID-checked worker2 paused50.863s; Ready=Unknown/NodeStatusUnknown sample49.993s after pause command; endpoints withdrew and healthy-worker Service call succeeded before unpause |
| Recovery |successful Service/direct calls; same post-rollback Pod UIDs; recovered sample61.562s after pause command |
| Client/persistence |193 unique mutation IDs:184 APPLIED,9 UNKNOWN.191 stored records, all version1;7 UNKNOWN persisted,2 UNKNOWN absent. No UNKNOWN was resent |
| Process/accounting |sampled Running CRI maximum5 total/4 on a worker; conservative6 retained.9 final Local receipts each owned0, peak≤C+1 and acquired=released |
| Quiescence / final shutdown |pending/retained/execution/session/ingress ledgers0; last scheduler census18/16/18 live goroutines, ordinary backend idle owners1/0/1. After scale0 all Weir Pods/Running CRI/owners0 |
| Restarts / OOM |all Weir, ES and client restartCounts0; all three node oom/oom_kill/oom_group_kill0 |

Local metric sums are non-atomic observations: sampled active executions3, backend
owners7, ingress connections4/sessions7. Paused processes retain their last sample;
there is no fresh observed zero on the paused node. Exact per-owner peak events
remain separate from sampled aggregate peaks and DB counters.

Complete DB JSON shows HTTP current connections4 steady,8/8 during revision
transitions,4 during pause,3 after recovery,1 after scale0 (the direct observer).
Refresh listeners0/3/3/1/0/0; observed write active/queue/rejected0 throughout.
The held write was already persisted before its20s client timeout; the paused-tail
listener represents incomplete refresh/response retirement, not continued write
execution. Recovery and fixture refresh released it. These values do not establish
an unconditional remote C-derived bound under arbitrary partitions.

Control-plane memory remains pressured: current1,341,374,464 of1,342,177,280B,
`memory.events.max=1109`, OOM0; anon649,707,520B and file609,820,672B.
Healthy/faulted workers current3,314,561,024 /477,745,152B; anon967,479,296 /
140,836,864B and file2,228,506,624 /306,008,064B. These aggregate samples include
cache and kernel usage. They are not spare capacity, Weir RSS or peak memory.
The original M21 CP max574/OOM0 evidence is unchanged.

## Cleanup, preserved discrepancy and handoff

All fixture children were stopped/reaped. The3 node containers, their3 anonymous
volumes, client/ES/Weir Pods and newly created empty kind network were removed by
recorded owner/ID. Generated kubeconfig is absent without having been read or
printed. No load, log follower, test, node or cluster remains running. The short
VM probe was separately removed before bootstrap. Local client image/binaries
remain only as inert artifacts. The old4 exited M2 container identities, existing
volumes and `redis_default` identity remain unchanged.

A stricter initial whole-network inventory comparison **failed**: built-in Docker
`bridge` changed from ID284a66736a67 to dd70fe6746ef. Its new creation timestamp
22:17:35.962UTC falls within the VM memory-probe window and precedes the cluster's
first command at22:19:23.363UTC. No command issued by this executor modified or
removed that default network. Available Docker event history retained only the
owned kind network's cleanup, so the cause is not established. No global network
was restored or modified to conceal this discrepancy. The original audit script,
failure log and both inventories remain. The final ownership audit records
`networks_unchanged=false`, the exact difference, and separately verifies all owned
resources gone, all existing volumes/containers unchanged, and other networks
(including redis_default) unchanged. This is an environment evidence limitation
for coordinator review, not an unqualified claim of identical global inventory.

The credential-free [receipt](milestone-21-remediation-receipt.json) references the
hash index over native evidence, regression logs, adapted/original probes and
cleanup audits:281 files, index SHA256
`9b10e145a28c2de301fcc641ad1eb0a8c2f66a0b485eafa87c87062d0dc003e8`.
Original M21 reports, receipts and all three evidence directories
are preserved; the coordinator's original review still records nonacceptance.

No push/PR/tag/release/deployment, existing-secret access, subsequent phase, new
chat or automation was performed. Main receives only local implementation and
report commits. Work stops for coordinator acceptance after the final callback.
Reference three-replica2CPU1GiB/4CPU2GiB capacity, permanent-node/physical-host loss,
backend replication, cross-version compatibility, other native platforms and24h
remain unverified; OpenSearch security and Linux Mongo/kernel gates remain blocked.
Weir authentication stays excluded; ProgramTransform stays deferred/UNSUPPORTED.
