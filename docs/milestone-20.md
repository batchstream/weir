# M20 — minimal Kubernetes deployment and single-replica lifecycle

Baseline: `6573fd0b63d129930e21e14d13c7fea1eaff4fd4`. This stage does not
qualify all platforms, multi-node availability, capacity or 24-hour operation.
M19R has received the coordinator's limited independent acceptance; original
M19's unimplemented history remains unchanged.

Implementation: `cmd/weir/probe.go`, CLI-only fixed live/ready HTTP checks with
750 ms total deadline; no configuration, backend, Guard or listener construction.
The canonical configuration is `deploy/kubernetes/weir.json`, with deployment,
resource, Secret, probe, drain and UNKNOWN instructions beside it. No shared
lifecycle, Core execution semantics or backend retry path was changed.

The frozen owned fixture is kind v0.33.0 / Kubernetes v1.36.4, one control-plane
node running natively on Linux arm64. The node limit is 6 CPUs / 6 GiB / 2048 PIDs;
Weir is 1 desired replica at 2 CPUs / 1 GiB, 768 MiB soft process budget; one
Elasticsearch 8.19.22 at 2 CPUs / 2 GiB with 512 MiB Java heap; one bounded client
Pod at 0.5 CPU / 256 MiB. Recreate has no rollout surge; manual deletion may overlap
one terminating and one replacement Weir. This is a single Docker VM, not multiple
physical nodes. Bootstrap is bounded to 20 minutes and functional validation to
10 minutes. All Kubernetes operations use a freshly generated explicit kubeconfig,
unique cluster/context, empty HOME/Docker client config and loopback API endpoint.

Official selection evidence:
[kind v0.33.0 release](https://github.com/kubernetes-sigs/kind/releases/tag/v0.33.0),
[Kubernetes supported releases](https://kubernetes.io/releases/),
[probe semantics](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/),
[termination lifecycle](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/).
The selected node is
`kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed`.
These URLs and successful immutable tool/image acquisition were checked on
2026-09-28; the kind release and node availability were checked directly, not
inferred from a moving documentation homepage.

## Executed result and identity

The limited single-replica native gate passed on 2026-09-28. This is executor
evidence awaiting the coordinator's independent acceptance, not whole-product
production qualification. Evidence root: `.testdata/m20-20260928`; reproducible
artifacts: `dist/m20/{first,second}`. `evidence-index.json` records explicit file
sizes/hashes for159 files and verifies all70 current product build inputs against
the artifact receipt. Index SHA256:
`7c9e996c37b072dacfaf74ac87dc223c5343d6b63d657cc1973cc6f6be2fbdd1`.

- Implementation and accurate product artifact source:
  `315819fcd2c0cae1c22604e85ccdb5bb9291f585`.
- Test-client correction: `7b933820669c828d7bfe56285a00c8ffd67779ed` (Bulk index
  starts at zero). It changes no product/package input. Subsequent delivery
  commits only record evidence/documentation; use the source above for rebuilding.
- OCI index: `sha256:ab777e7e00cc83f2f322f4324bca566fdfe9b7970b1dae458d39844cbe7142b7`.
- Linux arm64 manifest:
  `sha256:f2799aae8e5468ece29fa820cdc6546f6b409c86b48de05376277391c2365270`;
  config/runtime image ID:
  `sha256:caa699e6ca172cbfa24ed4d311f4cb346817a354b05df52601abd954edcb4dab`.
- Running `/proc/PID/exe` SHA256:
  `9def37fc9f4d552d35af552f87f86ba0b45bd2bdcf948114237fb5d312c97a32`, identical
  to the Linux arm64 artifact. The actual Pod ran
  `weir-m20:315819fcd2c0cae1c22604e85ccdb5bb9291f585`, locally loaded from the OCI,
  and `/weir -version` reported that exact clean-commit source, Go1.27.1 and linux/arm64.
- Linux amd64 manifest (build only):
  `sha256:876959cf37d228e884089c8b68cff39388dc056bcc92b72af8336b505da23563`;
  config `sha256:2694d7a56a0df27f95d6000983a46d8261f1ce37aee0fd2c65302dbbbb573761`.
- `dist/m20/comparison.json`: six binaries, six archives and dual-platform OCI
  receipts identical between independent clean exports/caches. First receipt
  SHA256 `21dc8eb025bc0f8163620f602ef4b86d52b1b40d259ce79f3df3e8e326a22cff`.
  No dependency, base-image or shared lifecycle change; no new security scan or
  backend-version upgrade is claimed.

The native test client is separate from the product image and built only with
`-tags integration`: `internal/testutil/testkube`. Its final image config is
`sha256:90a7280bf50e2d2fdd371cd3ab647ea8185f1e50f9889216df2fabebb08f4a15`, binary
SHA256 `c674a1d198d12d9ce2ca3b0fcb2999ce2bff31c1d0e59d2d703be8c52f94f8f9`.
It requires `WEIR_KUBE_INTEGRATION=1`, disables gRPC retries/proxies, has bounded
calls and idle lifetime, and never reads kubeconfig or invokes the Kubernetes API.

## Native fixture and observations

Host is Darwin arm64; Docker VM has 8 CPUs / 8,319,770,624 bytes, initially no
running containers. Native node: Debian13, Linuxkit7.0.12/arm64, Kubernetes1.36.4,
containerd2.3.4, cgroup-v2. CNI is kindnet (ptp + host-local + portmap), subnet
10.244.0.0/24; no host application port, ingress, LoadBalancer or user HOME mount.
API listen address is explicitly 127.0.0.1 with a random port. The official native
kind binary SHA256 is `0c8c7dbe5e23594a198b786c4bc13dacc101fa6196b0cb0b23a1ca44e61f4b4f`;
new native kubectl1.36.4 SHA256 is
`c9e4f713d6fee0043a3d835cca13077cda2bc0973840eb9779360df0b5bdfc69`.
Both were verified against official fixed-version checksums. The existing amd64
kubectl was used only for a client-version preflight; native kubectl ran the cluster
validation. No application or node ran through cross-architecture emulation.

One real ES8.19.22 Linux arm64 image was used, fixed manifest
`sha256:c2a3ed5f968be6d59c960aa0c60cfdaee667b6bc8211142021a41d0e85b43237`, config
`sha256:a1cc67962f24c058c854acc6aab0d0adaefefc945c0bfaaebb52aba6129de160`.
It is a credential-free isolated HTTP fixture, one shard/zero replicas with a
1GiB bounded emptyDir. It is not a TLS, replication or Linux Mongo qualification.

The application resource settings are the reference 2CPU/1GiB values, not a
smaller resource smoke profile. The fixture explicitly uses a **3-second input
stall deadline** to observe a finite partial unary upload during drain (the
canonical default is 30 seconds); this does not change the 5-second process close
cap or 15-second Pod grace. A held native `refresh=wait_for` write and an incomplete
Read upload share one bounded client connection. This is not a capacity workload.

| Check | Actual evidence |
| --- | --- |
| Bootstrap and manifest | 27.133s bootstrap; server-side dry-run and Deployment rollout passed; functional phases completed in 178.224s including fixture-observation corrections |
| Startup/live/ready | Actual Kubelet exec probes, `started=true`, Ready=true and explicit in-container `/weir -probe live/ready` exit0; only loopback diagnostics, no diagnostic Service port |
| Identity and security | Initial and replacement Pod imageID equals the original OCI config; PID1 `/weir`; UID/GID65532, empty capabilities, NoNewPrivs=1, seccomp filter mode2, kernel root mount read-only, config mounted read-only, no token mount |
| Resources | Allocated requests/limits 2CPU/1GiB; actual CRI CPU quota200000/period100000 and memory limit1073741824; Guard budget805306368, RSS20807680, cgroup current8282112/limit1073741824, valid leaf-v2, unknown=0 at initial sample |
| Service DNS | Independent client Pod uses `weir.m20.svc.cluster.local:7447`; initial, recovered and replacement Mutate=APPLIED, Read=found, Bulk index0=APPLIED plus End/EOF; direct ES readback confirms each version1/n1 |
| Backend outage | SIGSTOP/SIGCONT only the owned ES launcher/server Java PIDs; mutation fails in 2.005876s with NOT_APPLIED/UNAVAILABLE during index qualification before write dispatch; independent ES GET confirms the outage document absent; no implicit retry |
| Outage health/recovery | live/ready remain successful while business fails; initial Pod restartCount remains0; fresh post-recovery reads/writes/Bulk succeed |
| Active deletion | Real native bulk write persisted, refresh response held; actual Pod deletion/SIGTERM; first observation at0.229s has ready503/live200 and EndpointSlice ready=false/terminating=true (serving may still be true in the asynchronous slice) |
| Exit and replacement | Old Pod disappeared after3.727s; captured terminated state exit0/Completed; replacement has a different UID, same imageID, Ready and restartCount0; client remains a separate Pod throughout |
| In-flight result | Native receives RST_STREAM/INTERNAL_ERROR without a complete response, so effects remain UNKNOWN; no client replay. Direct ES state is version1 before/after replacement. Partial Read ends with bounded input timeout. No claim that disconnection proves NOT_STARTED |
| Negative CLI | Accurate Linux image rejects absent listener, non-loopback target, mixed probe/config and missing config with exit1; exact Darwin artifact also rejects malformed config, and hung/partial responses exit1 in0.763/0.771s with peer close and child Wait |
| Peak scope | Desired1, observed old/new process overlap only; final node sample1.692GiB/6GiB and412 PIDs, no OOM or unexplained Weir restart. This is a sample, not a peak-memory or sustained-capacity claim |

Initial Pod UID `c5e53de8-b45a-41c8-b4e7-918e3da8d481`, replacement UID
`1278ac62-481f-4559-943c-0ea7da809e65`. `termination.json` preserves every observed
health/EndpointSlice/terminal state; `active.log` and direct backend persistence
are separate evidence. Existing default no-replay/UNKNOWN regression remains
passing; this actual native incomplete-response test adds one real no-replay case,
not all possible lost-ack/partition paths.

## Regression and repeatable commands

All default Go commands were offline (`GOENV=off GOTOOLCHAIN=local GOWORK=off
GOPROXY=off GOSUMDB=off`) with `.tools/go1.27.1/bin` first in PATH and inherited
GOROOT removed. Real Kubernetes work was separately opt-in; default tests do not
start a cluster, Docker or a backend.

| Regression | Result |
| --- | --- |
| `CGO_ENABLED=0 go test -count=1 -timeout=4m ./...` | PASS 60.405s |
| `CGO_ENABLED=1 go test -race -count=1 -timeout=4m ./...` | PASS 63.626s |
| `go vet ./...`, `go vet -tags integration ./...` | PASS; corrected final client also independently vetted |
| CLI/app/server probe/diagnostics/Close/Shutdown/Drain/admission three-round race | PASS 100.887s; no shared lifecycle changes |
| Whole `internal/overload` three-round race | PASS 4.028s; the earlier name-filter selected no Guard tests and is not used as its evidence |
| `python3 -m unittest discover -s scripts -p package_test.py` | PASS |
| Six-target/two-OCI clean export build twice | PASS; all receipts equal, only arm64 actually run |

Build and fixture command records/scripts are retained in the evidence directory:

```sh
# Exact product source above must be checked out clean before formal packaging.
env -u GOROOT PATH="$PWD/.tools/go1.27.1/bin:$PATH" \
  BUILDX_CONFIG="$PWD/.testdata/m20-20260928/docker/buildx" \
  python3 scripts/package.py --output dist/m20 --oci --builder weir-m20-20260928

# Opt-in client; never installed in /weir image.
env -u GOROOT PATH="$PWD/.tools/go1.27.1/bin:$PATH" \
  GOENV=off GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off \
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -tags integration -o .testdata/m20-20260928/client ./internal/testutil/testkube

# command.py supplies isolated HOME/DOCKER_CONFIG/KUBECONFIG and explicit context.
python3 .testdata/m20-20260928/bootstrap.py
python3 .testdata/m20-20260928/fixture.py
python3 .testdata/m20-20260928/cleanup.py
```

Do not rerun these commands against stale owner names or a removed kubeconfig.
Create a new owner and fresh evidence directory first. The retained fixture script
includes the documented observation corrections; actual resumed execution is in
`functional*.log` and `resume*.py`. `freeze.json`, `kind.json`,
actual `weir.json`, `node.json`, image receipts and scripts preserve the exact
executed values. The packaging builder used fixed BuildKit v0.32.2
`moby/buildkit@sha256:28a898719c18a33f4e8000685287fa36fd0dd9560c6440227d3a732d79bb41d8`
and Buildx v0.36.1-desktop.1; it had a separate1GiB/2CPU cap and was stopped before
functional Pods were created.

## Preserved failures and cleanup

No failed observation is silently counted as success:

- Initial inherited Go settings selected inconsistent toolchain/root paths;
  the documented pinned/offline environment corrected them. No toolchain or
  dependency change was made to fix tests.
- The first test client used two wrong generated API symbol names; compile/vet
  failed before execution. Corrected compile/vet passed. Review also caught Bulk
  index7 before any cluster call; the final client uses the required index0.
- Classic Docker `load` rejected the OCI tar for missing `manifest.json`. The
  one evidence-based format correction imports the **original archive** directly
  into the owned node's containerd with `ctr images import --platform linux/arm64
  --index-name docker.io/library/weir-m20:<source> -`. Config and actual executable
  hashes match the original receipt; no image reconstruction or daemon change.
- ES has two Java PIDs, not one. The initial assertion stopped before SIGSTOP;
  the resumed phase validated the exact owned PID list and resumed both in finally.
  This did not change workload, resource or time limits.
- A shell observation attempted `findmnt` inside the distroless mount namespace,
  where it is absent. The kernel `/proc/PID/mountinfo` and CRI readonly/resource
  state subsequently verified the actual mount; the failed command output remains.
- The old CRI container was already garbage-collected when a redundant final
  inspect ran. Its exit0/Completed state had already been captured twice from the
  real Pod before deletion. That captured state is the exit evidence; no replacement
  run or fabricated CRI result was used. Remaining replacement/negative assertions
  finished within the original functional deadline.

Cleanup checked exact cluster labels/IDs, generated node volume and newly created
unused kind network; kind deleted the one owned node and all its Pods/backends/
client/material. The node volume, BuildKit container/state volume and owned network
are absent; generated kubeconfig was unlinked without reading/printing it. No
port-forward was started. Final Docker running-container list is empty; existing
`redis_default` was retained. Test/build/client processes all ended. Only sanitized
evidence, generated credential-free configuration and local image/binary artifacts
remain. No push/PR/Git tag/release, existing cluster, production or paid resource was used.

Three replicas on at least two workers, node failures, production DNS/network,
backend replication failover, other native architectures, capacity and 24-hour
soak remain unverified. Linux Mongo8.0.32/kernel7.0.12 startup and OpenSearch
upstream security evidence remain blocked. Weir authentication stays explicitly
excluded and general ProgramTransform remains user-approved deferred/UNSUPPORTED.
