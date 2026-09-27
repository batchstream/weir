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

Execution evidence and final identities are appended after validation. Until then,
only implementation is claimed. Evidence root: `.testdata/m20-20260928`.

Three replicas on at least two workers, node failures, production DNS/network,
backend replication failover, other native architectures, capacity and 24-hour
soak remain unverified. Linux Mongo8.0.32/kernel7.0.12 startup and OpenSearch
upstream security evidence remain blocked. Weir authentication stays explicitly
excluded and general ProgramTransform remains user-approved deferred/UNSUPPORTED.
