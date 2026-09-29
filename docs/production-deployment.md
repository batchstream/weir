# Production deployment

Weir is a synchronous data plane for pre-created MongoDB collections and exact
Elasticsearch/OpenSearch profiles. Deploy the server, [Go SDK](https://github.com/batchstream/weir-go)
and [Helm chart](https://github.com/batchstream/weir-charts) as a versioned set.
ProgramTransform remains unsupported. Static peers and configuration changes
require controlled restart; Weir does not perform backend failover or replay writes.

## Network and access boundary

Application and peer gRPC use plaintext HTTP/2 and have no built-in authentication
or authorization. A Store route is not an authorization boundary: every admitted
application client can address every configured route. Use separate Weir deployments
and backend accounts for tenants or applications that must not share access.

Use the chart's ClusterIP Service, default-deny NetworkPolicy and explicit ingress
selectors for the intended client namespace and Pods. Permit egress only to cluster
DNS, selected backend endpoints and configured peer Pods. The cluster CNI must
actually enforce NetworkPolicy; verify a permitted and a forbidden client from
separate Pods. A rendered policy is not evidence of enforcement. Do not expose
the Service using a public LoadBalancer, NodePort or unauthenticated ingress.
If traffic crosses the isolated cluster trust boundary, the operator must supply
an authenticated TLS/mTLS proxy or service mesh and restrict direct Pod access.

Diagnostics default to loopback. To scrape from an in-cluster Prometheus, explicitly
set `diagnostics_allow_intranet: true` with `diagnostics: "0.0.0.0:7449"` and allow
that port only from the monitoring namespace/Pods. This exposes only the bounded
`/livez`, `/readyz` and `/metrics` handlers; there is no debug or pprof endpoint.
The existing loopback exec probes continue to work. Readiness means the serving
lifecycle is ready, not that every backend request will succeed. Monitor backend
failure outcomes, UNKNOWN mutations, admission rejection and memory pressure too.

Store configuration and backend credentials in an operator-created Kubernetes
Secret mounted read-only. The chart can reference that Secret; do not put actual
credentials in chart values, a ConfigMap, Git or release assets. TLS CA bundles
are explicit read-only mounts. MongoDB authenticated connections require verified
TLS and SCRAM-SHA-256; Search credentials require HTTPS with verified certificates.
Credential or CA rotation requires an independently validated replacement config
and rollout. Existing Secret contents are not part of automated qualification.

## Configuration and resource checks

Run `weir -check-config /path/to/node.json` before deployment. It parses the same
strict configuration used by startup, without DNS, backend connections, listeners
or CA-file access. A successful check does not prove backend availability, account
permissions or readable certificate mounts; startup qualification proves those.

The chart uses non-root execution, a read-only root filesystem, dropped capabilities,
startup/readiness/liveness probes and bounded termination. Set explicit CPU/memory
requests and limits, and keep `memory_mib` below the container memory limit with
room for transient runtime/backend buffers. Use at least three replicas across
two workers for the corresponding availability qualification. A single replica
is a functional installation, not the same availability profile.

Count backend concurrency across all live Local adapters, including maxSurge,
terminating Pods and other applications. Start with `local.concurrency: 2` and
calibrate against the actual backend workload. Bounded local socket ownership
does not imply an instantaneous bound on backend work after lost replies.

Use a PodDisruptionBudget and topology spread or anti-affinity. For rolling
updates, keep enough capacity for surge and termination overlap. New connections
can reach replacements while existing gRPC connections stay pinned until closed;
clients must handle connection loss without automatically replaying writes.

## Version and rollout procedure

All main changes use a reviewed PR. `CI` runs native Linux amd64/arm64 default
tests, race detection, vet and offline qualification-tool tests for every PR and
main push. `Verified Weir images` is manually dispatched from main. Leave
`release_version` empty for a source-image build, or set a canonical version such
as `v0.1.0` to publish a release after both native image smoke jobs succeed.

Release assets contain reproducible archives for six build targets, source and
linked-module inventories, the build/delivery receipts and SHA256SUMS. The OCI
version tag points to the exact verified source image; deploy its immutable digest.
`weir -version` reports the source commit, making the running artifact independently
identifiable. A conflicting version tag or release asset is never overwritten.
A repeated dispatch of the same source/version verifies existing content and can
finish uploading missing assets after an interruption. A changed source must use
a new version.

1. Confirm the SDK module version, chart version, image source and digest.
2. Validate configuration, then install in an isolated namespace with owned data.
3. Run SDK Read, all mutation outcomes, Bulk, Native and Scan against real backends.
   Confirm stream End counts and final gRPC OK; an UNKNOWN mutation is recorded
   for reconciliation and is never replayed automatically.
4. Verify Prometheus scraping and permitted/forbidden network paths. Exercise
   backend interruption/recovery and a rolling image/config update with active
   clients. Check retained data, outcome accounting and termination budgets.
5. Run the workload qualification described below, then promote the same digest
   and configuration. Roll back with the previously verified chart/config/image;
   a rollback cannot undo a mutation that already reached a backend.

## Evidence required for a production claim

A consumable software release requires passing PR checks, reproducible artifacts,
verified image/source identity, a usable SDK and chart, and real end-to-end smoke.
These establish a deployable release. They do not establish an arbitrary deployment's
capacity, security controls or high availability.

Production qualification additionally requires the exact backend versions and
permissions, enforced network isolation, reference resources, three replicas on
at least two workers, a measured workload/SLO, overload recovery, rolling upgrade
and rollback, fault/UNKNOWN correctness, resource stability and at least 24 hours
of sustained load on the final revision. Record actual timestamps, image digests,
SDK/chart revisions, backend versions, topology and workload with the result.
Changing the tested artifact or meaningful configuration starts new qualification
evidence; older evidence remains historical.

The six archive targets are a build matrix, not six equivalent production platforms.
Each claimed native platform needs its own runtime, resource and backend evidence.
The current Kubernetes target is Linux; macOS/Windows archive availability cannot
substitute for Linux deployment tests or establish their own production support.
The broader historical platform/soak gates and unresolved OpenSearch security
findings in [production-readiness.md](production-readiness.md) remain recorded.
No outstanding gate is waived by publishing an SDK, chart or release.
