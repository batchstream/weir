# Weir local artifact

This archive identifies an immutable source build. A GitHub Release associates
that source with a version and publishes SHA256SUMS and verified image digests.
Neither a local archive nor a release alone establishes production qualification.
Run `weir -version` (Windows: `weir.exe -version`) without a database or config.
Run `weir -config /path/to/node.json` with an explicit strict static configuration.
The included `node.example.json` is a template: replace the database endpoint,
database and collection with your pre-created isolated backend. Do not assume
that a development database exists on a deployment host. Configuration changes
require a restart; no user configuration is created automatically.

Application and peer gRPC are plaintext and require a trusted isolated network.
Backend TLS retains standard certificate and hostname verification. Use a read-only
explicit CA file when required; never disable verification. Do not embed credentials
or private CA material in the binary/archive/image. UNKNOWN writes must not be
implicitly replayed. `lua.v1` ProgramTransform requires configured `lua_worker`;
the worker has no hard per-process memory cap, so enable only trusted programs.

OCI runs as UID/GID 65532 with exec ENTRYPOINT /weir. Mount configuration and any
explicit backend CA read-only. A read-only root filesystem, dropped capabilities,
no-new-privileges and finite memory/CPU/PID limits are expected. Weir requires no
writable runtime directory in this profile; /tmp from the base may remain read-only.
Diagnostics default to loopback. An explicitly isolated monitoring deployment may
enable `diagnostics_allow_intranet` and restrict port 7449 using enforced network
policy. Use `/weir -probe live` or `-probe ready`
(optionally `-probe-address 127.0.0.1:7449`) for bounded exec probes without
loading configuration or connecting to databases. Exit 0 is healthy; all failures
exit 1. Probe/version/server flags are exclusive. See deploy/kubernetes in the
source repository for the canonical Deployment/ClusterIP and termination budget.
The distroless base supplies standard CA roots and timezone data; pinned base
updates require deliberate rebuild and revalidation.

Compiler floors: Go 1.27 macOS 13+, Linux kernel 3.2+, Windows 10/Server 2016+.
These are compiler requirements, not tested Weir OS support. Linux Weir additionally
requires the documented readable static cgroup-v2/RSS profile. This stage has only
short Darwin arm64 and Linux arm64 native artifact evidence; see docs/milestone-15.md
in the source repository for exact OS, backend, resource and lifecycle limits.
Other architectures are build-only. No signing/notarization, full vulnerability
scan, standard SBOM, Kubernetes, capacity or soak qualification is implied.
Keep gRPC receive-buffer compaction enabled (the default); setting
`GRPC_GO_EXPERIMENTAL_ENABLE_RECEIVE_BUFFER_COMPACTION=false` disables an upstream
security fix. Dependency and base updates require fresh SBOM/scans and behavioral
regression; a clean scan alone does not establish deployment safety.

Dependencies beside the archives distinguish the module graph from binary-linked
modules; they are inventory, not a security audit or a standard SBOM.

The manually dispatched `Verified Weir images` Actions workflow publishes only
`ghcr.io/batchstream/weir` and `ghcr.io/batchstream/weir-qualification`, tagged with
the complete source commit. It is restricted to the public batchstream/weir main
branch. No latest tag or automatic deployment is created. Supplying the optional
`release_version` input publishes a stable Git tag, verified archive assets and
matching version image only after both native platform smoke jobs pass. See
`docs/production-deployment.md` for release and target-environment gates.
The separate qualification image contains only the internal
integration helper and the same pinned runtime base; it is not a product API.

The workflow first runs native Linux amd64/arm64 default CGO0 tests, independent
CGO1 race, static vet and tool regressions. It then uses the existing clean-source
packager for two independent six-target product builds and two Linux helper
builds, validating both OCI indices twice. SDK, Buildx, BuildKit and regctl pins
are in scripts/ci-tools.json. Public SDK/module download is preparation; tests
and builds run with offline proxies, local toolchain, disabled Go env/workspace
and read-only modules. Source exports and build caches are independent per round.

Verified OCI archives are imported by standard regctl without rebuilding. Final
registry content is downloaded and checked before the full source tag is set;
existing tags with different digests fail without overwrite. A fresh anonymous
client downloads every platform blob of each index and checks source labels,
base layers and extracted binary hashes. Each native Linux platform then runs
the exact product `-version` and a one-second, network-disabled helper pacing
smoke under finite CPU/memory/PID/time limits. This proves execution and output
contracts only; it does not qualify generator timing, databases, EKS or 24h soak.

GHCR packages initially default to private. An anonymous failure is retained as
a blocked delivery, even after a successful authenticated push. The approved
package administrator may select Public in each package's settings; no token,
organization policy or registry workaround is needed. See docs/milestone-24.md
for actual attempt URLs, source/delivery commits, public status and digests.

Darwin uses current physical footprint in bytes from the system libproc API,
through fixed purego v0.10.2. Its Apache-2.0 and Go-derived BSD-3-Clause notices
are included in Darwin archives under licenses/. The existing CGO_ENABLED=0
single-binary build still makes native system calls; no separate dylib installation
or runtime compiler is required. Go or bridge upgrades require renewed native
validation. Linux and Windows binaries do not link purego.

One Guard samples every 100ms and closes admission at 80% of the explicit soft
memory budget, reopening at 70%. Darwin OS sampling failures close admission;
Go heap estimates cannot reopen it. A synchronous kernel API cannot be forcibly
canceled if the kernel stalls; the measured short shutdown bounds are not a
promise to interrupt a hung kernel call. Windows still uses a Go-only fallback.
