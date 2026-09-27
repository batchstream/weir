# Weir local artifact

This is an unsigned local build, not a production qualification or release.
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
implicitly replayed. ProgramTransform remains UNSUPPORTED.

OCI runs as UID/GID 65532 with exec ENTRYPOINT /weir. Mount configuration and any
explicit backend CA read-only. A read-only root filesystem, dropped capabilities,
no-new-privileges and finite memory/CPU/PID limits are expected. Weir requires no
writable runtime directory in this profile; /tmp from the base may remain read-only.
Diagnostics remain loopback-only, not a Pod-IP probe or a public management service.
The distroless base supplies standard CA roots and timezone data; pinned base
updates require deliberate rebuild and revalidation.

Compiler floors: Go 1.27 macOS 13+, Linux kernel 3.2+, Windows 10/Server 2016+.
These are compiler requirements, not tested Weir OS support. Linux Weir additionally
requires the documented readable static cgroup-v2/RSS profile. This stage has only
short Darwin arm64 and Linux arm64 native artifact evidence; see docs/milestone-15.md
in the source repository for exact OS, backend, resource and lifecycle limits.
Other architectures are build-only. No signing/notarization, full vulnerability
scan, standard SBOM, Kubernetes, capacity or soak qualification is implied.
Dependencies beside the archives distinguish the module graph from binary-linked
modules; they are inventory, not a security audit or a standard SBOM.
