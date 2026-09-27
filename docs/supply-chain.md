# Local supply-chain checks

This procedure produces evidence for a specific local artifact set. It does not
publish, sign, upload source, or qualify a production deployment. Current results
and remaining risks are recorded in [M18](milestone-18.md), with the artifact
baseline in [M17](milestone-17.md). M15/M16 remain historical evidence.

## Fixed inputs and tools

Build only through `scripts/package.py` from a clean commit, using the exact Go
version in `.go-version` (currently 1.27.1). The script exports the captured full
source SHA, including when HEAD subsequently moves. Its two source directories
and compilation caches are independent. Keep both receipts, SHA256SUMS, archive
hashes, each binary's build info, and both OCI platform manifests/configs/layers.
SHA256 proves identity relative to a trusted receipt, not publisher authenticity.

M16 and M17 use [govulncheck v1.8.0](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck),
[Syft v1.52.0](https://github.com/anchore/syft/releases/tag/v1.52.0), and
[Grype v0.119.0](https://github.com/anchore/grype/releases/tag/v0.119.0).
Install only into a new ignored project directory, from official releases with
verified published checksums (Go tools also use the public module checksum DB).
The M16 tool/checksum receipt is `.testdata/m16-evidence/tools.json`. These tools
are not shipped in Weir. Never scan the checkout, HOME, Docker daemon, fixtures,
existing credentials, or an unbounded directory tree. Use explicit binary paths,
OCI archives and a whitelisted immutable Go source export. Use an empty HOME,
explicit scanner configuration, and no existing Docker auth configuration.

Disable scanner update checks, remote license enrichment, external lookup, user
ignore rules and VEX inputs. Syft should not inspect local module/vendor caches.
Keep scanner native output as well as CycloneDX; neither is a substitute for the
original artifact. No report is manually edited to remove a finding.

## Databases and standard output

Snapshot the [official Go vulnerability DB](https://go.dev/doc/security/vuln/database)
index and all historical OSV records for the union of the old/new module paths,
stdlib and toolchain. Save retrieval time, DB modified time, scope and per-file
SHA256. A file:// mirror is supported by govulncheck. Missing required records,
failed retrieval or a stale index is an incomplete check, never zero findings.
M16 retained the full index and 278 relevant OSV records; two historical records
were added for newly selected dependency paths without changing the frozen index.

Run `grype --config OWN_CONFIG db update` once before scanning, record `db status
-o json`, the published archive checksum, local DB SHA256 and build timestamp.
Then disable auto-update, retain hash/age validation and require the normal 120h
freshness limit. Old and new artifacts must use the same DB. An expired snapshot
can be used to reproduce historical results, but cannot authorize a new release;
refresh separately and preserve previous reports. Do not disable age validation
and label the result current. Record command exit status, stderr and timestamps.

For every Linux/macOS/Windows × amd64/arm64 target, run the following standard
tool operations using absolute paths and the isolated environment described above:

```sh
# Execute source analysis in the immutable Go-only export, with matching compiler.
GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  govulncheck -db file:///ABS/FROZEN-GO-DB -json ./cmd/weir > source.govuln.json

govulncheck -db file:///ABS/FROZEN-GO-DB -json -mode binary \
  /ABS/ARTIFACTS/binaries/linux-arm64/weir > binary.govuln.json

syft scan file:/ABS/ARTIFACTS/binaries/linux-arm64/weir \
  --config /ABS/OWN-SYFT-CONFIG --source-name weir-linux-arm64 \
  --source-version FULL_SOURCE_SHA \
  -o cyclonedx-json@1.6=/ABS/REPORT/linux-arm64.cdx.json \
  -o syft-json=/ABS/REPORT/linux-arm64.syft.json

syft scan oci-archive:/ABS/ARTIFACTS/weir-linux.oci.tar --platform linux/arm64 \
  --config /ABS/OWN-SYFT-CONFIG --source-name weir-oci-linux-arm64 \
  --source-version FULL_SOURCE_SHA \
  -o cyclonedx-json@1.6=/ABS/REPORT/oci-linux-arm64.cdx.json \
  -o syft-json=/ABS/REPORT/oci-linux-arm64.syft.json

grype sbom:/ABS/REPORT/oci-linux-arm64.cdx.json \
  --config /ABS/OWN-GRYPE-CONFIG -o json > /ABS/REPORT/oci-linux-arm64.grype.json
```

Repeat OCI selection for amd64; do not infer the second architecture from one
scan. Verify each catalog's architecture/manifest against the packaging receipt
and each Go inventory against actual linked modules/build info, including stdlib.
Bind SBOM/report hashes to binary/archive hashes, source SHA, Go version and
manifest/config/layers in a separate receipt. Syft's main-module version may be
UNKNOWN for local builds; use the receipt and source-version for exact provenance,
not an invented semantic release version.

Catalog the full MVS graph separately by supplying Syft a derived isolated go.mod
containing every exact version from `module-graph.json`. Label this **module graph,
including tests/experiments and dependency-only modules**, not binary contents.
`govulncheck -mode query module@version ...` provides official OSV candidates for
that graph; it does not establish package or function reachability. Also run
`govulncheck -test ./experiments/...` separately. Experimental Lua is not product
runtime and ProgramTransform remains UNSUPPORTED.

Validate every CycloneDX 1.6 output using official
[CycloneDX 1.6 schemas](https://github.com/CycloneDX/specification/tree/1.6/schema),
including their spdx/jsf references, with a local-only schema registry.
M16 uses jsonschema 4.25.1 Draft7Validator and FormatChecker; installer receipt
pins downloaded validator dependencies. Preserve schema hashes and per-file
validation errors. Schema validity is structural, not proof of catalog completeness.

Exact local orchestration and command receipts are retained in
`.testdata/m16-evidence/{catalog.py,scan-go.py,scan-grype.py,validate-sbom.py}` and
`dist/m16-scans/{before,after}/`. These only call standard scanners/validators;
there is no custom vulnerability matching engine or handwritten SBOM.

[M17](milestone-17.md) reuses the verified tool binaries and still-fresh databases,
with its own empty HOME and explicit inputs. Its `dist/m17-scans/after/` contains
8 newly generated product catalogs and 12 target-specific Go scans. The unchanged
module graph/base are checked against M16; unchanged experimental dependencies
need not be reinvestigated. `.testdata/m17-evidence/` retains exact command receipts.
Official backend images are separately saved and scanned under `dist/m17-backends/`;
those packages are not contents of the Weir image. Preserve raw schema failures
as well as findings: M17's external ES catalog has two SPDX-enumeration errors,
while its 8 Weir catalogs validate. The coordinator independently reproduced
those two ES errors and accepted M17's limited functional/artifact qualification,
without accepting OpenSearch server security. Never repair raw reports by hiding fields.

[M18](milestone-18.md) retains all 78 OpenSearch High/Critical location matches
as 46 package/version groups and 28 advisories in the committed
[location and alias index](milestone-18-findings.json). It combines fixed upstream
source, actual JAR contents, fixture configuration and an opt-in classloader
snapshot. A loaded class is not an invoked vulnerable method; an absent class in
one observation is not proof that future inputs cannot load it. The current REST
pipeline excludes specific SNI/SPDY conditions, and the fixture's certificate/CRL
configuration excludes the two BC conditions there. JDK CVE-2026-47063's precise
API path and several optional plugin paths remain unproven; OpenSearch stays blocked.

The single official 3.8.0 arm64 candidate in `dist/m18-candidate/` improves JDK,
BC and Jackson versions but retains Netty/HttpCore candidates. It was inspected
offline, not adopted or run as a supported backend. Its scan counts are not an
exploit count or a reason to upgrade across a major version. Neither blank VEX
nor blanket suppression was added. CLI-only, per-plugin and separate RCA process
copies are distinguished; arm64 does not dispose of Intel-only entropy conditions
on an unqualified amd64 deployment. Any extra deployment restriction is proposed,
not silently part of the existing profile. Remaining upstream dependencies and
the finite requalification scope are in M18; no polling or custom backend fork
is created. Product artifact source remains M17's `1bb93fd`; this investigation
does not require rebuilding the unchanged six-target artifact set.

## Triage and release policy

For every candidate record, retain ID/aliases, package/version, affected range,
fixed version, scanner severity, source, observed scope and a bounded decision.
Distinguish version match, package linkage, symbol presence, source call trace and
confirmed runtime exposure. govulncheck JSON returns exit 0 even with findings;
binary mode lacks a call graph, and source mode has reflection/interface limits.
Never equate a successful command or an empty reachability result with universal
safety. Unknown severity, unsupported coverage and tool failures stay explicit.

Review actual transport/decoder/CA code when changing dependencies. Preserve
one-attempt mutation behavior, UNKNOWN classification, finite connection/buffer
ownership, cancellation, drain, TLS hostname/CA checks and OCSP behavior. gRPC's
WithDisableRetry does not disable all transparent transport retries; review the
exact upstream implementation and exercise ambiguous replies. Keep upstream
receive-buffer compaction enabled; do not set
`GRPC_GO_EXPERIMENTAL_ENABLE_RECEIVE_BUFFER_COMPACTION=false`.

High/critical findings with a reachable path, or uncertain exposure on a deployed
path, block release until patched and revalidated or independently accepted with
specific evidence. Package-absent findings can be recorded as not affected in this
binary, but are reevaluated when imports/build targets change. Do not introduce
blanket suppressions or upgrade an entire graph just to erase scanner counts.
External MongoDB/Elasticsearch/OpenSearch servers are separate products; checking
their official advisories/support policies is mandatory, and their version risks
remain even if the Weir image has no OS-package findings.

These artifacts are **unsigned local evidence**. Darwin arm64 linker ad-hoc code
signing is not an organizational identity or notarization. A production signing
policy still requires an authorized publisher/key or workload identity, protected
signing process, verifiable artifact + SBOM + provenance binding, key rotation and
revocation, and independent verification. No keys are created or read here; no
signing, registry publication, automatic update or CI deployment is authorized.
Refresh scans at a release candidate, after any source/dependency/compiler/base
change, and on relevant new advisories. Human security review and native/platform,
resource, topology, Kubernetes, capacity and soak gates remain separate.
