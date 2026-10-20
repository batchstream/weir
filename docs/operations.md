# Operations

## Health and deployment

Enable an independent diagnostics listener, usually on loopback:

```yaml
diagnostics:
  address: 127.0.0.1:7449
```

`GET /livez` reports a running diagnostics handler. `GET /readyz` reports that the
Node entered service and is not draining or failed. Neither endpoint tests backend
health, workload capacity or directory convergence. `GET /metrics` exports a
Node-owned Prometheus registry. Diagnostics accepts HTTP/1 GET requests without a
body or query, with independent progress deadlines.

```sh
weir probe live --address 127.0.0.1:7449
weir probe ready --address 127.0.0.1:7449 --timeout 750ms
weir version
```

Application and peer ports require isolated networking. Peers advertise reachable
addresses; Store groups use equivalent local definitions. A node can bootstrap
through any peer, and clients can resolve Stores through any application endpoint.
Kubernetes manifests and scaling guidance live under
[deploy/kubernetes](../deploy/kubernetes/scaling.md). Every Pod runs one Weir process.

## MongoDB discovery and failover

For automatic primary discovery, use the strict
[replica-set connection profile](configuration.md#store-settings): an explicit set
name with one to seven seeds and `directConnection` omitted or `false`. The driver
uses ordinary replica-set topology discovery; Weir does not implement elections,
secondary routing, SRV, mongos or load balancing. Direct mode intentionally stays
on one member and cannot provide automatic failover.

Seeds are bootstrap addresses, not a routing allowlist or global connection cap.
Every advertised member address must be reachable from Weir. With TLS, each
member's certificate must verify its actual hostname/IP; a seed's identity is not
reused for other members. `max_connecting` limits creation per member pool, while
monitors and pools can exist for multiple discovered members. Monitor aggregate
backend demand, connection ownership and cancellation together.

Keep database transition time separate from Weir's reaction: observe when the
external replica set can serve the required primary/majority operation, then
measure recovery of new operations through the same Weir process/client. A live
or ready diagnostics response alone does not prove backend service usability.
Caller deadlines still bound unavailable selection; do not increase them or
resubmit a possibly applied mutation to conceal a failed recovery.

Weir retains primary reads and majority-acknowledged typed writes, disables
automatic read/write replay, and does not promise transparent migration of Native
cursors, sessions or transactions. Preserve actual APPLIED/UNKNOWN/failure or
Native completion evidence. An ambiguous Lua commit may only confirm that same
commit; fresh Lua evaluation requires confirmed abort/conflict evidence. Native
remains opaque: the caller supplies command-level concerns and manages any
backend-specific cursor/session semantics.

## Metrics

Observe completed work and actual backend demand together with queue and memory
occupancy. A lower connection count caused by rejected traffic is not evidence of
improved capacity.

| Family | Interpretation |
| --- | --- |
| `weir_node_state`, `weir_node_ready` | Lifecycle and readiness |
| `weir_store_active_executions`, `weir_store_active_executions_limit` | Per-Store backend invocations; limit zero means unlimited |
| `weir_store_batch_max_operations`, `weir_store_batch_max_bytes`, `weir_store_batch_max_wait_seconds` | Resolved aggregation policy |
| `weir_store_records_total` | Final typed results: mutation effect (`applied`, `not_applied`, `not_started`, `unknown`, `invalid`) or read `success`/`failure` |
| `weir_store_record_failures_total` | Final typed read/mutate results carrying `Failure`, including `APPLIED` with `Failure` |
| `weir_store_native_completions_total` | Final Native forwarding terminal evidence (`not_started`, `response_complete`, `response_incomplete`, `invalid`), not backend business success, RPC status, or client receipt |
| `weir_store_rejections_total` | Preparation/admission denials, not completed records |
| Store queue and result metrics | Waiting work, dispatch and retained publication |
| `weir_execution_memory_*` | Shared reservations, ownership, ingress and capacity denials |
| `weir_memory_*` | Actual observations, capacity, overload latch and runtime policy |
| `weir_search_http_*` | Record/startup exchange attempts, elapsed time, framed records and bytes |

Execution memory reports `budget_bytes`, `used_bytes`, `resident_bytes`,
`input_reserved_bytes`, `input_used_bytes`, `wire_bytes`, `wire_buffers`,
`io_reserve_bytes` and lease states. `ingress_connection_bytes` is a conservative
workspace promise; `ingress_bytes` and `ingress_reserved_bytes` describe receive
ownership. These are reservation estimates, not RSS measurements. Compare them
with actual memory metrics and `budget_rejections_total`.

An admitted record's later working-memory growth may await a finite snapshot of
other Native invocations still in initial preparation, only within the actual
allocation/evaluation context's deadline. This is FIFO scheduling precedence over
fresh execution starts across the Node, not extra memory credit: allocations
remain forbidden until the complete reservation fits the unchanged ledger.
Native initial IO can continue growing, converging or retiring while growth waits.
Other records, settled slow outputs and future Native starts cannot extend that
snapshot. If its remaining speculative working reservations cannot make the growth
fit, or the snapshot finishes without releasing sufficient capacity, growth fails
rather than awaiting a consumer Ack. A missing deadline fails promptly under
pressure; cancellation removes the scheduling claim without releasing live data.

Typed record counters are updated once at Store completion, after final result
normalization (including result-budget failures). Result-budget normalization can
replace `Failure` with `RESOURCE_EXHAUSTED`, but preserves an already reported
legal mutation effect; missing or invalid effect evidence falls back to `UNKNOWN`,
not an assertion that the mutation was never started. `weir_store_records_total`
preserves mutation effect evidence: `outcome="applied"` does **not** mean business
success. For example, a query-visibility barrier can fail after application,
producing `APPLIED` with `Failure` even when backend HTTP returns 200 and the RPC
completes OK. This increments both `records_total{operation="mutate",outcome="applied"}`
and `record_failures_total{operation="mutate"}`. A successful document read or a
missing record without `Failure` does not increment the failure counter.

`weir_store_record_failures_total` has only the fixed `operation` label (`read`,
`mutate`), with both series initialized to zero. It counts admitted records that
terminate with `Failure`, including queued cancellations; it does not count
preparation/admission rejections, individual HTTP exchanges, Lua evaluations or
retries, RPC completion, publication acknowledgements, or scrapes. No failure
message, code, resource, ID, or backend text becomes a label. This failure counter
adds exactly two series per Store.

Both typed record counters exclude Native. Native is opaque forwarding: its
`NativeEnd` describes transfer completion/failure, not a portable backend business
result. In particular, a complete non-2xx HTTP response is not interpreted as a
generic business failure. These counters are not generic Native failure metrics.

`weir_store_native_completions_total` counts each admitted Native call once at the
same Store terminal point, after final result-budget normalization. Its only label
is the fixed `completion` vocabulary: `not_started` (`NATIVE_NOT_STARTED`),
`response_complete` (`RESPONSE_COMPLETE`), `response_incomplete`
(`RESPONSE_INCOMPLETE`), or `invalid` (unspecified, unknown or illegal enum values).
Exactly these four series are initialized to zero; no enum string, failure detail,
HTTP status, resource or identity becomes a label. Queued cancellations count as
`not_started`; preparation/admission rejections do not count as completions.
Result-budget degradation counts the final normalized terminal evidence, not the
adapter's original event. Publication acknowledgements and scrapes do not count
again.

This is **Native forwarding terminal evidence**, not backend business success,
RPC status or proof that the client received the response. An incomplete response
with `Failure` can be delivered in an RPC that finishes gRPC OK; it still increments
`completion="response_incomplete"`. A complete opaque HTTP 500 response increments
`completion="response_complete"`, without inferring business failure or updating
either typed record counter. Native completion adds exactly four series per Store:
61 runtime series in total, or 67 with Mongo connection metrics. The maximum
16-Store diagnostics fixture has 1,195 Weir series (excluding Go/process collectors).

Search HTTP metrics include failed attempts and exclude Native relay. HTTP
success does not prove every bulk item succeeded. Bulk elapsed time includes
`refresh=wait_for`; concurrent durations overlap and cannot be summed as wall time.
Framed request byte counts exclude headers and do not prove delivery. Lua evaluation
time excludes external JSON decoding/encoding. Metrics use bounded labels without
document or target identities.

## Memory pressure and recovery

Capacity is the smallest trusted physical host, finite process address-space or
visible container limit. Admission latches at the configured high watermark and
recovers below the low watermark; defaults are 80% and 70%. Recovery needs complete
trusted observations. Unknown samples alone do not manufacture overload.

On Linux cgroup v2, each finite visible level uses its own current/limit pair.
Validated reclaimable `inactive_file` is subtracted from `memory.current`; missing
or inconsistent statistics retain raw usage. Metrics expose both
`weir_memory_cgroup_current_bytes` and `weir_memory_cgroup_working_set_bytes`.
Cgroup usage is not added to process RSS.

The CLI owns an automatic Go soft limit of 80% of the remaining process recovery
allowance after memory outside RSS is accounted for. With default watermarks its
upper bound is 56% of capacity. Positive targets have a 16 MiB floor capped by
that upper bound. This encourages release of idle Go heap after allocation bursts.
It cannot reclaim a live working set or memory outside Go.

An explicitly present `GOMEMLIMIT`, including `off`, or an already finite runtime
limit takes precedence. A later external runtime-limit change ends automatic
ownership. Shutdown restores the previous setting only while Weir still owns it.
Embedded `app.Open` leaves global GC policy unchanged; process owners can explicitly
call `Node.ManageRuntimeMemoryLimit`.

Inspect `weir_memory_go_runtime_limit_bytes`, `weir_memory_go_runtime_managed`,
`weir_memory_recovery_allowance_bytes` and
`weir_memory_recovery_allowance_known`. Unknown or known-zero remaining allowance
preserves the previous soft limit while actual pressure keeps admission closed.
Use container/OS limits for the hard boundary.

## Slow consumers, failures and shutdown

Store queues bound waiting input. Stream publication windows and the shared memory
account bound retained output. A progressing Native stream can pause backend IO,
release an execution slot and fairly regain it before reading more. A stalled
connection can be closed by the transport watchdog; other RPCs on that connection
then fail too. Consume output promptly and set application deadlines.

A transport failure after a possible write leaves it UNKNOWN unless application
was already confirmed. Preserve indexed results received before failure. Do not
replay unconfirmed mutations automatically. Query-visible completion can fail
after application; APPLIED with Failure is not a successful visibility guarantee.

MongoDB transaction cleanup is bounded and best effort. An acknowledged abort or
`NoSuchTransaction` does not create a permanent session tombstone; delayed remote
work can retain locks until MongoDB's own transaction timeout and cleanup. A lost
commit acknowledgement remains UNKNOWN. Confirmed abort/conflict retries retain
the original caller lifetime.

Align termination grace with preStop, `lifecycle.shutdown_timeout` and cleanup.
Each Store preserves the full admitted drain (default `5s`), then owns one
absolute retirement budget (default `2s`), shared by concurrent Close callers.
Detached MongoDB cleanup keeps its ordinary caller-independent `2s` bound, but
forced adapter teardown cancels that cleanup and all operation/metadata waiters.
Remote cleanup gets at most the first three quarters of retirement; adapter close
gets the remaining context through seven eighths, reserving the final eighth for
actual execution, publication and close-worker joins. MongoDB stops admission and
closes/joins its owned raw transports before disconnecting the client. An expired
best-effort session cleanup is not confirmed remote cleanup or replay permission;
actual disconnect/raw-close errors and missed joins remain shutdown failures.
A noncooperative adapter returns a bounded failure, never a false successful stop.
Consumer-owned record evidence remains charged until genuine Ack/session closure.
These offline lifecycle bounds are not deployment or production qualification.
During
rolling updates, terminating Pods can retain connections while new Pods start;
`maxSurge: 0` does not cap that overlap. Scaling replicas increases aggregate
backend demand and does not add database capacity.

Validate the real deployment with persisted reads/writes, visibility requirements,
slow consumers, cancellation, memory bursts and rolling replacement. Unit tests,
tagged compilation and short functional probes do not establish sustainable
throughput or universal latency guarantees. Use the workload tooling in
[weir-tests](https://github.com/batchstream/weir-tests) and write run artifacts under
ignored `.testdata/`.

## Test execution profiles and provenance inputs

The integration-only production load and fault entrypoints retain their explicit
`WEIR_PRODUCTION_QUALIFICATION=1` and `WEIR_PRODUCTION_FAULT_QUALIFICATION=1`
opt-ins. Without opt-in they skip. With opt-in, `WEIR_EXECUTION_PROFILE` and
`WEIR_PROVENANCE_INPUTS` must name existing, absolute, non-symlink JSON files.
Missing, conflicting or unsupported inputs fail before fixtures, seed, proxies,
listeners or CLI creation. Old qualification environment variables, if present,
are consistency checks only; they never supply missing profile/provenance values.
Ordinary memory/Lua/ingress tests are not migrated to this interface.

`internal/testutil/testexecution` is test infrastructure, not a production command
dependency. Its version-1 `ExecutionProfile` has these exact JSON fields:
`schemaVersion`, `id`, `suite`, `intent`, `policy`, `executor`, `driver`, `cli`,
`backends`. Targets contain `os` and `arch`; backend entries contain `product`,
`version`, `transport`, and `endpoints` with distinct `driver`/`cli` role names,
not caller-selected remote addresses. Policy is `production-v1`; SLOs and quotas
are code constants, not JSON parameters. `LegacyProfile(id)` supplies the complete
immutable legacy specification for tooling to serialize:

| Profile ID | Suite / intent | Execution status |
| --- | --- | --- |
| `legacy-desktop-load-v1` | load / legacy; 30s warmup + 600s measurement | Desktop reproduction only |
| `legacy-desktop-smoke-v1` | load / smoke; 30s warmup + 120s measurement | Desktop smoke only |
| `legacy-desktop-fault-v1` | fault / legacy | Desktop fault reproduction only |
| `linux-native-load-v1` | load / formal | Implemented only on native Ubuntu 24.04 amd64, MongoDB 8.0.32 + Elasticsearch 8.19.22, both plain; other cells fail closed |
| `linux-native-fault-v1` | fault / formal | Implemented only on native Ubuntu 24.04 amd64, MongoDB 8.0.32 + Elasticsearch 8.19.22/plain; smoke and other cells fail closed |
| `linux-native-es-functional-v1`, `linux-native-opensearch-functional-v1` | functional / finite; at most 16 documents, 64KiB/document, 60s driver | Owned native Linux ES8.19.22/plain or OpenSearch3.3.0/plain Weir correctness only; not smoke/formal |

Legacy driver/CLI targets are Darwin arm64 and Linux arm64 respectively. MongoDB
8.0.32 and Elasticsearch 8.19.22 use the named owned plain Desktop fixtures. The
adapter independently checks the Mongo owner marker, actual PID/argv, version and
replica-set identity; it inspects a fresh full Elasticsearch container ID, owner,
pinned image and private bridge address each run. CLI cleanup requires the captured
full ID and owner to match; mismatch refuses stop/removal and retains failure
output. Desktop routes and proxy translation belong only to that named adapter.
Formal/smoke Linux declarations use matching native Linux amd64 or arm64 driver/CLI
targets, `owned-local` endpoint roles, MongoDB 8.0.32 plus Elasticsearch 8.19.22 or
OpenSearch 3.3.0, and explicit plain/TLS transport. Only the formal amd64
MongoDB+Elasticsearch/plain load and fault cells are implemented. Smoke,
TLS, OpenSearch load/fault and arm64 load/fault remain unexecutable; the distinct finite
ES/plain and OpenSearch/plain capabilities are unchanged. Unsupported combinations
do not skip, fall back, or pass.

Version-1 `ProvenanceInputs` uses `schemaVersion`, `profileID`, separate `server`
and `harness` objects (each `root` and full `revision`), and separate `cli` and
`driver` objects (each `role`, absolute `path`, and lowercase `sha256`). Roles are
literally `cli` and `driver`. The CLI must be the actual static Linux Weir ELF;
the driver must be the actual running native Go test executable, not the CLI or
a byte-identical copy at a different inode. Build info, object machine and actual
file hashes are checked. Driver bytes and hashes come from one bounded, pinned
file descriptor, with kernel-loaded-image identity checked before and after the
read; replacing an executable pathname cannot substitute another inode or SHA256.
Linux amd64/arm64 opens `/proc/self/exe` and compares its kernel executable object
to that descriptor. Darwin arm64 queries the public libproc
`PROC_PIDREGIONPATHINFO` for a function address in the running Go image and compares
the executable VM mapping's vnode identity with descriptor `fstat`. Neither method
trusts `os.Executable()` pathname stats or an init-time pathname snapshot. Renaming
the actual backing file remains valid when its new path names the same inode;
a missing/unlinked supplied path is not silently repaired. These checks bind the
executable backing file, not a reconstruction of all resident process memory.

The Darwin bridge reuses the already-pinned purego dependency and works with
CGO disabled. Its public SDK record sizes/offsets are checked before native calls;
short/failed observations, unavailable procfs/libproc, and unsupported platforms
fail closed. Native driver validation is implemented only for Linux amd64/arm64
and Darwin arm64. Driver validation is separate from execution capability. The test-only package has a Darwin dependency on the existing
purego module; Linux's implementation uses only the standard library. No module
pin/replacement or production command dependency changes are involved.

Both source roots must be clean repository top levels
at their declared revisions; the consumer also binds the harness root to its
current module. Compile a driver first with `go test -c -tags=integration`, record
that executable's hash, then supply it as the driver when invoking that test
binary. Keep inputs/artifacts in ignored `.testdata/` so they do not dirty sources.

Optional `candidate` contains `directory`, `deliverySHA256` and `receiptSHA256`.
It references an already-present retained distribution bundle: `delivery.json`,
`SHA256SUMS`, and the fixed `weir-first/` receipt, graph, four linked metadata files
and four archives. Validation is read-only, bounds reads, checks exact file sets,
source-input hashes and delivery/receipt/archive/member byte mappings, and rejects
links, duplicate members, path escapes, unsupported tar extensions and trailing
payloads without extracting archives. Sensitive source paths remain forbidden.
The sole source-name exception is tracked non-test public `.go` members in the
known patched gRPC module's `credentials/`, `internal/credentials/` and
`experimental/credentials/` packages. Before any such member is read, its
receipt hash must match the upstream/patch member manifest; the module, version,
upstream checksum, patch identities and metadata bytes must match the declared
Git source snapshot. Secret-style descendants, unlisted members and symlinks
remain forbidden, and the generic file reader has no credentials-directory
exception. The selected archive's CLI must match the
actual CLI bytes. No candidate revision, workflow run or OCI index is a permanent
allowlist. OCI index, platform manifest and image config/layer identities remain
separate **receipt claims**: the retained bundle has no OCI runtime/layer bytes.

`ValidatedInputs` has private state, cannot be JSON-decoded, and is returned only
by validation. `passed`, `verified` and `formalEligible` inputs are not accepted.
CLI `version` text is only a cross-check: linker flags can forge source strings.
Clean roots, hashes and local metadata demonstrate consistency, **not independent
build/workflow origin**. No result/qualification receipt or stable-promotion
implementation is included here. Even a completed legacy 600s run logs
`qualification=false`; 120s smoke cannot become formal evidence.

Existing gates remain 2 CPU/1GiB for the load CLI and 2 CPU/512MiB for fault,
memory=swap, 96 PIDs, nonroot/read-only/cap-drop, with driver/proxies outside the
CLI quota. Workload remains 200 RPC/s, 32 eight-MiB slow consumers and 256 burst;
p99 Read <=100ms, ordinary <=250ms, Lua/four-record batch <=500ms, max <=3s;
final owners/gauges zero and memory recovery <=5s. Fault healthy/cancel <=3s,
recovery <=5s and SIGTERM <=8s, no normal-load budget rejection, independent
mutation oracle, UNKNOWN and no-replay assertions remain unchanged.

The precise backend-free contract selector is:

```sh
go test -tags=integration -count=1 -timeout=120s -run '^TestExecutionProfileContract' ./internal/testutil/testexecution ./internal/app
```

These tests verify decoding, local byte/source consistency, preserved policy and
fail-closed dispatch, not runtime qualification. An isolated owned-child contract
requires native driver validation to succeed, rejects same-byte/new-inode and
different-byte/claimed-SHA pathname replacements, and checks rename, restore and
unlink behavior without starting a service. It runs under the same selector on
Linux CI and Darwin arm64; an unsupported observation is a failure, not a skipped
positive contract. An optional
`WEIR_EXECUTION_CANDIDATE_EXAMPLE` points the candidate-byte contract at an existing
local bundle. That regression checks the actual complete `verifyCandidate` path,
including all source inputs in a read-only-used local checkout of the bundle's
declared source revision, not the potentially dirty developer worktree. It does
not run a CLI or produce `ValidatedInputs`, and it establishes neither origin
nor runtime proof. Three-backend plain/TLS CI, formal measurements/pass receipts
and same-byte stable promotion are separate follow-on work. Pinned local
Mongo/LinuxKit compatibility blockers are not bypassed by changing versions or
host settings.

### Formal native Linux plain fault

The independent `native-linux-formal-fault` matrix job executes
`scripts/test-production-linux.py --profile=linux-native-fault-v1` with the same
explicit inputs, pinned dependencies and disposable Ubuntu 24.04 amd64 boundary
as load. TLS, OpenSearch, smoke and arm64 fault fail closed. This is Weir fault
execution against external dependencies, not database/kernel/network tuning or
Mongo replica-set failover qualification.

`TestProductionLinuxFaultPreflight` validates sources and actual native binaries
before fixtures; `TestProductionLinuxFaultMongo` initializes and verifies the owned
native Mongo dependency. The fault driver owns loopback Mongo wire and ES HTTP
proxies outside the CLI quota. A fresh controller-directory handshake binds exact
owner/driver identities and driver-owned listening socket inodes before publishing
CLI routes. The single-member Mongo fault route explicitly uses direct connection
and polling monitoring so hello cannot bypass the proxy; the actual upstream
replica-set/member/version was independently verified first. This is not a
replica-set discovery or failover cell. The controller alone creates, observes, signals, waits and removes the captured
full-ID CLI container. Actual controls are 2 CPU, 512MiB, zero swap, 96 PIDs,
nonroot/read-only/cap-drop/no-new-privileges; driver, proxies and databases remain
outside that cgroup. PID/start ticks and executable/cgroup identities must remain
unchanged through recovery and before shutdown.

The native driver reuses the existing production fault cases for **both** backends:
healthy Read/Native, lost native and typed acknowledgements, cancellation after
ack, held IO with healthy concurrent calls, and proxy unavailability/recovery.
Existing <=3s healthy/cancellation/credit-return, <=5s same-SDK recovery, independent
durable mutation effects, typed UNKNOWN, native INCOMPLETE and no-replay gates
remain. The final acknowledged mutation stays held while the controller delivers
real SIGTERM; the driver releases the proxy only after signal confirmation. The
controller checks actual wait/exit 0, no OOM, and <=8s from the absolute signal
operation boundary; the driver also bounds its whole shutdown handshake to 8s.
Default drain 5s and retirement 2s are not overridden. Proxy/socket and SDK joins,
owned database/index drops, and independent shutdown oracle precede structured
completion. The controller then verifies native dependency retirement, container/
network absence and marker/inode-guarded data removal using the same bounded
storage ownership path as load.

Fault has a 360s controller deadline, 210s driver envelope, separate 60s cleanup,
and 8-minute outer CI command envelope. Load keeps its independent 30s+600s workload
and 18-minute command envelope; finite execution is unchanged. Each matrix cell
retains its own bounded observations, never dependency data or generated Git
records. `fault_complete` requires all actual gates and cleanup; records retain
`qualification=false`. Offline tests and Linux cross-compilation do **not** claim
that this native cell ran or passed.

### Formal native Linux plain load

`scripts/test-production-linux.py --profile=linux-native-load-v1` now implements
one exact formal execution cell on a disposable GitHub-hosted CI runner: Ubuntu
24.04 amd64, stock MongoDB 8.0.32 and Elasticsearch 8.19.22, both plain. The
controller rejects self-hosted/non-CI environments before fixtures; these environment
checks prevent accidental use, not prove workflow origin. Supply the same canonical source/binary/observer
arguments as the finite controller plus `--mongod` naming the prepared stock
native binary. `scripts/ci-tools.py test --mongo` prepares that dependency in the
separate CI preparation step; execution neither downloads nor changes the host.
The native binary's exact 222890240-byte size and SHA256
`15c7caf565edeaca228c76fbe6c4503b1cff7ae6517f344cc555552423b3209a` are checked
before fixtures. The controller owns a fresh stock, loopback-only Mongo process,
directory and single-member replica set; the driver verifies actual version,
primary/set identity and every advertised member address. This is an external
Weir test dependency, not database productionization or failover qualification.

The CLI alone uses explicit host networking **only on this disposable owned
native Linux CI fixture**, with application and diagnostics bound to ephemeral
IPv4 loopback ports. It reaches the actual advertised Mongo loopback member and
the owned ES loopback publication; there is no seed-only address substitution.
No shared/EKS network, firewall, kernel or daemon change is made. Host networking
is not network isolation. Inspection checks actual host mode, absence of port
publications, full owned container/image/PID identity and unchanged nonroot,
read-only, cap-drop, no-new-privileges controls. The same observer validates actual
2CPU/1GiB, zero swap and 96-PID cgroup controls before/after the workload. The
native Mongo process and driver remain outside that CLI cgroup. Mongo uses stock
settings; ES retains its existing startup/heap/resource settings, but load data
uses a new owned disposable host directory rather than finite-only 256MiB tmpfs.
Admission requires 8GiB free host/Docker storage. During load, the controller
checks owned dependency data between bounded two-second driver waits, aborting above
2GiB per dependency or below 2GiB free storage (native Mongo logs: 64MiB).
Traversal failures, unexpected links or a changed owner/path/inode fail closed.
These are monitored abort thresholds, **not hard filesystem quotas**; the workload
itself remains the fixed finite arrival/document/payload schedule. Load-only
setgid/group-writable data permissions and startup umask allow the runner to
remove the ES dependency's files without privilege or database tuning.
Database data and native process logs are not uploaded. After verified container/
network absence and native process join, cleanup removes only marker-verified
owned data directories; logs and the owner marker remain. If cleanup cannot
remove data, it reports retained paths and last observed bytes, fails execution,
and does not claim full cleanup. No shared directory or cache is removed.

`TestProductionLinuxLoadPreflight` completes source/binary/actual-loaded-driver
checks before fixture creation. `TestProductionLinuxLoadMongo` initializes the
owned dependency; `TestProductionLinuxLoad` feeds verified endpoints into the
**same** existing workload, sampler and independent oracles. The original load
entrypoint dispatches this exact native cell only with controller opt-in.
There are no workload tuning flags: 30s warmup + 600s measurement at 200 RPC/s,
all default Store policy checks, 32 eight-MiB slow consumers, 256 burst, original
latency maxima/p99s, no normal-load execution-budget rejection, final ownership
and five-second memory recovery remain unchanged. Structured load completion is
written only after SDK/index/database cleanup; the controller rejects missing,
skipped, premature (<630s), wrong-policy or identity-mismatched completion.

The dedicated `native-linux-formal-load` CI job has a 25-minute job budget.
Controller execution is bounded by 960s, including preflight, startup, a load-driver
budget of 800s (630s workload plus seed/oracle/recovery), and observation/shutdown;
cleanup has its own 60s bound. CI's 18-minute command envelope leaves cleanup time.
Existing finite job/controller budgets are unchanged. A `load_complete` result
requires all driver gates, cgroup observations, shutdown and owned cleanup to
succeed. Results still say `qualification=false`: they are execution observations,
not independent build-origin or stable-promotion receipts. Offline mocks, race
tests and cross-compilation cannot establish a real formal run pass. TLS,
OpenSearch load/fault, arm64 load/fault, matched performance, EKS and final same-byte
candidate promotion remain separate deliverables; no final server lock is advanced.

### Finite native Linux executor

`scripts/test-production-linux.py` runs the actual compiled native `app.test` on
the host and owns a fresh standard Docker bridge, selected search dependency and CLI. It takes
explicit `--server-root`, `--harness-root`, `--revision`, `--cli`, `--driver`,
`--observer`, an exact `--profile=linux-native-es-functional-v1` or
`--profile=linux-native-opensearch-functional-v1`, and a **new** `--output` directory.
All paths are canonical and absolute.
Keep binaries/output in ignored `.testdata/`; prepare their parent directories
before invocation. Supply an explicitly clean-source CLI build with
`-ldflags="-buildid= -X main.sourceRevision=$(git rev-parse HEAD)"`, not an ordinary
dev binary; compile `app.test` with `go test -c -tags=integration` and the observer
from `./internal/testutil/testresource`, all with CGO disabled. The preflight test
uses the merged strict source, CLI, version and kernel-loaded-driver validation,
then emits structured completion **before** any network/backend/CLI creation.
The finite test repeats validation, executes real SDK Resolve, typed CRUD,
Native, Lua and Batch4, independently checks all persisted effects and counts,
closes the SDK and requires final ownership recovery. There is no business replay.
An absent selected test, any skip, missing case/sample/record, or exit 0 alone
cannot pass.
The controller sets `WEIR_FINITE_NATIVE=1`; ordinary unselected integration runs
retain an opt-in skip, which cannot satisfy the controller's completion contract.

Execution supports same-architecture Linux amd64/arm64 only. Darwin, Desktop,
remote/custom Docker endpoints, contexts, emulation, wrong ELF targets, missing
prepared prerequisites, cgroup-v1 and insufficient resources fail before service
changes. Require a local `/var/run/docker.sock`, at least two CPUs, 4GiB currently
available host memory and 2GiB free host/Docker storage. No install, build, pull,
Kubernetes action, host sysctl/cgroup write, or daemon configuration change occurs
inside the executor. The separate CI preparation step pulls only immutable public
images with a new empty Docker configuration. Execution always uses `pull=never`.
Runtime base pins come from `deploy/docker/base.json`, not ARM-only BusyBox.
The server ES pin is the arm64 child `c2a3ed5f…`; public 8.19.22 index
`e98f9c3b…` also names amd64 child `f1e3d882…`. The controller records and verifies
the exact selected native child and local image config identity; the companion
repository's different pin is not substituted.

Only exact IPv4 `127.0.0.1` application, diagnostics and backend-oracle ports are
published; missing, wildcard, extra or wrong bindings fail execution. CLI backend
routing stays inside the uniquely owned network. The network must report
`Driver=bridge` and `Internal=false`: an internal-only bridge did not program the
required host publications on the supported CI runner. This standard bridge does
**not provide egress isolation**. The executor creates/removes only its own network
and does not change shared networks, host firewall/CNI rules, sysctls or daemon
configuration. Cleanup rechecks the same network mode and requires an empty network
after verified container absence. CLI controls are 2CPU/1GiB,
memory=swap, 96 PIDs, nonroot/read-only/cap-drop/no-new-privileges. Backend controls
are 2CPU/1536MiB, memory=swap, 512 PIDs, 512MiB heap, normal backend settings, and
bounded disposable tmpfs data (256MiB), logs (32MiB), temporary storage (128MiB)
and rotated Docker logs (2MiB). The ES dependency copies the complete immutable image
`config/.` tree, including dotfiles, without preserving ownership into a newly
created private `/tmp/weir-es-config` directory. This shares the existing 128MiB
`/tmp` tmpfs; it adds no writable root or storage/memory allowance. The static
startup command exports `ES_PATH_CONF` to that same staged directory, then execs
the stock `docker-entrypoint.sh eswrapper`. The pinned image's `/bin/tini` remains
PID1, and normal startup/keystore checks run unchanged. Runtime inspection requires
the exact staging command, stock entrypoint and original finite tmpfs settings;
configuration is not substituted with a partial hand-written file or a host bind.
The OpenSearch dependency instead uses its pinned stock image/entrypoint with a
normal disposable writable container layer, not a held config volume or helper.
Its data/log/tmp tmpfs and resource limits have the same 256/32/128MiB and
2CPU/1536MiB/512-PID bounds; ordinary executable `/tmp` supports stock startup.
It remains nonroot/cap-drop/no-new-privileges. This is an external test dependency,
not a database deployment recommendation, security claim or production gate.
Only identity/readiness, bounded fixture controls and owned cleanup are required;
there are no PA/RCA, JDK/JVM-byte, heap/native, bootstrap or seccomp proofs.
Database warnings do not qualify or disqualify Weir, and database TERM exit values
are recorded rather than required to equal the Weir CLI's exit 0.
No container environment enumeration or secret input is used. An exited owned
backend fails readiness immediately, retaining logs and guarded cleanup instead
of waiting for the full HTTP deadline. Driver/probes/proxies are not placed in the
CLI workload quota, except that the brief **in-container observer exec** itself is
charged to the observed container: these are not formal workload-only accounting
samples.

For the CLI and existing ES path, the known read-only mounted observer runs through the captured full owned
container ID with private cgroup namespace. Bounded `--once --identity-only`
establishes PID1 executable path/SHA/start identity inside that namespace (a
nonroot host runner cannot read another UID's executable). A second controls-only
`--once --expected-sha256=… --pid=1` validates that byte identity, matching target/
observer namespace membership and read-only unified-root mount mapping **before**
reading cpu.max, memory.max, memory.swap.max, pids.max, throttle and memory/OOM
counters. Host PID/start/membership are independently matched to the full container
ID; Docker configuration alone or a host PID alone is not accepted as quota proof.
The existing long-running sampler is preserved. Actual driver/CLI bytes and
process identities are checked again during the finite run. CLI SIGTERM/wait is
bounded by 8s, exit must be 0 and OOM false. Execution/startup/driver bounds are
240s overall, 90s backend readiness, 20s CLI readiness and at most60s driver;
cleanup has a separate 60s bound. Readiness uses one absolute deadline, including
backend inspection, HTTP and retry waits; command and HTTP timeouts use only the
positive remaining budget. Deadline checks after inspection, response open/read
and before acceptance reject late success. Standard-library socket timeouts are
per blocking operation, not hard wall-clock cancellation of an in-progress read.
The executor does not treat a late response as timely readiness. The driver also
uses one absolute phase/overall deadline, rechecking after process snapshots,
wait, log reads and completion parsing. Late exit-0/completion is rejected, with
owned process-group kill/reap and independently bounded cleanup on failure.

Cleanup guards are registered before creates, recover partial creates by unique
name plus owner, and subsequently use full IDs. Every destructive action rechecks
ID and owner; log failures do not suppress cleanup, primary and cleanup failures
are both retained, inspect uncertainty is not absence, and both ID/name absence
must be verified. No shared prune, image/cache deletion or forced live removal.
Sanitized selected metadata, bounded logs, digests, structured completion,
controls, shutdown and cleanup evidence are retained under the owned output root.
They are finite correctness evidence, **not a formal/pass-promotion receipt or
an SLO claim**.

The independent `native-linux-finite` and `native-linux-opensearch-finite` CI jobs
check out the exact PR head (push SHA
otherwise), prepares pinned Go/modules, builds those exact clean-source binaries,
explicitly prepares images, and executes the real path on Ubuntu amd64. Offline
Python and Go contracts remain a separate gate and cannot substitute for this
live job. A passing native CI run is required before merge; Linux arm64 remains
unexecuted until a native arm64 result exists. These test Weir against external
search dependencies only; they do not enable formal load/fault, Mongo, TLS or EKS.
The separate exact formal plain-load cell is described above.
Generated logs/results, identities and cleanup receipts stay in ignored output
or CI artifacts, never in tracked documentation or source.

### Held tmpfs config-copy prerequisite

`scripts/test-production-volume-linux.py` is a separate native Linux filesystem
probe, **not an executable OpenSearch production/functional profile**. The
`native-linux-volume-copy` CI job checks out the exact PR head without persisted
credentials, builds the stdlib-only `internal/testutil/testvolume` helper with
native Go1.27.1 from clean source, records its SHA256/build information/source,
and explicitly prepares public pinned native images in an empty Docker config.
Because trimmed Go build information omits linker flags, a bounded pre-mutation
helper identity command also verifies the embedded revision and loaded SHA256.
Execution requires those prepared images and uses `--pull=never`; it never
pulls, builds, installs, starts the stock OpenSearch process, or reads Docker/user
authentication configuration. Darwin/Desktop, remote/context/TLS Docker,
emulation, non-unified cgroups and unsupported hosts fail closed, not skip/PASS.
No existing ES controller, profile, three-resource lifecycle or budget changes.

This proves a specific storage prerequisite: a local Docker tmpfs volume may
lose its copy-up contents on its last unmount between target create and start.
A known-helper **read-only mount holder must already be running and verified
before target CREATE**. Its otherwise-empty `/held` mount remains alive while
Docker copies the pinned image's config into the same volume at
`/usr/share/opensearch/config`. The holder writes nothing, including no ready
file that could suppress copy-up. After target CREATE and before target START,
a bounded read-only holder observer hashes the selected public files; the target
must report those identical hashes after START and its owned writes. This proves
held create-time data, rather than inferring copy-up timing from exit 0 or logs.
The probe replaces the image entrypoint only
with the known read-only-mounted filesystem helper. Both consumers use nonroot
1000:1000, explicit working directory `/`, read-only roots, private cgroups,
dropped capabilities,
no-new-privileges, network=none, IPC=none (no writable shared-memory mount)
and no ports. The volume is their only writable filesystem for probe operations.
The holder has 0.1CPU/64MiB/memory=swap/32PIDs
and a maximum 300s TTL; the target has 0.5CPU/128MiB/memory=swap/32PIDs and a
finite 60s controller phase (55s helper TTL). The controller is bounded to 240s,
with a separate 60s cleanup budget; positive remaining budgets have no floor.
These are process/command budgets, not a claim of hard wallclock cancellation
of arbitrary in-progress stdlib I/O.

The single 16MiB local volume has exact `device=tmpfs`, `type=tmpfs` options
`rw,noexec,nosuid,nodev,size=16777216,uid=1000,gid=1000` and local scope. Docker
volumes have **no immutable 64hex ID**: identity is the exact
Name/CreatedAt/Driver/Options/Labels/localScope tuple, including the fresh owner
label. Same-name recreation, changed tuples and unknown inspection results are
not absence. No host traversal of the daemon volume Mountpoint is performed.
The helper checks actual tmpfs magic, capacity and restrictive mount flags,
actual loaded PID1 bytes/start identity and actual private-cgroup CPU,
memory/swap/PID controls. The controller binds these observations to the owned
full container IDs and host PID1 membership/start identity. Holder and target
must report the same filesystem device, without confusing observer/self
cgroups with target controls.

Only `opensearch.yml`, `jvm.options` and `log4j2.properties` are read: each must
be a nonempty, bounded, regular non-symlink file; evidence records size/SHA256,
never contents. The probe writes/reads/renames/removes exclusively-created
run-owned files, fills only a new run-owned file in at most 273 attempts of
64KiB each to ENOSPC within 16MiB, cleans it up, then rechecks the public input
hashes.
No stock config file is modified/deleted; no sensitive config/PEM/KEY/credential
files are read. Exit 0, metadata, an empty directory, logs alone, skipped tests,
late/duplicate/missing or identity-mismatched completion are not success.
Structured helper effects, runtime observations, controlled shutdown and exact
cleanup are all necessary. Offline mocks explicitly discard tmpfs contents on
the last unmount and are **not real backend or filesystem runtime evidence**.

Cleanup is reverse order: target, holder, volume. Every container mutation is
guarded by fullID/name/owner; every volume mutation by unchanged tuple/owner.
Both consumers must be absent by fullID and name, and **all containers including
stopped consumers** must show no attachments before volume removal. Foreign
consumers are never stopped/removed. There is no force, prune, shared cleanup,
manual mount/unmount or host control change. Unknown inspection, changed
identity, timeout, permission, logs/removal failure or remaining resources
retain primary plus cleanup errors. Normal TERM/wait must finish within 8s with
exit 0; TTL expiry is a bounded failure, never cleanup-success evidence.

The target is OpenSearch 3.3.0, repository `opensearchproject/opensearch`, index
`sha256:d96afaf6cbd2a6a3695aeb2f1d48c9a16ad5c8918eb849e5cbf43475f0f8e146`,
native linux/amd64 child
`sha256:59e9e49ac00f19c2af9094e367c8e6586f7d07e6c06465c3f4c43b40934309f2`
and native linux/arm64 child
`sha256:98db5375d94a62ee3d9073681bb40fe1087657d036f3dfa79053b0b1f83f5577`.
The holder preserves `deploy/docker/base.json` pins. Public image metadata
identifies `./opensearch-docker-entrypoint.sh`, CMD `opensearch`, USER 1000 and
WORKDIR `/usr/share/opensearch`; this is not verification of pinned-image script
bytes. This probe intentionally does not test that entrypoint, keystore,
performance-analyzer, Java, Weir SDK, TLS or backend startup. The separate finite
OpenSearch executor described above derives its product from validated inputs,
but does not depend on this filesystem probe or its config volume. This probe
retains its own 16MiB config/112MiB temporary split unchanged; the finite external
dependency uses ordinary disposable writable storage instead. Existing ES
budgets remain unchanged.

A passing new-head **actual native CI run** is required before this prerequisite
can be accepted. The job retains bounded sanitized source/build/image/engine,
container, mount, process, effects, shutdown and exact absence evidence even on
failure. Only actual amd64 CI is evidence here; cross-compilation is not ARM
runtime evidence. This does not admit OpenSearch SDK integration, production,
load/fault/formal, Mongo replica-set failover, TLS, EKS or promotion qualification.
