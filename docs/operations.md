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
Shutdown can require two cleanup phases after the graceful drain deadline. During
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
| `linux-native-load-v1`, `linux-native-fault-v1` | load or fault / formal or smoke | Declarable, **not implemented**; fail closed |

Legacy driver/CLI targets are Darwin arm64 and Linux arm64 respectively. MongoDB
8.0.32 and Elasticsearch 8.19.22 use the named owned plain Desktop fixtures. The
adapter independently checks the Mongo owner marker, actual PID/argv, version and
replica-set identity; it inspects a fresh full Elasticsearch container ID, owner,
pinned image and private bridge address each run. CLI cleanup requires the captured
full ID and owner to match; mismatch refuses stop/removal and retains failure
output. Desktop routes and proxy translation belong only to that named adapter.
Linux declarations use matching native Linux amd64 or arm64 driver/CLI targets,
`owned-local` endpoint roles, MongoDB 8.0.32 plus Elasticsearch 8.19.22 or OpenSearch
3.3.0, and explicit plain/TLS transport. Declaration is not executability: this PR
implements no Linux executor, backend resource controller, or TLS/OpenSearch load
or fault adapter. Unsupported combinations do not skip, fall back, or pass.

Version-1 `ProvenanceInputs` uses `schemaVersion`, `profileID`, separate `server`
and `harness` objects (each `root` and full `revision`), and separate `cli` and
`driver` objects (each `role`, absolute `path`, and lowercase `sha256`). Roles are
literally `cli` and `driver`. The CLI must be the actual static Linux Weir ELF;
the driver must be the actual running native Go test executable, not the CLI or
a byte-identical copy at a different inode. Build info, object machine and actual
file hashes are checked. Both source roots must be clean repository top levels
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
fail-closed dispatch, not runtime qualification. An optional
`WEIR_EXECUTION_CANDIDATE_EXAMPLE` points the candidate-byte contract at an existing
local bundle. That regression checks the actual complete `verifyCandidate` path,
including all source inputs in a read-only-used local checkout of the bundle's
declared source revision, not the potentially dirty developer worktree. It does
not run a CLI or produce `ValidatedInputs`, and it establishes neither origin
nor runtime proof. Real Linux/cgroup-v2 execution,
three-backend plain/TLS CI, formal measurements/pass receipts and same-byte stable
promotion are separate follow-on work. Pinned local Mongo/LinuxKit compatibility
blockers are not bypassed by changing versions or host settings.
