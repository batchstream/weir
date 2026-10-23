# Operations

## Health and deployment

Enable an independent diagnostics listener, usually on loopback:

```yaml
diagnostics:
  address: 127.0.0.1:7449
```

`GET /livez` reports a running diagnostics handler. `GET /readyz` reports that
the Node entered service and is not draining or failed. Neither endpoint tests
backend health or workload capacity. `GET /metrics`
exports a Node-owned Prometheus registry. Diagnostics accepts HTTP/1 GET
requests without a body or query, with independent progress deadlines.

```sh
weir probe live --address 127.0.0.1:7449
weir probe ready --address 127.0.0.1:7449 --timeout 750ms
weir version
```

The application port requires isolated networking. Every replica behind one
address uses the same Store definitions. In Kubernetes, put the replicas behind a
headless Service so clients connect to every Pod; the
[weir-charts](https://github.com/batchstream/weir-charts) Helm chart is the
deployment reference. Every Pod runs one Weir process.

## MongoDB discovery and failover

For automatic primary discovery, use the strict [replica-set connection
profile](configuration.md#store-settings): an explicit set name with one to
seven seeds and `directConnection` omitted or `false`. The driver uses ordinary
replica-set topology discovery and follows MongoDB's own elections; secondary
routing, SRV, mongos and load balancing are unsupported. Direct mode stays on
one member and cannot provide automatic failover.

Seeds are bootstrap addresses, not a routing allowlist or global connection cap.
Every advertised member address must be reachable from Weir. With TLS, each
member's certificate must verify its actual hostname/IP; a seed's identity is
not reused for other members. `max_connecting` limits creation per member pool,
while monitors and pools can exist for multiple discovered members. Monitor
aggregate backend demand, connection ownership and cancellation together.

Keep database transition time separate from Weir's reaction: observe when the
external replica set can serve the required primary/majority operation, then
measure recovery of new operations through the same Weir process/client. A live
or ready diagnostics response alone does not prove backend service usability.
Caller deadlines still bound unavailable selection; do not increase them or
resubmit a possibly applied mutation to conceal a failed recovery.

Weir retains primary reads and majority-acknowledged typed writes, disables
automatic read/write replay, and does not promise transparent migration of
Native cursors, sessions or transactions. Preserve actual
APPLIED/UNKNOWN/failure or Native completion evidence. An ambiguous Lua commit
may only confirm that same commit; fresh Lua evaluation requires confirmed
abort/conflict evidence. Native remains opaque: the caller supplies
command-level concerns and manages any backend-specific cursor/session
semantics.

## Bounded CPU investigation

CPU profiling is disabled by default and exposes no HTTP pprof endpoint. For a
matched performance investigation, start the actual CLI with both flags:

```sh
weir serve --config weir.yaml --routes routes.yaml \
  --cpu-profile .testdata/weir-cpu.pprof --cpu-profile-duration 30s
go tool pprof -top .testdata/weir-cpu.pprof
```

Prepare the private output directory first. The output must be a new file;
creation is exclusive with mode `0600`, and existing files (including symlinks)
are never overwritten. Both flags are required together; duration must be
positive and at most `60s`. Capture begins before node assembly, not after
warmup. The timer stops and flushes the runtime profiler and closes the file
without stopping service; serve exit also stops capture and joins its worker
without waiting out the capture duration. Profiler flush and file close add
cleanup latency outside the node drain budgets. Abrupt termination can leave an
incomplete file. Use a local regular-file destination on trusted storage;
filesystem IO and runtime flushing are not hard wall-clock bounded. File-close
errors fail serve; the Go profiler API does not report asynchronous write
errors, so verify that the retained profile parses. Treat profiles as sensitive
process information.

Retain generated output only in ignored `.testdata/` or external artifacts.
Match binary, workload, resource limits and observation windows across
comparisons, and account for profiling overhead and startup samples.

## Metrics

Observe completed work and actual backend demand together with queue and memory
occupancy. A lower connection count caused by rejected traffic is not evidence
of improved capacity.

| Family | Interpretation |
| --- | --- |
| `weir_node_state`, `weir_node_ready` | Lifecycle and readiness |
| `weir_store_active_executions`, `weir_store_active_executions_limit` | Per-Store record batches; limit zero means unlimited |
| `weir_store_active_natives`, `weir_store_active_natives_limit` | Per-Store running Native commands |
| `weir_store_queued_operations`, `weir_store_queued_bytes` | Waiting work |
| `weir_store_draining`, `weir_store_overloaded` | Store admission state |
| `weir_store_records_total` | Final typed results: mutation effect (`applied`, `not_applied`, `not_started`, `unknown`) or read `success`/`failure` |
| `weir_store_record_failures_total` | Final typed read/mutate results carrying `Failure`, including `APPLIED` with `Failure` |
| `weir_store_native_completions_total` | Final Native forwarding terminal evidence (`not_started`, `response_complete`, `response_incomplete`), not backend business success, RPC status, or client receipt |
| `weir_store_rejections_total` | Preparation, draining and overload denials, not completed records |
| `weir_store_executions_total`, `weir_store_batch_operations`, `weir_store_queue_wait_seconds`, `weir_store_execution_seconds` | Adapter invocations, batch sizes, queue wait and execution time |
| `weir_ingress_sessions`, `weir_admission_rejections_total` | Active RPCs and RPCs refused at admission |
| `weir_watchdog_expirations_total` | RPCs ended by the input or output stall timeout |
| `weir_memory_*` | Actual observations, capacity, overload latch and runtime policy |
| `weir_search_http_*` | Record/startup exchange attempts, elapsed time, framed records and bytes |

Typed record counters are updated once at Store completion.
`weir_store_records_total` preserves mutation effect evidence:
`outcome="applied"` does **not** mean business success. For example, a
query-visibility barrier can fail after application, producing `APPLIED` with
`Failure` even when backend HTTP returns 200 and the RPC completes OK. This
increments both `records_total{operation="mutate",outcome="applied"}` and
`record_failures_total{operation="mutate"}`. A successful document read or a
missing record without `Failure` does not increment the failure counter.

`weir_store_record_failures_total` has only the fixed `operation` label (`read`,
`mutate`), with both series initialized to zero. It counts admitted records that
terminate with `Failure`, including queued cancellations; it does not count
preparation/admission rejections, individual HTTP exchanges, Lua evaluations or
retries, RPC completion, publication acknowledgements, or scrapes. No failure
message, code, resource, ID, or backend text becomes a label. This failure
counter adds exactly two series per Store.

Both typed record counters exclude Native. Native is opaque forwarding: its
`NativeEnd` describes transfer completion/failure, not a portable backend
business result. In particular, a complete non-2xx HTTP response is not
interpreted as a generic business failure. These counters are not generic Native
failure metrics.

`weir_store_native_completions_total` counts each admitted Native call once at
the same Store terminal point. Its only label is the fixed `completion`
vocabulary: `not_started` (`NATIVE_NOT_STARTED`), `response_complete`
(`RESPONSE_COMPLETE`) or `response_incomplete` (`RESPONSE_INCOMPLETE`). Exactly
these three series are initialized to zero; no enum string, failure detail, HTTP
status, resource or identity becomes a label. Preparation and admission
rejections, and Natives abandoned while waiting for a slot, do not count as
completions.
Scrapes do not count again.

This is **Native forwarding terminal evidence**, not backend business success,
RPC status or proof that the client received the response. An incomplete
response with `Failure` can be delivered in an RPC that finishes gRPC OK; it
still increments `completion="response_incomplete"`. A complete opaque HTTP 500
response increments `completion="response_complete"`, without inferring business
failure or updating either typed record counter. Native completion adds exactly
three series per Store.

Search HTTP metrics include failed attempts and exclude Native relay. HTTP
success does not prove every bulk item succeeded. Bulk elapsed time includes
`refresh=wait_for`; concurrent durations overlap and cannot be summed as wall
time. Framed request byte counts exclude headers and do not prove delivery. Lua
evaluation time excludes external JSON decoding/encoding. Metrics use bounded
labels without document or target identities.

## Memory pressure and recovery

Capacity is the smallest trusted physical host, finite process address-space or
visible container limit. Admission latches at the configured high watermark and
recovers below the low watermark; defaults are 80% and 70%. Recovery needs
complete trusted observations. Unknown samples alone do not manufacture
overload.

On Linux cgroup v2, each finite visible level uses its own current/limit pair.
Validated reclaimable `inactive_file` is subtracted from `memory.current`;
missing or inconsistent statistics retain raw usage. Metrics expose both
`weir_memory_cgroup_current_bytes` and `weir_memory_cgroup_working_set_bytes`.
Cgroup usage is not added to process RSS.

The CLI owns an automatic Go soft limit of 80% of the remaining process recovery
allowance after memory outside RSS is accounted for. With default watermarks its
upper bound is 56% of capacity. Positive targets have a 16 MiB floor capped by
that upper bound. This encourages release of idle Go heap after allocation
bursts. It cannot reclaim a live working set or memory outside Go.

An explicitly present `GOMEMLIMIT`, including `off`, or an already finite
runtime limit takes precedence. A later external runtime-limit change ends
automatic ownership. Shutdown restores the previous setting only while Weir
still owns it.

Inspect `weir_memory_go_runtime_limit_bytes`, `weir_memory_go_runtime_managed`,
`weir_memory_recovery_allowance_bytes` and
`weir_memory_recovery_allowance_known`. Unknown or known-zero remaining
allowance preserves the previous soft limit while actual pressure keeps
admission closed. Use container/OS limits for the hard boundary.

## Slow consumers, failures and shutdown

Store queues bound waiting input, and per-stream publication windows bound
retained output. A Native command occupies one of its Store's 64 Native slots
while its consumer reads, and further Natives wait outside the queue, so slow
Native consumers can delay other Native commands but not typed reads and
mutations. The stall timeout ends an RPC whose input or output makes no
progress; keepalive closes unresponsive connections. gRPC keeps a stalled
stream's last unsent message until the client reads it or the connection
closes, and graceful shutdown waits for such streams until its deadline. Consume
output promptly and set application deadlines.

A transport failure after a possible write leaves it UNKNOWN unless application
was already confirmed. Preserve indexed results received before failure. Do not
replay unconfirmed mutations automatically. Query-visible completion can fail
after application; APPLIED with Failure is not a successful visibility
guarantee.

MongoDB transaction cleanup is bounded and best effort. An acknowledged abort or
`NoSuchTransaction` does not create a permanent session tombstone; delayed
remote work can retain locks until MongoDB's own transaction timeout and
cleanup. A lost commit acknowledgement remains UNKNOWN. Confirmed abort/conflict
retries retain the original caller lifetime.

Align termination grace with preStop, `lifecycle.shutdown_timeout` and
`lifecycle.cleanup_timeout`. Each Store drains admitted work for up to the
shutdown timeout (default `5s`), then gets one cleanup budget (default `2s`) to
cancel remaining work, end MongoDB sessions and close backend connections. An
expired best-effort session cleanup is neither confirmed remote cleanup nor
permission to replay. Disconnect errors and work that does not stop within the
budget are reported as shutdown failures, never as a clean stop. Results not yet
acknowledged by the consumer stay charged until Ack or stream closure. During
rolling updates, terminating Pods can retain connections while new Pods start;
`maxSurge: 0` does not cap that overlap. Scaling replicas increases aggregate
backend demand and does not add database capacity.

Validate the real deployment with persisted reads/writes, visibility
requirements, slow consumers, cancellation, memory bursts and rolling
replacement. Unit tests, tagged compilation and short functional probes do not
establish sustainable throughput or universal latency guarantees. Use the
workload tooling in [weir-tests](https://github.com/batchstream/weir-tests) and
write run artifacts under ignored `.testdata/`.
