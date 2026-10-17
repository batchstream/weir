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
