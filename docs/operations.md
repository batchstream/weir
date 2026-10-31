# Operations

## Health and deployment

Enable an independent diagnostics listener. Kubernetes HTTP probes connect to
the Pod address, so bind it on `[::]` or the Pod IP there and restrict access with
network policy; elsewhere loopback is enough:

```yaml
listeners:
  application: 127.0.0.1:7447
  diagnostics: 127.0.0.1:7449
```

`GET /livez` reports a running diagnostics handler. `GET /readyz` reports that
the Node entered service and is not draining or failed. Neither endpoint tests
backend health or workload capacity. `GET /metrics`
exports a Node-owned Prometheus registry. Diagnostics serves HTTP/1 GET and HEAD
on these three paths, closes each connection after one response and gives every
request one-second read and write deadlines. Point
HTTP liveness and readiness probes at these paths; `weir version` prints the
build identity.

The application port requires isolated networking. Every replica behind one
address uses the same Store definitions. In Kubernetes, put the replicas behind a
headless Service so clients connect to every Pod; the
[weir-charts](https://github.com/batchstream/weir-charts) Helm chart is the
deployment reference. Every Pod runs one Weir process.

Each client connection ends with GOAWAY after about five minutes. Running streams
finish first, and the client reconnects and re-resolves the address, so replicas
added by scaling start receiving calls within that time. Adding or removing a
Store needs a coordinated rollout: until every replica runs the new definitions,
a request can reach a replica that answers NotFound, and the SDK does not retry.

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
not reused for other members. Each member pool creates at most two connections
at a time, while monitors and pools can exist for multiple discovered members. Monitor
aggregate backend demand and connection counts on the database side.

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

## Metrics

Observe completed work and actual backend demand together with queue and memory
occupancy. A lower connection count caused by rejected traffic is not evidence
of improved capacity.

| Family | Interpretation |
| --- | --- |
| `weir_node_ready` | Readiness |
| `weir_store_active_executions`, `weir_store_active_executions_limit` | Per-Store record batches and their limit |
| `weir_store_active_natives`, `weir_store_active_natives_limit` | Per-Store running Native commands |
| `weir_store_native_seconds` | Native call time, including response delivery to the client |
| `weir_store_queued_operations`, `weir_store_queued_bytes` | Waiting work |
| `weir_store_records_total` | Final typed results: mutation effect (`applied`, `not_applied`, `not_started`, `unknown`) or read `success`/`failure` |
| `weir_store_record_failures_total` | Final typed read/mutate results carrying `Failure`, including `APPLIED` with `Failure` |
| `weir_store_native_completions_total` | Final Native forwarding terminal evidence (`not_started`, `response_complete`, `response_incomplete`), not backend business success, RPC status, or client receipt |
| `weir_store_rejections_total` | Preparation, draining and overload denials, not completed records |
| `weir_store_batch_operations`, `weir_store_queue_wait_seconds`, `weir_store_execution_seconds` | Record batch sizes, queue wait and record batch execution time; Natives are excluded |
| `weir_lua_evaluation_seconds` | Lua evaluation time by `outcome` (`success`, `error`, `timeout`, `canceled`), for both backends |
| `weir_ingress_sessions`, `weir_rpc_completions_total`, `weir_admission_rejections_total` | Active RPCs, completed RPCs by `status` and RPCs refused at admission |
| `weir_watchdog_expirations_total` | RPCs ended by the input or output stall timeout |
| `weir_memory_live_bytes`, `weir_memory_limit_bytes`, `weir_memory_overloaded` | Live memory, the Go memory limit and whether memory pressure refuses new work |
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
evaluation time includes compilation and document conversion, not the backend's
own decoding or encoding. Metrics use bounded labels without document or target
identities.

## Alerts

The [weir-charts](https://github.com/batchstream/weir-charts) PrometheusRule
alerts on one Store group, selected by its `namespace` and `weir_group` labels.
Rejections during a drain never alert; a single failover burst stays below the
ten-minute thresholds. A replica is not Ready only while it starts, for at most
30 seconds, or drains, so Pod restarts and crash loops are left to the cluster's
Kubernetes alerts.

### WeirGroupUnavailable

Prometheus sees no Ready replica of the group, so every call to its Stores fails
or the group runs unobserved. Check the Pods and their events. A replica that
never becomes Ready logs why startup failed: the configuration, a credential
file, or a backend it could not qualify within 30 seconds. If the Pods are
Ready, the scrape path is broken; see WeirScrapeFailing.

### WeirScrapeFailing

Prometheus cannot read `/metrics` from a replica. Check that `metrics.ingress`
admits the scraper under the NetworkPolicy, and that the metrics Service and
ServiceMonitor select the Pod. Other alerts are blind to that replica meanwhile.

### WeirMemoryPressure

A replica keeps refusing new work with `UNAVAILABLE` because live memory
reached 80% of its limit ([Memory pressure](#memory-pressure)). Compare
`weir_memory_live_bytes` with the in-flight work: large documents, slow
consumers holding results or Native responses keep memory live. Raise the
memory limit from the [batching bounds](batching.md#memory-sizing), add
replicas, or slow the clients down.

### WeirAdmissionRejections

The group keeps refusing whole RPCs. `overload` means memory pressure; see
WeirMemoryPressure. `execute` means calls name a Store the group does not host:
a client points at the wrong group, or a Store was removed before its clients.
`method` means clients call a method Weir does not serve, which points to a
client built against another protocol.

### WeirStoreRejections

A Store keeps refusing records before queueing them. `overload` means memory
pressure refused records on streams that were already admitted; see
WeirMemoryPressure. `prepare` means records name a resource or carry a body the
Store cannot use, which points to a client bug. A full queue does not reject:
it holds senders back before their records are queued, which shows as
`weir_store_queued_operations` or `weir_store_queued_bytes` staying near the
Store's queue bounds rather than in queue wait.

### WeirUnknownOutcomes

Mutations keep ending without proof of whether they were applied, usually
because backend connections fail or time out mid-write, or because client
deadlines or cancellations end writes that were already sent. Check backend
health, elections, network paths and client deadlines. Clients must reconcile
each UNKNOWN mutation by reading the record before retrying it, never replay it
blindly ([failures](#slow-consumers-failures-and-shutdown)).

### WeirRecordFailures

More than 5% of a Store's records end with a Failure. This includes mutations
the backend rejected, failed reads and APPLIED mutations whose visibility
barrier failed. Conditional writes fail by design: a replace or expression on a
missing record, a duplicate key, a write conflict or a validation rule. If those
are the application's expected results, this threshold does not fit that
Store. Otherwise read the failure messages in client logs to tell a client bug
from a backend fault.

### WeirQueueWaitHigh

Records of a Store wait more than a second before dispatch at the 99th
percentile. Compare `weir_store_execution_seconds` and
`weir_store_active_executions` with their limit. Busy slots with slow
executions mean the database is the bottleneck, and more Weir replicas add
backend demand without adding capacity. Free slots mean records wait for
earlier mutations of the same resource: one stream's mutations of a resource
run one at a time, so a client that keeps rewriting the same records queues
behind itself.

### WeirWatchdogExpirations

Weir keeps ending RPCs whose input or output made no progress for the stall
timeout. `output` means clients stopped reading results; `input` means clients
opened a stream and stopped sending. Both usually point to blocked client
workers. The ended streams' unconfirmed mutations are UNKNOWN.

## Memory pressure

Weir compares its live memory with the Go memory limit. Live memory is the live
heap at the last garbage collection plus goroutine stacks and runtime metadata;
free heap and garbage do not count, because the collector reclaims them before
the limit. At 80% of the limit, new RPCs and records receive
`UNAVAILABLE`, as during a drain; admission reopens at or below 70%. While refusing work,
Weir collects garbage at most once a second so that it notices finished work
promptly. Release builds contain no C code, so the Go runtime accounts for
nearly all process memory.

`weir serve` sets the Go memory limit to 90% of the `memory.max` of its own
cgroup v2 group, leaving the rest for memory outside the Go runtime; the
collector then works harder before the container's hard limit is reached. A
nonempty `GOMEMLIMIT` takes precedence, and `GOMEMLIMIT=off` disables both the
soft limit and shedding. Weir does not read parent groups or cgroup v1, so set
`GOMEMLIMIT` when the binding limit is elsewhere, such as a Kubernetes pod-level
limit or a systemd slice. Without either, Weir does not shed and the OS remains
the only boundary.

Shedding cannot reclaim a live working set. Size the container from the bounds
in [batching](batching.md#memory-sizing) and observed document sizes.

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

Align termination grace with preStop, `shutdown_timeout` and the 2s cleanup.
Each Store drains admitted work for up to the shutdown timeout (default `5s`),
then gets one fixed 2s cleanup budget to cancel remaining work, end MongoDB
sessions and close backend connections. An expired best-effort session cleanup
is neither confirmed remote cleanup nor permission to replay. Disconnect errors
and work that does not stop within the budget are reported as shutdown failures,
never as a clean stop. Results not yet acknowledged by the consumer stay charged
until Ack or stream closure. During rolling updates, terminating Pods can retain connections while new Pods start;
`maxSurge: 0` does not cap that overlap. Scaling replicas increases aggregate
backend demand and does not add database capacity.

Validate the real deployment with persisted reads/writes, visibility
requirements, slow consumers, cancellation, memory bursts and rolling
replacement. Unit tests, tagged compilation and short functional probes do not
establish sustainable throughput or universal latency guarantees. Use the
workload tooling in [weir-tests](https://github.com/batchstream/weir-tests) and
write run artifacts under ignored `.testdata/`.
