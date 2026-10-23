# Scaling Weir on Kubernetes

`hpa.json` is an optional CPU HPA example with one to four replicas and at most
one scale change per minute. Each Pod runs one Weir process, including Lua
evaluation.

## Backend demand

Each Store and replica has its own backend adapter. MongoDB pools and Search
HTTP transports have no connection-count limit; connections grow with actual
demand. `batching.max_in_flight` (default 16 simultaneous adapter invocations,
shared by reads, mutations and Native streaming) is the only per-Store,
per-replica concurrency cap, and explicit `0` removes it. Business connections
and RPCs have no separate cap. Paused Native output releases its execution slot
but can keep its backend socket open, so open connections can exceed
`max_in_flight`.

Size database connection limits for the sum over Stores and replicas, including
rollout overlap. Adding Weir replicas raises aggregate database demand and does
not add database capacity. Separate Stores provide independent scheduling
budgets at the cost of additional backend connections. See
[batching](../../docs/batching.md) for queue, batch and execution interaction.

Observe backend connection ownership, active executions, queue count/bytes,
paused Native output, completed throughput and latency together with backend
CPU and memory. Consume Native responses promptly.

After callers close, idle backend connections can remain for reuse. MongoDB pools
close at adapter shutdown; Search idle connections expire after 30 seconds by
default.

## Memory

Set a container memory limit; it is the hard memory boundary. Weir detects the
visible capacity, excludes reclaimable cgroup `inactive_file` from admission and sets
the Go soft limit below the overload recovery watermark unless `GOMEMLIMIT` is
set. Verify any override under allocation bursts. See
[operations](../../docs/operations.md#memory-pressure-and-recovery) for the
gauges that show the active policy.

## Connections and rollouts

A gRPC connection usually pins calls to one instance, and scaling replicas does
not redistribute an active RPC. Distribute new connections across discovered
instances and measure per-instance load before interpreting average HPA CPU
utilization. The transport stall watchdog closes a connection that stops output
progress, affecting the other RPCs on that connection; continuously progressing
streams are bounded only by caller deadlines.

During rolling updates, terminating Pods can retain backend connections while new
Pods start. `maxSurge: 0` and HPA `maxReplicas` do not cap that overlap. The
grace period bounds normal termination; connection metrics record sockets until
their close completes. Align `terminationGracePeriodSeconds` with preStop and the
`lifecycle` timeouts.

## Measuring

Use the [weir-tests](https://github.com/batchstream/weir-tests) repository for
matched direct and Weir workloads. Compare the same submitted request rate and
verify completed work and backend outcomes as well as transport status; fewer
sockets due to rejected work are not a throughput improvement.
