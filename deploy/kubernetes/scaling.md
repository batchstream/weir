# Scaling Weir with observed backend demand

`hpa.json` is an optional CPU HPA example with one to four replicas and at most
one scale change per minute. It is not applied by the integration fixture.
Each Pod runs one Weir process, including Lua evaluation.

Each Store defaults to 16 simultaneous adapter invocations, shared by reads,
mutations and Native streaming. `batching.max_in_flight` controls
this budget; explicit `0` removes the execution limit. Each Store and replica has
its own budget, so their aggregate database demand can grow when scaling.
Business connections, RPCs and backend pools have no additional concurrency cap.
Each local Store owns an independent backend adapter. Connections grow with
actual demand; every replica can increase aggregate database traffic.
Observe backend connection ownership, active executions, queue count/bytes,
completed throughput and latency together with backend CPU and memory.
Use the container memory limit for the hard memory boundary; Weir detects the
visible capacity and observes actual pressure automatically.

The configured queue bounds waiting input. Reads and mutations form separate
batches within that one queue. Reads use compatible reads already queued without
waiting for new arrivals; they do not flush the write collection window. Both
can wait for an occupied execution slot. A full queue waits for dispatch or caller
cancellation, and dispatch immediately returns its capacity. An active request
does not hold waiting queue capacity. Use integration measurements to select
execution and replica budgets for the actual workload.

During rolling updates, account for overlap with terminating Pods when measuring
backend connections. `maxSurge: 0` and HPA `maxReplicas` do not cap that overlap.
The grace period bounds normal termination; connection metrics record sockets
until their actual close completes.

A gRPC connection usually pins calls to one instance. Scaling replicas does not
redistribute an active RPC. Distribute new connections across discovered instances
and measure per-instance load before interpreting average HPA CPU utilization.

Use the independent [weir-tests](https://github.com/batchstream/weir-tests)
repository for matched direct and Weir workloads. Compare the same submitted
request rate and verify completed work; fewer sockets due to rejected work are
not a throughput improvement. Check backend outcomes as well as transport status.

After callers close, idle backend connections can remain for reuse. MongoDB pools
close at adapter shutdown; Search idle connections expire after 30 seconds.
Observe shutdown as well as active traffic when measuring aggregate ownership.
