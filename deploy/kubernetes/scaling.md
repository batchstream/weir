# Scaling Weir while bounding database connections

`hpa.json` is an optional CPU HPA example, with one to four replicas and at most
one scale change per minute. Apply it only after sizing the backend's total
connection and execution budgets. It is not applied by the integration fixture.
Every Pod runs one Weir process; Lua runs inside that process.
The bounded policies use the documented
[HPA scaling behavior](https://kubernetes.io/docs/concepts/workloads/autoscaling/horizontal-pod-autoscale/#configurable-scaling-behavior).

A CPU HPA responds to CPU utilization only. Business RPC admission can reach its
configured limit while CPU remains below the HPA target. Check
`weir_admission_rejections_total{reason="sessions"}` and the ingress occupancy
metrics alongside CPU; this example does not automatically scale on those
metrics. More replicas also require callers to distribute new sessions across
them.

Each LocalStore owns a separate backend adapter. Its connection limit is
`max_concurrency + 1` raw TCP sockets, for both MongoDB and Search. The extra
credit covers polling/retiring MongoDB connections or the Search native transport
and connection lifecycle. Raw ownership includes connections before pool
insertion and during close. Several LocalStores targeting the same database add
their limits; every replica contributes its own backend connection budget.

For each database endpoint, reserve:

```
(maxReplicas + rollout surge + terminating Pod overlap)
    * sum(max_concurrency + 1 for each LocalStore targeting the endpoint)
    + other database connections
```

The sample `routes.yaml` has one Search LocalStore with `max_concurrency: 2`.
At four replicas its steady raw connection ceiling is 12. If four terminating
Pods still retain sockets, reserve 24 instead, plus administration, replication
and unrelated clients. Calculate this reserve before applying the HPA.

`maxSurge: 0` and HPA `maxReplicas` do not cap terminating Pods. The overlap value
is an operational assumption, not a guarantee supplied by these manifests. The
existing rolling-update patch uses zero surge; its grace period bounds normal
termination time, but repeated rollouts, hung termination or additional owners
still require observation and a connection reserve.
Kubernetes explicitly excludes terminating Pods from the available-replica bound;
see the [Deployment rollout note](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment).

Execution concurrency also multiplies with replicas. Independently controlled
stores on four Pods can run four times the work of one Pod. Allocate the aggregate
database work budget across all stores and the maximum active replicas, and
configure each Store within that allocation. HPA adds Weir CPU; it does
not increase the fixed database budget. No distributed coordinator is required
for a conservative fixed allocation.

gRPC connections usually pin clients to one Weir instance. Increasing replicas
does not redistribute an active business RPC. Use client-side distribution
across discovered instances, sufficient independent connections, or reconnect
through the normal lifecycle, and measure per-instance load before interpreting
HPA CPU averages.

## Measuring connection concentration

Use the independent [weir-tests](https://github.com/batchstream/weir-tests)
repository for matched direct and Weir workloads. Compare the same total request
rate and number of business processes, and verify all submitted work completes.
Observe database socket ownership together with Weir backend connection metrics;
a lower connection count caused by rejected or dropped work is not a benefit.

After clients close, backend pools may retain idle sockets for reuse. MongoDB pools
remain until adapter shutdown; Search HTTP idle connections expire after 30 seconds.
Observe shutdown as well as active traffic when checking the aggregate budget.
