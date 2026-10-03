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
and unrelated clients. Check an explicit budget before applying the HPA:

```sh
python3 scripts/connection-load.py budget \
  --store-concurrency 2 --max-replicas 4 --max-surge 0 \
  --terminating-pods 4 --other-connections 16 --database-budget 40
```

`maxSurge: 0` and HPA `maxReplicas` do not cap terminating Pods. The overlap value
is an operational assumption, not a guarantee supplied by these manifests. The
existing rolling-update patch uses zero surge; its grace period bounds normal
termination time, but repeated rollouts, hung termination or additional owners
still require observation and a connection reserve.
Kubernetes explicitly excludes terminating Pods from the available-replica bound;
see the [Deployment rollout note](https://kubernetes.io/docs/concepts/workloads/controllers/deployment/#rolling-update-deployment).

Execution concurrency also multiplies with replicas. Independently controlled
stores on four Pods can run four times the work of one Pod. Allocate the aggregate
database work budget across all stores and the maximum active replicas, then let
each controller reduce its own share under congestion. HPA adds Weir CPU; it does
not increase the fixed database budget. No distributed coordinator is required
for a conservative fixed allocation.

gRPC connections usually pin clients to one Weir instance. Increasing replicas
does not redistribute an active business RPC. Use client-side distribution
across discovered instances, sufficient independent connections, or reconnect
through the normal lifecycle, and measure per-instance load before interpreting
HPA CPU averages.

# Measuring connection concentration

Build the opt-in capacity helper with `go build -tags integration`. Create a
disposable database and seed the 1000 `read-NNNN` records once using its `setup`
mode. `connection-probe` never resets, seeds or audits the collection. It starts
clients at a shared timestamp, offers only reads, keeps pools open during an idle
phase, then closes them. Use the same total rate for one and many real processes.

`scripts/connection-load.py run` accepts JSON command arrays that launch the
helper in existing owned containers or Pods. For example, an observer command
may be `["docker", "exec", "--user", "1000", "OWNED_ES", "/client"]`; it needs
the database UID or suitable permission to read its `/proc/PID/fd` links. The
observer intersects database-owned socket inodes with that PID's network TCP
tables and counts only its listening port. It does not query the database or open
an additional backend connection.

Compare direct one-process, direct 16-process, 16-process to one Weir, and
16-process distributed across three Weir instances. The runner divides the
offered rate exactly across processes and saves every launch command, client
ledger, raw socket sample and phase summary. Its `qualified` field requires every
planned read to succeed, so a lower connection count obtained by failed or
dropped work cannot be presented as a benefit.

Account for workers as well as processes: 16 processes with eight workers can
have 128 RPCs in flight, independently of their connection pools. A Weir session
limit of 32 or 64 can reject this workload before its CPU is saturated. The probe
uses a shared arrival timestamp and the same read-ID sequence in every process,
so equal-rate processes produce synchronized reads of the same key. This probe sends single-record RPCs and does not measure bulk API throughput; this is a connection and admission
test, and dispersed-key load is needed to evaluate general batching throughput.

After business clients close, Weir's backend pool may remain connected for reuse;
this is expected. MongoDB pools stay until adapter shutdown, while Search idle
HTTP connections have a 30-second idle timeout. Continue observing while stopping
the owned Weir instances to verify return to the original database baseline.
Kernel sampling can miss brief connection transitions; combine the measured
ESTABLISHED counts with Weir's owned/peak/limit metrics and final balanced
acquired/released shutdown evidence when checking the raw ownership bound.
