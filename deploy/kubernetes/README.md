# Minimal Kubernetes deployment

One ordinary Deployment and ClusterIP Service, with a read-only standard Secret
volume containing `node.json`. JSON is a native Kubernetes manifest format.
There is no operator, chart, sidecar, Weir RBAC, KubeAPI access or public ingress.
Use a trusted isolated namespace/network; application gRPC is plaintext.

Copy `node.example.json`, set the pre-created backend/index and any required
standard backend TLS/credentials. The example has no credentials. Use the existing
Search `connection` or Mongo configuration for those profiles; mount an explicit
backend CA read-only if required. Do not disable TLS verification. Create the
`weir-config` Secret from this file using your approved secret provisioning process.
No Secret content belongs in an image or source control. Configuration is immutable
for a running process; changing the Secret requires a controlled restart.

Replace `weir:REPLACE_WITH_IMMUTABLE_IMAGE` in `weir.json` with the verified release
`repository@sha256:...` (or a locally loaded unique source tag for isolated tests).
The placeholder is intentionally not runnable. Select the namespace, kubeconfig
and context explicitly in every kubectl command. Server-side dry-run the manifest,
apply it, then verify rollout, EndpointSlice and an actual RPC through Service DNS.
`weir.<namespace>.svc.cluster.local:7447` is the client endpoint for default cluster
DNS. An available Deployment alone does not prove backend success.

PID 1 remains `/weir -config /etc/weir/node.json`. UID/GID 65532, read-only root,
RuntimeDefault seccomp, no escalation/capabilities and no service account token
apply to the application. The reference resource profile requests/limits 2 CPUs
and 1 GiB. `memory_mib: 768` leaves headroom for kernel/socket/page accounting and
short probe processes. Linux Guard pairs process RSS with its 768 MiB soft budget,
and each visible cgroup current/max independently; 80%/70% admission hysteresis
is unchanged. These are per-process/local limits, not a global database quota.
Each Local uses concurrency 2 and the existing C+1 connection owner boundary.
No capacity or sustained-load qualification follows from these starting values.

The three probes execute `/weir` directly, with no shell/curl. Diagnostics bind
only `127.0.0.1:7449`; no Pod-IP HTTP probe or diagnostic Service port is used.
`-probe live|ready [-probe-address IP:port]` loads no configuration and initializes
no database/Guard/server. Exit 0 means an exact HTTP 200 success body, exit 1 means
invalid arguments, unavailable, unhealthy or timed out. Success is silent; errors
are fixed messages. Only numeric loopback addresses and fixed paths are accepted.
No proxy, redirect, compression or keepalive; total timeout 750 ms, response header
limit 1 KiB, body read limit 65 bytes with exact `ok\n` / `ready\n` matching.
The probe flags are exclusive with version and all server/config flags.

Startup checks readiness every 2 seconds, allowing about 30 seconds for startup;
until successful it suppresses readiness/liveness checks. CLI backend construction
itself still has its existing 5-second deadline. Kubelet's 2-second exec timeout
allows process scheduling overhead beyond the 750 ms network budget. Readiness
uses one failure every 2 seconds to withdraw a draining process; liveness needs
three failures at 5-second intervals to tolerate transient diagnostic contention.
These are initial operational values, not measured availability SLOs.

Readiness means validated listeners are serving and not failed/draining. It remains
true during ordinary saturation or temporary backend loss; it does not promise a
successful database operation. Liveness only checks the diagnostic process path.
Neither performs a synthetic database ping or turns overload into a restart.

SIGTERM enters drain immediately: readiness lowers and new admission stops; admitted
finite work retains its original deadlines within the existing 5-second shutdown
cap. The 15-second Pod grace allows kubelet delivery/scheduling overhead. There is
no arbitrary preStop sleep. EndpointSlice removal and local drain are asynchronous;
a client can still race a stale endpoint. Recreate avoids rollout surge, but a
manual Pod deletion can overlap one terminating and one replacement process. Budget
both: up to 4 CPU / 2 GiB of application limits and twice the Local connection budget
during that bounded overlap. Kubernetes is not an absolute live-process ceiling in
partition/force-delete scenarios; those require separate qualification.

A single replica has downtime during replacement and no zero-interruption promise.
Long-lived gRPC connections may break; new calls can reconnect to the replacement.
Do not replay a sent mutation whose result is missing: it is UNKNOWN. APPLIED needs
an actual reply proving backend acknowledgement; a disconnected Pod is not proof
of NOT_STARTED. Resolve uncertain application effects explicitly. Static config
or image rollback uses the same controlled Recreate lifecycle, with a known prior
immutable image/config pair; schema/backend compatibility must be checked first.

See `docs/milestone-20.md` for exact fixture evidence and untested gates. Kubernetes
1.36.4 is the initial selected test version; other versions, three replicas/two
workers, node failure, capacity and 24-hour soak are separate gates.
