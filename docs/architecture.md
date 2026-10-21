# Architecture

Weir has one process per node. A Node owns local Store runtimes, application and
peer listeners, an in-memory directory, diagnostics and shared execution-memory
accounting. Lua evaluates inside that same process.

```text
client -> any node: ResolveStore -> owning nodes: Execute -> local backend
                  peer directory synchronization
```

Business payloads go directly to a Store owner. Peers exchange only Store names,
replica identities, reachable addresses and leases. Backend configuration and
authentication remain local. See [discovery](discovery-design.md) and
[protocols](protocols.md).

## Module and ownership boundaries

| Component | Responsibility |
| --- | --- |
| `cmd/weir` | CLI, signals and process lifecycle |
| `internal/app` | Configuration, resource assembly, diagnostics and Node shutdown |
| `internal/directory` | Store ownership, lease expiry and peer synchronization |
| `internal/server` | Public/peer RPCs, transport admission and ordered publication |
| `internal/store` | Queueing, batch selection, execution slots and memory ownership |
| `internal/execution` | Prepared plans and the MongoDB/Search adapter contract |
| `internal/backend` | Native backend encoding, execution and completion evidence |
| `internal/luaengine` | Restricted VM, value conversion and allocation ownership |
| `internal/overload` | Actual memory observations, admission hysteresis and Go soft limit |
| `internal/protowire` | Validation over existing protobuf buffer fragments |

The independent `weir-protocol` module owns public schemas and shared validation.
The independent `weir-go` SDK depends on it. Server acceptance tests consume the
SDK; the production server does not import the SDK. Only the internal peer schema
is generated in this repository.

## Request execution

Execute is a finite bidirectional stream selecting one Store and one operation
kind. Each Read/Mutate request carries one resource and a consecutive one-based
ordinal. Receiving and ordered publication proceed concurrently through a bounded
per-stream ticket window, defaulting to 32 records. The server does not retain a
whole-call request/result table.

Validated records enter one Store scheduler. It combines compatible records from
one or several streams using count and byte targets. Reads and mutations form
separate batches. Writes can collect until their oldest admission reaches the
configured wait; reads use already queued work. Queue capacity returns at dispatch.
Active execution slots return when backend execution finishes. See
[batching](batching.md) for grouping and backpressure.

Same-resource mutations within a stream execute in input order. Each plan keeps
its caller context, ordinal and memory ownership. Results route by plan identity,
so equal ordinals in different RPCs cannot collide. Canceling one caller leaves
other batch members active. Shared execution uses the latest participating
deadline; an unbounded caller keeps it unbounded. Canceling all callers stops
shared backend work.

Native is a singleton invocation on the same scheduler. The client sends one
command, half-closes input and consumes metadata/body events plus NativeEnd.
Success requires the terminal event and final gRPC OK. Native bypasses typed
target qualification and forwards the backend's request/response formats.
It preserves backend pagination, cursors, sessions and permissions.

## Completion evidence

Read failures and mutation outcomes are individual typed results; transport
failure is gRPC status. Confirmed item results survive a later stream failure.

| Outcome | Required evidence |
| --- | --- |
| `NOT_STARTED` | No backend attempt |
| `NOT_APPLIED` | Definite rejection or no-send evidence |
| `APPLIED` | Validated application acknowledgement |
| `UNKNOWN` | Application cannot be determined |

A failure may accompany APPLIED when application is confirmed but the requested
completion guarantee fails. Neither server nor SDK automatically replays possible
writes. Mutate is not an application transaction; a later invalid request does
not undo earlier effects. There is no durable deduplication or exactly-once claim.

MongoDB Lua groups use short snapshot transactions. A confirmed abort can allow a
fresh read/evaluation/write attempt. An ambiguous commit can only retry confirmation
of that same commit. Search uses real-time reads and version-conditional writes;
only confirmed conflicts allow reevaluation of affected items. Neither adapter
adds revision fields to business documents. Query visibility is requested
explicitly and remains part of completion.

## Memory and transport

The Node's execution account is shared across Stores and covers ingress, queued
input, backend workspace, retained results, Lua allocations and encoded output.
Its bound derives from two thirds of the trusted process recovery allowance.
Unknown observations preserve the last trusted bound; shrinking capacity does not
discard existing ownership. Cold unknown capacity cannot admit work that requires
a trusted bound. Native has an additional count bound of 1024 invocations.

Ingress reserves message and DATA buffer storage before allocation. Connection
parser workspace has a conservative reservation. Decoded input transfers ownership
to execution rather than receiving a second unrelated reservation. Encoded output
keeps its charge until the transport frees the final buffer reference. An
application acknowledgement cannot release memory still held by the transport.
Actual memory exhaustion returns explicit capacity failures.

The gRPC and Lua allocation hooks are bounded local dependency patches. The
[third-party guide](../third_party/README.md) records their purpose, upstream
identities and verification. The transport remains native gRPC/HTTP2.

Lua parsing, compilation, VM objects, allowed library buffers and document
conversion reserve allocations on the same account. Each evaluation gets fresh
mutable state. The budget conservatively accumulates allocations until evaluation
and result processing finish; it does not measure live Go heap. Allocation failure
and cancellation cannot be suppressed with Lua protected calls. Programs must
still be trusted. See [Lua](lua.md).

Actual RSS/physical footprint and visible container working-set pressure control
overload admission independently of reservation estimates. The CLI manages the Go
soft limit below the recovery threshold, while explicit runtime policy takes
precedence. Container/OS memory limits remain the hard boundary.

Application listeners serve StoreService; peer listeners serve SyncDirectory.
Both use plaintext gRPC on an isolated network. RPC cancellation and input stalls
normally affect only that RPC. Connection framing/output stalls can close the
connection and affect its other RPCs. Confirmed write evidence remains valid.

## Lifecycle

Startup resolves configuration, qualifies local backends, binds listeners and
assembles the directory. Partial startup closes already owned resources.
Diagnostics readiness tracks lifecycle rather than backend health or Store load.

Shutdown stops new admission, withdraws directory ownership and drains admitted
work to its deadline. Cancellation then stops owned work; bounded cleanup joins
runtimes and closes backend connections, listeners and diagnostics. Cancellation
never proves a sent write was not applied. Remote transaction locks can outlive
local cancellation or best-effort session cleanup. See [operations](operations.md).
