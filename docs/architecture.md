# Architecture

Weir has one process per server. A Node owns its Store runtimes, the application
listener, diagnostics and the memory-pressure guard. Lua evaluates inside that
same process.

```text
client -> Weir address -> any replica: Execute -> local backend
```

Every replica behind an address hosts the same Stores and shares nothing with
the others. A request naming a Store the server does not host fails with
NotFound. Backend configuration and authentication remain local. See
[protocols](protocols.md).

## Module and ownership boundaries

| Component | Responsibility |
| --- | --- |
| `cmd/weir` | CLI, signals and process lifecycle |
| `internal/app` | Configuration, resource assembly, diagnostics and Node shutdown |
| `internal/server` | StoreService, transport admission and ordered publication |
| `internal/store` | Queueing, batch selection and execution slots |
| `internal/execution` | Prepared plans and the MongoDB/Search adapter contract |
| `internal/backend` | Native backend encoding, execution and completion evidence |
| `internal/luaengine` | Restricted VM, value conversion and execution limits |
| `internal/overload` | Actual memory observations, admission hysteresis and Go soft limit |

The independent `weir-protocol` module owns public schemas and shared validation.
The independent `weir-go` SDK depends on it. Server acceptance tests consume the
SDK; the production server does not import the SDK. No code is generated in this
repository.

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
its caller context, and each result returns to the ticket that submitted it, so
equal ordinals in different RPCs cannot collide. Canceling one caller leaves
other batch members active. Shared execution uses the latest participating
deadline; an unbounded caller keeps it unbounded. Canceling all callers stops
shared backend work.

Native is a singleton invocation that bypasses the record queue and execution
slots. Each Store runs at most 64 Native commands; further ones wait for a free
slot until their caller gives up. A Native command lasts until its caller has
read the response or the output watchdog ends the RPC, so slow Native consumers
delay only other Native commands, never typed work. The client
sends one command, half-closes input and consumes metadata/body events plus
NativeEnd.
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

Memory use is bounded by static limits rather than per-byte accounting. Every
request and response message is at most 64 MiB. Each Store bounds its queue by
operation count and request bytes, its batches by count and bytes, and its
concurrent adapter invocations by `max_in_flight`. Each record stream holds at
most 32 outstanding records. Size the container for these
limits and the largest documents you serve; [batching](batching.md) shows the
calculation.

Actual RSS/physical footprint and visible container working-set pressure latch
overload admission: new work receives `RESOURCE_EXHAUSTED` until pressure falls
below the recovery watermark. The CLI manages the Go soft limit below the
recovery threshold, while an explicit runtime policy takes precedence.
Container/OS memory limits remain the hard boundary.

Lua programs run within the caller's deadline and the VM's call-depth and stack
limits. There is no per-evaluation memory or instruction limit, so transforms
must be trusted. See
[Lua](lua.md).

The application listener serves only StoreService, over plaintext gRPC on an
isolated network. A stalled input or output
cancels only its RPC after the stall timeout: the handler and its Store work end
at once, but gRPC keeps the stream's last unsent message until the client reads
it or the connection closes. Keepalive pings and the idle and handshake timeouts
close unresponsive connections. Every connection ends with GOAWAY after about
five minutes so clients re-resolve their address; running streams finish first.
Confirmed write evidence remains valid.

## Lifecycle

Startup loads the configuration file, then qualifies local backends and binds
listeners within 30s. Partial startup closes already owned resources.
Diagnostics readiness tracks lifecycle rather than backend health or Store load.

Shutdown stops new admission and drains admitted work to its deadline. Cancellation then stops owned work; bounded cleanup joins
runtimes and closes backend connections, listeners and diagnostics. Cancellation
never proves a sent write was not applied. Remote transaction locks can outlive
local cancellation or best-effort session cleanup. See [operations](operations.md).
