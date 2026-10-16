# Configuration decisions

Start with the addresses and backend identity that the process cannot discover:

```yaml
# weir.yaml
listeners:
  application: 127.0.0.1:7447
```

```yaml
# routes.yaml
stores:
  - name: mongo
    backend:
      mongodb:
        uri: mongodb://127.0.0.1:27017
```

Every other field may be omitted for this standalone node. The complete
[process reference](../config/weir.reference.yaml) and
[Store reference](../config/routes.reference.yaml) document the available controls,
their units and their defaults. Omission selects a documented default, never a
second compatibility path.

Copy the reference files to `weir.yaml` and, when hosting local Stores,
`routes.yaml`. The CLI defaults to `--config ./weir.yaml` and an empty `--routes`;
it does not discover other files. Both paths resolve from the process working
directory. An empty routing path reads no Store document and assembles no backend.

Each file contains one YAML mapping with exact lowercase keys. Unknown or
duplicate keys, multiple documents, anchors, aliases, merge keys and explicit tags
are rejected. Counts use integers, flags use `true` or `false`, and text, sizes and
durations use strings. Omission selects a default; YAML null is not omission.
Process fields cannot be null. Optional routing adapter, authentication and TLS
blocks can be null; Store entries cannot.

Durations require a unit: `ns`, `us`/`µs`/`μs`, `ms`, `s`, `m` or `h`, including
combined forms such as `1m30s`. Values must fit a signed 64-bit nanosecond count;
each field documents whether zero is allowed. Byte sizes use integer `B`, `KiB`,
`MiB` or `GiB` units. Strings are literal; only `discovery.peer_address_env` reads
the explicitly named environment variable.

Inspect the resolved policy before startup:

```sh
weir check --config weir.yaml --routes routes.yaml --effective
```

One normalization path fills defaults for file loading, programmatic startup and
inspection. Runtime assembly only maps the resolved fields; inspection only
redacts the same configuration. This reports `basic`, `routing` and `automatic`. Store queue, batch, scan and
backend defaults are materialized; Lua call and stack limits show the actual
native defaults instead of the input sentinel `0`. Explicit overrides remain
visible. The automatic section identifies values that require startup, such as
bound ephemeral ports, the standalone replica group and discovered memory
capacity. Inspection does not bind listeners, resolve DNS, open backend
connections or read CA files. Normal configuration loading still resolves any
explicit credential file sources.

Authentication values and CA paths are replaced with `<redacted>`; backend
addresses, Store names and public discovery addresses remain visible. The output
is an inspection report, not a replacement configuration file. Configuration
changes require a restart.

Memory capacity is discovered from visible container limits, physical host memory
and any finite process address-space limit. Container/OS limits provide the hard
boundary; overload admission observes actual use against that capacity. There is
no configurable memory budget or startup reservation.

Business RPCs and backend IO inherit caller contexts. Transport progress,
connection establishment, keepalive and cleanup have separate controls.
`batching.max_in_flight` controls simultaneous adapter invocations independently
for each Store. Omission selects the default of `16`; an explicit
`0` always means unlimited. Positive integers retain excess work
in the bounded queue for natural batching and admission backpressure. Execution
completion returns the credit without waiting for consumer acknowledgement. The
budget includes Search visibility waits and Native streaming, so use the actual
visibility and traffic pattern when evaluating `1`, `4` or `16`. A smaller budget
does not by itself prove lower database load or higher throughput.
Diagnostics uses an independent HTTP listener; readiness describes node lifecycle,
rather than backend health. Directory records carry Store names and public
endpoints, without backend credentials.

Record exchange targets split grouped work into native requests. One item larger
than the target proceeds alone. MongoDB also respects its advertised BSON command
boundary, normally `16MiB`; response paging remains controlled by MongoDB, with a
document-count hint only below that boundary. Search uses the exchange target for
record request/response groups. Scan publication has separate windows, and Native
requests follow the backend's own command/message boundaries. Exchange targets
do not define a Lua workspace budget.

`batching.max_bytes` bounds both the Store's retained-input estimate and Search's
final framed NDJSON aggregation. Lua-generated documents count at their actual
encoded size. Search flushes when the smaller of this target and
`backend.max_exchange_bytes` is reached, before processing another item. One legal
item larger than a target is sent alone; these are aggregation targets, not a
per-document rejection limit. The mget, Scan and metadata exchange budgets remain
independent of this write target. See [batching guidance](batching.md) for joint
batch-size, wait and execution-concurrency calibration.

The configuration review makes these decisions:

| Setting or mechanism | Decision | Reason |
| --- | --- | --- |
| `lua.vm` | Remove the wrapper; use `lua.max_instructions`, `lua.max_call_depth`, `lua.max_stack_slots` | There is one in-process Lua engine. The extra group adds no independent responsibility. Old keys are rejected. |
| MongoDB target cache | Move into `backend.mongodb.metadata_cache_entries` | Search has no cache configuration at all. Ownership follows the adapter rather than an extra validation branch. `0` disables MongoDB caching. |
| MongoDB pool and adapter retry wrappers | Flatten | Use `backend.mongodb.max_connecting`, adapter `retry_attempts` and MongoDB `retry_backoff`. These fields belong directly to their adapter; single-purpose wrapper types are removed. |
| Separate directory heartbeat and seed schedules | Remove; renew and visit a bootstrap seed during each synchronization cycle | One cadence controls the same directory lifecycle. There is no independent hidden five-second timer. |
| `discovery.lease`, `discovery.sync_interval`, `discovery.sync_timeout` | Expose | Operators control announcement lifetime, exchange frequency and unreachable-peer deadlines. Defaults are `60s`, `1s` and `2s`. The lease uses positive whole milliseconds; local interval and timeout accept any positive duration. The relation `lease > 2 * sync_timeout + sync_interval` leaves room for one peer and one seed exchange. There are no fixed policy maximums. |
| Memory capacity and advertised concrete listener addresses | Discover automatically | The process observes the container/OS limits and actual bound endpoints. Wildcard binds still require explicit reachable advertisements. |
| `queue`, `batching`, `scan`, transport windows and progress deadlines | Retain | These govern different retained work and progress boundaries. Removing them would replace useful control with an invisible threshold. |
| `batching.max_wait` | Expose; default `20ms` | A positive duration bounds write collection from the oldest eligible admission; explicit `0s` disables collection. Reads form separate batches from already queued compatible reads without waiting for future arrivals; they do not flush write collection. Both kinds share the Store queue and execution budget. Full batches, streaming commands, drain and shorter caller deadlines also bypass collection. The resolved value is visible in effective configuration and Store metrics. |
| `diagnostics.allow_intranet` | Retain | A non-loopback diagnostics bind needs an explicit opt-in because it exposes operational endpoints. |
| `discovery.peer_address_env` | Retain | Dynamic instances need one explicit public address source, including Kubernetes downward-API values. General string substitution stays unsupported. |
| Backend authentication and trust roots | Retain | These are deployment identity inputs; inferring them could select an unintended identity or trust boundary. |
| `lifecycle.cleanup_timeout` | Expose one shared budget | Store scan release, forced runtime joins and backend session/cursor/disconnect cleanup use one process control, default `2s`. Startup rollback uses the same budget. |
| MongoDB and Search transform retries | Expose adapter-specific controls | `backend.mongodb.retry_attempts` and `backend.search.retry_attempts` default to `5`; `1` prevents a second attempt. MongoDB also exposes `retry_backoff`, default `5ms`; `0s` skips the pause. Search has no backoff setting. Only confirmed conflicts or aborts allow reevaluation; MongoDB uses the same bound separately to confirm an existing commit without replaying its writes. |
| Search PIT retention and idle HTTP connections | Expose | `backend.search.pit_keep_alive` defaults to `60s`, with whole milliseconds and a minimum of `1ms`. `backend.search.idle_timeout` defaults to `30s`; `0s` disables idle connection expiry. |
| CLI probe deadline | Expose `weir probe --timeout` | The loopback health check has a separate default `750ms` deadline. |
| MongoDB connection creation workers | Retain | `backend.mongodb.max_connecting` controls connection establishment bursts. The native default of `2` is shared by adapter assembly and effective inspection. |

Some fixed values define the supported execution contract and cannot safely be
treated as independent tuning controls:

| Boundary | Fixed behavior and rationale |
| --- | --- |
| Peer exchange timing | Each direction anchors received lifetimes to its own local pre-send time. Forwarding cannot extend a lease, and the protocol needs no fixed transit deduction or global lease ceiling. Announcement lifetime and synchronization deadlines remain explicit process policy. |
| Lua source and bytecode | The pinned Lua parser allows `200` syntax nesting levels and `200` local variables per function. The compiler permits `255` registers and `255` upvalues per function, constrained by instruction encoding. These are language implementation boundaries, not hidden workload budgets. |
| Lua metamethod chains | The VM native limit is `2000`. The sandbox does not expose `setmetatable` or `getmetatable`; client scripts cannot construct arbitrary metamethod chains. Adding a knob for that unreachable mechanism would create ineffective control. |
| Lua garbage collection | Lua-triggered collection and instruction-triggered global collection are disabled; Go owns process collection. Scripts cannot call `collectgarbage`. |
| Backend TCP keepalive | Explicit Weir `30s` overrides are removed. Go networking and the operating system own native TCP keepalive; Weir business progress deadlines and Search HTTP idle expiry have their own exposed controls. |
| MongoDB semantics | The adapter uses direct primary access, majority acknowledgement and no native automatic read/write replay. Pools have no Weir active-connection cap. These choices preserve completion evidence and caller-owned operation deadlines. |

Automatic derivation and fixed protocol constraints must stay visible in this
document or effective inspection. A new threshold that changes workload admission,
operation size, execution time or progress policy needs an explicit configuration
field unless it can be derived from an existing control or a backend-advertised
protocol boundary.
