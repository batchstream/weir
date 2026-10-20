# Configuration

Weir loads process settings and local Store definitions separately. Copy the
[process reference](../config/weir.reference.yaml) to `weir.yaml` and the
[Store reference](../config/routes.reference.yaml) to `routes.yaml`, then edit
the addresses, backend identities and authentication sources.

```sh
weir check --config weir.yaml --routes routes.yaml --effective
weir serve --config weir.yaml --routes routes.yaml
```

## Loading and validation

The CLI defaults to `--config ./weir.yaml` and no Store document. Both CLI paths
resolve from the process working directory. An omitted `--routes` opens no backend;
a node without local Stores can still participate in discovery. Changes require
a restart.

Each file contains one YAML mapping with exact lowercase keys. Unknown or
duplicate keys, multiple documents, anchors, aliases, merge keys and explicit tags
are rejected. Counts use integers, flags use booleans, and text, sizes and
durations use strings. Null is not omission. Process fields and Store entries
cannot be null; optional backend, authentication and TLS blocks can be null.

Durations require units such as `ms`, `s`, `m` or `h` and must fit a signed 64-bit
nanosecond count. Byte sizes use integer `B`, `KiB`, `MiB` or `GiB` units. Fields
have their own zero-value rules, documented in the references. Strings are literal;
only `discovery.peer_address_env` reads a named environment variable.

File loading, programmatic startup and effective inspection share one defaulting
and validation path. `check` does not bind listeners, resolve DNS or connect to
backends. It resolves explicit credential files but does not read CA files.
`--effective` reports resolved `basic`, `routing` and `automatic` settings, with
authentication and CA paths redacted. It is an inspection report, not a replacement
configuration file.

## Process settings

| Group | Responsibility |
| --- | --- |
| `listeners` | Application and peer gRPC bind addresses; at least one is required |
| `discovery` | Replica group, reachable addresses, bootstrap seeds and lease synchronization |
| `diagnostics` | Independent HTTP health and metrics listener |
| `transport` | Handshake, idle, stall, keepalive and stream publication policy |
| `lifecycle` | Startup, graceful shutdown and cleanup deadlines |
| `overload.memory` | Actual memory-pressure watermarks and sampling cadence |
| `lua` | Instruction, call-depth and stack-slot policy for the in-process VM |

Bind addresses use explicit IP:port. Port zero requests an ephemeral port.
Concrete standalone listeners can advertise their bound addresses automatically.
Wildcard listeners require explicit reachable discovery addresses.
`peer_address_env` supplies a complete peer host:port and is mutually exclusive
with `peer_address`. Advertised targets can be IP or DNS host:port values.

Replicas serving equivalent Stores share one `discovery.group`. Seeds are peer
bootstrap addresses. `discovery.lease`, `sync_interval` and `sync_timeout` default
to `60s`, `1s` and `2s`; the lease must exceed the interval plus two exchange
timeouts and use whole milliseconds. See [discovery](discovery-design.md).

The diagnostics listener defaults to disabled. A non-loopback bind requires
`diagnostics.allow_intranet: true`. Readiness describes Node lifecycle, independent
of Store pressure or backend health.

Business work inherits caller cancellation and deadlines. Connection setup,
transport progress and ordinary detached cleanup have independent deadlines.
`lifecycle.shutdown_timeout` (default `5s`) is the full admitted drain. After it,
`lifecycle.cleanup_timeout` (default `2s`) is one Store retirement budget, not a
fresh timeout for each cleanup and disconnect phase. Forced MongoDB adapter
teardown cancels its immutable lifetime, including already-running and later
cleanup; ordinary caller cancellation alone does not shorten detached cleanup.
Concurrent Store closes share the first close plan and result. Successful close
requires actual execution, publisher and adapter-close joins, while a missed join
or a genuine close error remains a failure. Lua's default
instruction count is unlimited; zero call-depth and stack-slot settings select
the VM defaults. See [Lua](lua.md) for allocation ownership and supported functions.

## Store settings

Each Store has a unique name and exactly one `backend.mongodb` or `backend.search`.
Hosting a Store requires an application listener.

| Group | Responsibility |
| --- | --- |
| `backend` | Connection establishment, exchange targets, authentication and trust roots |
| `backend.mongodb` | URI, target metadata caching, connection creation and confirmed-abort retries |
| `backend.search` | Origin URL, idle connection expiry and confirmed-conflict retries |
| `queue` | Waiting input count and bytes; dispatch returns capacity |
| `batching` | Aggregation count, bytes, write collection wait and active execution limit |

MongoDB uses primary reads, majority-acknowledged typed writes and no automatic
read/write replay. Configure an ordinary nonsharded replica set in either mode:

- **Discovery:** an explicit `replicaSet` name and one to seven IP/DNS seeds, each
  with an explicit port. `directConnection` must be omitted or `false`. The MongoDB
  driver discovers the members and selects the current primary. For example:
  `mongodb://mongo-a:27017,mongo-b:27017,mongo-c:27017/?replicaSet=orders`.
- **Direct:** exactly one explicit endpoint without `replicaSet`;
  `directConnection` must be omitted or `true`. This intentionally isolated mode
  still requires a replica-set member, but does not discover another primary or
  provide failover. For example: `mongodb://127.0.0.1:27028/?directConnection=true`.

The URI is limited to 4096 bytes. Set names contain 1–64 ASCII letters, digits,
underscores or hyphens; this is Weir's accepted configuration grammar, not a
MongoDB naming or membership limit. Seed order is retained; DNS case, IP literals
(including mapped IPv4) and decimal ports are canonicalized before duplicate
checks, without DNS access. IPv6 seeds use brackets, such as
`mongodb://[::1]:27017,127.0.0.1:27018/?replicaSet=orders`.

URI userinfo, database paths, fragments, SRV, mongos/sharding, load balancing,
secondary routing, unknown options and duplicate option names (case-insensitive)
are unsupported. A query requires the `/` separator before `?`. Accepted options
are `replicaSet`, `directConnection`, `serverMonitoringMode=poll`, `authSource`,
`authMechanism=SCRAM-SHA-256` and `tls=true`. Authentication requires separately
configured username/password, explicit SCRAM-SHA-256, `authSource` and TLS;
TLS options require credentials. Custom CAs belong in `backend.tls.ca_file`, not
in the URI. Verification uses each actual member's hostname/IP, not the seed's
name. All discovered addresses must be reachable and covered by their member
certificates when TLS is enabled.

Completed target metadata checks default to 64 cached collections; zero disables
retention while concurrent callers still share pending checks. `max_connecting`
defaults to 2 and controls connection creation **per member pool**, not active
connection count. The seed-list bound is not a global topology or socket cap:
discovered members have their own monitor/data connections.

Primary discovery restores selection for new operations; it does not transparently
migrate Native cursors, sessions or transactions. Caller contexts still bound
selection and execution. Never infer that an interrupted operation was not applied
or resubmit an uncertain mutation merely because a new primary is available.

Search uses one HTTP(S) origin with an explicit port. Its idle timeout defaults
to `30s`; `0s` disables expiry. Each adapter invocation qualifies its distinct
indexes without retaining metadata between invocations.

Both adapters default to five confirmed-conflict/abort attempts. Setting
`retry_attempts: 1` prevents another attempt. MongoDB's `retry_backoff` defaults to
`5ms`; `0s` skips the pause. Possible writes and ambiguous commits are never
replayed. MongoDB may retry confirmation of the same commit separately.

Queue defaults are 1024 operations and 32 MiB. Batch defaults are 128 operations,
4 MiB, `20ms` write collection and 16 active executions. Explicit `max_wait: 0s`
disables collection; explicit `max_in_flight: 0` removes the execution limit.
Byte settings are grouping targets: one legal larger item proceeds alone when
its execution memory fits. Backend-advertised document and command limits still
apply. See [batching](batching.md) for interaction between the controls.

## Authentication and memory

Authentication belongs under `backend.authentication`. Each username/password
has exactly one explicit value or file source; URI userinfo is rejected. Relative
credential paths resolve from the Store document directory. Files contain UTF-8
text and allow one final LF/CRLF. Authentication requires native TLS. A custom
`backend.tls.ca_file` replaces system roots while retaining hostname verification;
relative CA paths resolve from the process working directory.

Backend identity and configuration stay local and never enter the peer directory.
Application and peer listeners require an isolated network.

Memory capacity is discovered from physical host memory, finite process limits
and visible container limits. The shared execution account covers typed, Native,
Lua, ingress and output ownership. Actual memory pressure controls overload;
container/OS limits remain the hard boundary. The CLI also manages the standard
Go soft memory limit unless an explicit runtime policy takes precedence.
See [operations](operations.md) for pressure, recovery and metrics.
