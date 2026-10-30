# Configuration

A Weir process reads one YAML file. Copy the
[reference](../config/weir.reference.yaml) to `weir.yaml`, then edit the
listeners, Stores and authentication sources.

```sh
weir check --config weir.yaml
weir serve --config weir.yaml
```

## Loading and validation

`--config` defaults to `./weir.yaml` and resolves from the process working
directory. Relative credential and CA paths inside the file resolve from the
file's own directory. Changes require a restart.

The file is one YAML document. Unknown and duplicate keys and null Store entries
are rejected. Counts are YAML integers. Durations require units such as `ms`, `s`
or `m`; byte sizes use integer `B`, `KiB`, `MiB` or `GiB` units. Strings are literal; no field reads environment variables.
Omitted fields and zero values select the defaults.

`check` runs the same validation as `serve` without binding listeners, resolving
DNS or connecting to backends. It validates the whole file before reading
credential files, and does not read CA files.
Validation errors never include configuration values.

## Process settings

| Field | Default | Behavior |
| --- | --- | --- |
| `listeners.application` | required | gRPC StoreService address |
| `listeners.diagnostics` | disabled | HTTP `/livez`, `/readyz` and `/metrics` address |
| `shutdown_timeout` | `5s` | Drain of admitted work after SIGTERM |

Listener addresses are explicit `IP:port` values; port zero requests an
ephemeral port. The two listeners must not overlap. Bind diagnostics to a
non-loopback address only on a trusted network. Readiness describes Node
lifecycle, independent of Store pressure or backend health.

Business work inherits caller cancellation and deadlines. After
`shutdown_timeout`, each Store gets a fixed 2s to cancel remaining work, end
MongoDB sessions and close connections. Work that does not stop within it is
reported as a shutdown failure. The same 2s bounds detached MongoDB transaction
abort and session cleanup after a caller cancels. Startup, including backend
qualification and listener binding, has 30s.

## Store settings

Each Store has a unique name of lowercase letters, digits and internal hyphens,
and exactly one `mongodb` or `search` backend.

| Field | Default | Behavior |
| --- | --- | --- |
| `mongodb.uri` / `search.url` | required | Backend address |
| `authentication` | none | Username/password values or files |
| `tls.ca_file` | system roots | Custom CA bundle |
| `batching.max_operations` | 128 | Records per backend execution |
| `batching.max_bytes` | 4 MiB | Request bytes per execution; one larger record runs alone |
| `batching.max_wait` | `1ms` | Write collection window; explicit `0s` executes immediately |
| `batching.max_in_flight` | 16 | Concurrent backend executions per Store |

The waiting queue holds 1024 operations / 32 MiB, and Native commands run in
their own pool of 64 per Store. See [batching](batching.md) for how these limits
interact.

### MongoDB

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
underscores or hyphens. Seed order is retained; DNS case, IP literals (including
mapped IPv4) and decimal ports are canonicalized before duplicate checks, without
DNS access. IPv6 seeds use brackets, such as
`mongodb://[::1]:27017,127.0.0.1:27018/?replicaSet=orders`.

URI userinfo, database paths, fragments, SRV, mongos/sharding, load balancing,
secondary routing, unknown options and duplicate option names (case-insensitive)
are unsupported. A query requires the `/` separator before `?`. Accepted options
are `replicaSet`, `directConnection`, `serverMonitoringMode=poll`, `authSource`,
`authMechanism=SCRAM-SHA-256` and `tls=true`. Authentication requires the
`authentication` block, explicit SCRAM-SHA-256, `authSource` and TLS; TLS options
require credentials. Custom CAs belong in `tls.ca_file`, not in the URI.
Verification uses each actual member's hostname/IP, not the seed's name. OCSP
follows the MongoDB driver: a stapled or responder answer of revoked fails the
connection, while an unreachable responder does not. All
discovered addresses must be reachable and covered by their member certificates
when TLS is enabled.

Replica-set clients poll topology every second; direct mode keeps the driver
default. Each member pool creates at most two connections at a time. Primary
discovery restores selection for new operations; it does not transparently
migrate Native cursors, sessions or transactions. Never infer that an
interrupted operation was not applied or resubmit an uncertain mutation merely
because a new primary is available.

### Search

Search uses one `http://` or `https://` origin with an explicit port and no
userinfo, path, query or fragment. Authentication or a custom CA requires HTTPS.
Idle backend connections close after 30s. Each adapter invocation qualifies its
distinct indexes without retaining metadata between invocations.

Both adapters make up to five attempts after a confirmed conflict or abort.
Possible writes and ambiguous commits are never replayed. MongoDB may retry
confirmation of the same commit separately.

## Authentication

Each username and password comes from exactly one explicit value or file; URI
userinfo is rejected. Files contain UTF-8 text and allow one final LF/CRLF.
Authentication requires TLS to the backend. A custom `tls.ca_file` replaces the
system roots while retaining hostname verification.

Backend identity and configuration stay local to each process. The application
listener requires an isolated network.
