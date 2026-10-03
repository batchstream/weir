# Go SDK repository migration

The client is maintained in [weir-go](https://github.com/batchstream/weir-go),
module `github.com/batchstream/weir-go`, package `weir`. The initial SDK migration source
is `a61189910fea1dc6e4dd9d621ba661e2f5267a53`. The server removes its
`weirclient` package and Go client examples. Existing server acceptance tests use
the external SDK; `cmd/weir` has no SDK package in its production dependency graph.

The initial extraction left a module cycle: the SDK consumed this server's public
API packages while server acceptance tests consumed the SDK. Checking only the
production package closure did not detect that cycle.

The canonical public schemas, generated bindings and shared protocol/DNS helpers
now live in [weir-protocol](https://github.com/batchstream/weir-protocol), module
`github.com/batchstream/weir-protocol`. Their implementations and contract tests
are preserved. The dependency direction is Weir → SDK → public protocol, with
Weir also importing public protocol directly. The protocol module depends on
neither SDK nor server, and the SDK has no server module dependency. Peer schemas,
directory state and backend adapters remain internal to Weir. Each module pins
stable releases (SDK v0.2.0 and protocol v0.1.0) and builds without a local replace or committed workspace.
CI checks module edges as well as production imports to prevent another cycle.

The SDK replaces its retired five-RPC API with ResolveStore initialization and
typed Read, Create, Put, Replace, Delete, AtomicTransform, Scan and Native methods.
Finite Execute batches use opaque SDK Commands and SDK result/event DTOs.
Callers never assemble protobuf oneofs or set protocol versions. Clients send Store-relative targets;
CLI examples parse full Store URIs before building Commands. Fixed-owner workload
examples verify discovery and execute on the specified owner connection.

All three modules are verified independently with `GOWORK=off`, published
dependency revisions and network-disabled default/race tests. The server also
passes the CGO-disabled suite and normal/integration vet and tagged compilation
with the published SDK dependency. Module graph checks reject direct and
transitive reverse edges, including unused and test requirements.

SDK race tests, normal/integration vet, tagged compilation, dependency boundary
checks and Python helper regressions pass with `GOWORK=off`. A separate consumer
module installs SDK v0.2.0 and executes a public-API smoke check.
The real ingress regression remains in `internal/server`: connection budgets of
1 and 16 support initialization and public reads beyond the initial two-second
TTL without borrowing an extra connection. SDK fairness regressions explicitly
control real ResolveStore admission and round deadlines, rather than combining
short TTLs with large business-response throughput. A dual-core race/coverage
stress run passes ten consecutive iterations, including failure priority, actual
renewal/reads and cancellation/join.

The initial migration's task-owned loopback MongoDB 8.0.32 and Elasticsearch 8.19.22 profiles passed under
race. SDK lifecycle coverage includes initialization, create/duplicate precondition,
replace/put/transform, persisted read, Complete-gated mixed Execute, ScanPage,
Native Execute and delete. Server backend acceptance covers 2 MiB documents,
Lua/partial outcomes, reply-loss non-replay and cross-instance scan continuation.
Multi-process profiles cover nonowner initialization, direct persistence, instance
replacement, DNS changes, stalled peer withdrawal and SIGTERM readiness/draining.
Python server helpers pass 124 normal tests and 124 optimized tests (30 intentional
assertion-related skips) using a freshly compiled completion fixture.

The typed API was additionally verified against task-owned loopback MongoDB
8.0.32 and Elasticsearch 8.19.22 using SDK source
`77505dd4ceab59ea2ee8c177eeda6bc1998cf015` and protocol v0.1.0. Both tagged
`TestMongoLifecycle` and `TestSearchLifecycle` passed with race detection. These
profiles exercise initialization, create/duplicate preconditions, replace, put,
atomic transforms, persisted reads, Complete-gated mixed Execute batches, Scan,
Native, and delete through the new typed API. They require explicitly provided
local endpoints; default tests do not contact these backends.

The initial extraction did not release tags. The typed API is published as SDK
v0.2.0 against protocol v0.1.0; install it with
`go get github.com/batchstream/weir-go@v0.2.0`. Images and deployments are outside
this SDK change. Historical production
measurements and release records retain their original source/protocol meaning.
Other language SDKs, URI affinity, live Kubernetes/HPA and sustained production
qualification remain outside this change.
