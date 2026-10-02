# Public/peer protocol split validation

This report covers the breaking protocol refactor on
`randy/split-public-peer-protocols`, using Darwin/arm64 Go 1.27.1 and task-owned
loopback MongoDB 8.0.32 / Elasticsearch 8.19.22 fixtures. Production and live
Kubernetes clusters were not contacted. The earlier discovery/resource evidence
is retained in [discovery-validation.md](discovery-validation.md).

The public StoreService schema and internal PeerDiscoveryService schema have
independent descriptor import closures. Contract tests verify RPC paths, stream
directions, field names/numbers/types and absence of replica groups in the public
response. Listener tests reject opposite-role RPCs and all three retired paths.
The production Go client dependency closure contains neither directory nor peer
protocol code. Independently generating and compiling the public schema without
the peer schema succeeds. Repeating `scripts/generate.sh` preserves every generated
Go file byte for byte. Existing Call/Event business DTOs retain their contract.

The complete Go race suite and CGO-disabled suite passed, along with normal and
integration-tagged vet and integration-tagged compilation for every package.
Python helper tests passed against a freshly compiled completion fixture: 124
ordinary tests with no skips and 124 optimized tests with 30 intentional
assertion-related skips. Focused tests retain physical connection-limit 1/16,
bounded refresh fairness, cancellation, TTL/conflicts, finite completion,
backpressure and mutation non-replay coverage. Mapped IPv4 wildcard/multicast
endpoints are now rejected after unmapping, with explicit regression cases.

The following actual profiles passed under race with the new RPCs:

```sh
WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
go test -race -tags=integration -count=1 -timeout=180s ./internal/app \
-run '^Test(IndependentWeirProcesses|EndpointIndependentProcessesDistributionReplacement|EndpointDNSAcrossProcesses|DirectoryWithdrawalDoesNotDelayAdmittedWriteDrain|DiagnosticProcessSIGTERMReadinessBeforeExit)$'

WEIR_INTEGRATION=1 WEIR_SEARCH_INTEGRATION=elasticsearch \
go test -race -tags=integration -count=1 -timeout=180s ./internal/server \
-run '^TestRoute(Mongo2MiBRecordLuaScanAndPartialBatch|MongoAppliedWriteAndNativeReplyLossAreNotReplayed|Search2MiBRecordAndAppliedReplyLoss|MongoScanContinuesOnNewInstanceAfterOriginShutdown|SearchScanContinuesOnNewInstanceAfterOriginShutdown)$'
```

These runs prove peer learning, initialization through a nonowner, persisted
direct MongoDB/Search requests, independent replica withdrawal/restart, IPv4/IPv6
DNS changes, prompt business draining while peer sync stalls, and bounded SIGTERM
readiness transition/exit. Real backend acceptance covers 2 MiB records,
Lua/partial outcomes, acknowledged/reply-loss non-replay and scan continuation on
a replacement instance. Historical test names containing Route remain coverage
names; their executable clients now use StoreService.Execute.

Other language SDKs, URI affinity, live Kubernetes/HPA, OpenSearch, generated
TLS/authentication, production, packaged-image, Linux-cgroup and 200/800 MiB
resource profiles were not executed for this protocol split. Tagged compilation
does not claim those profiles ran. The old protocol and SDK aliases are removed;
configuration file names, CLI flags and transport capacity settings remain outside
the naming changes in this refactor.
