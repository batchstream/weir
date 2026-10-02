# Route verification coverage

Coverage spans direct discovery, Route envelopes, the shared scheduler, typed
adapter Events and private backend conformance suites. Relay-only tests are
retired because peers no longer carry business traffic. Local transport and
backend execution coverage remains applicable. A test appearing below is a coverage pointer,
not a claim that its integration profile was executed in this change. Actual runs
and measurements are recorded separately in the PR verification report.

| Previous acceptance concern | Current executable coverage |
| --- | --- |
| Point read/write, backend failures, missing records | `route_acceptance_test.go`, `routeclient/client_test.go`, MongoDB/Search record and batch conformance suites |
| Large valid documents and request half-close | `TestRouteAcceptanceLargeResponseAndHalfClose`; `TestRunConsumesFragmentedFiniteBatch` |
| Destination mismatch, illegal IDs, malformed payload, no rollback of earlier writes | `TestRouteAcceptanceProtocolFailureDoesNotRollback`; `TestRouteEnvelopeIDsDestinationsAndFragments`; `TestCallVersionUnknownFieldsAndRelativeTarget` |
| Missing/duplicate terminal response, unknown ID, non-OK after terminal | `TestRecordRejectsIncompleteAndInvalidResponses` |
| Completion order and same-record execution order | `TestRouteAcceptanceIndependentCompletionAndSameRecordOrder`; `TestSameStreamOrderIndependentReadAndCancellation`; `TestMixedBatchRespectsBytesKeysAndSessionOrder` |
| Cross-RPC batching and cancellation isolation | `TestRouteAcceptanceCrossRPCBatchCancellationIsolation`; `TestAbandonAndSessionCloseCancelFuturePhasesOnly`; adapter cancellation conformance suites |
| Peer convergence, conflict, lease expiry and bounded atomic directory exchanges | `TestPeriodicSyncConvergesAcrossPeersAndStops`; `TestRelayLeaseCannotRenewOrResurrect`; `TestSameGroupUnionConflictAndWithdrawal`; `TestDirectoryBoundsAndAtomicValidation` |
| Direct streams remain pinned; lost writes are never replayed | `TestClientRefreshChangesGroupWithoutReplayingActiveStream`; `TestDiscoveredWriteLossIsNeverReplayed`; `TestIndependentWeirProcesses` |
| Slow consumer, bounded queued work, byte accounting and shutdown | `TestRouteSlowConsumerBackpressureAndShutdown`; `TestUnifiedStreamingBackpressureAndReservation`; `TestAllCallKindsShareWorkingSetAdmission`; `TestSlowConsumerRetainedBound` |
| At least 200 MiB valid direct responses; scaling total record count; heap/RSS/object recovery | `TestRouteMemory200MiBDirect`, with an independent executor process and 100/400 records of 2 MiB each |
| Graceful drain must preserve already admitted backend execution | `TestRouteGracefulDrainPreservesAdmittedExecution`; `TestCLISignalDrainsInflight`; Store integration drain tests |
| HTTP/2 preface, header, partial gRPC frame and send stalls | Retained `plaintext_test.go`, `http_transport_test.go`, and new Route slow-consumer tests |
| Receive buffering cannot eagerly drain unlimited request bodies | Retained credited-body tests for read-ahead, short-read refunds, cancellation and Close interruption |
| HTTP completion and gRPC completion jointly own session credits | Retained `TestDeliverySlotRequiresBothCompletions`; connection-bound single-close test |
| Portable endpoint validation, direct balancing, socket ownership and cleanup | `TestPortableAddressValidation`; `TestOpenResolvesMultipleStoresAndBalancesDirectStreams`; `TestConnectionBoundAndSingleClose`; `TestOpenCancellationAndCloseJoinDiscovery` |
| DNS A/AAAA, answer bounds, active refresh, replacement and deadline | `TestClientDNSDiscoversScaleAndDrainsRetiredReplica`; `internal/netlimit/dns_test.go`; `TestEndpointDNSAcrossProcesses` |
| Record batch item errors, typed ID correspondence, write concern and acknowledged-reply loss | MongoDB/Search batch, fault and runtime integration suites, retained after replacing execution DTOs |
| Native request exact bounds, errors and response loss | MongoDB/Search native conformance suites; native requests now enter the same Route scheduler with one complete bounded input Call |
| Finite Scan pages, cross-instance continuation, mixed BSON IDs, PIT expiry, cancellation and cleanup | MongoDB/Search scan conformance suites; Route continuation tests; unified Store streaming reservation/cancellation tests |
| Lua in the main process, transaction/CAS boundaries and ambiguous commit | `internal/luaengine` tests, MongoDB program commit/transaction tests, Search conditional-write program tests, adapter Route preparation tests |
| Application assembly, TLS/authentication configuration, startup cleanup, diagnostics and process lifecycle | Existing application and CLI tests migrated to Route; private backend connection qualification/TLS suites retained |

The new protocol intentionally removes Native's fragmented upload and early reply
before upload completion: one complete Native input body is capped at 8 MiB inside
a Call capped at 9 MiB. It keeps backend-native response streaming and explicit
Native completion evidence. The deleted tests that required the old fragmented
upload/Open protocol are no longer applicable. Native remains on the same bounded
scheduler and result channel as records and scans.

Scanning returns finite pages of at most 256 documents, fetching one document per
backend step. Each document remains at most 2 MiB and each response fragment at
most 64 KiB. A successful page requires a valid ScanEnd Event with explicit
exhaustion or a continuation token, matching page document count, the outer
request end, and the final gRPC OK status. The next page starts a new Route RPC;
its token does not depend on the previous Weir instance. Native similarly requires its NativeEnd
Event and complete outer transport termination. These business terminal Events
are not acknowledgements that the caller received or consumed bytes.

Default tests own only loopback fixtures and require no production backend.
Actual MongoDB/Search suites require explicit integration build tags and their
isolated fixture profiles. A successful tagged compilation is not live backend
validation.
