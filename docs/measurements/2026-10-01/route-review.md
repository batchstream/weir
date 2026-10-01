# Independent Route review

PR: https://github.com/batchstream/weir/pull/20

A new Agent, which had not implemented the refactor, started only after PR
creation. It reviewed the published initial head `9a8f30d` and repeated its
regressions against published fix head
`7bc2bd8877ec65215fc967d2bed2201171c90706`. It verified PR/local head equality
and the actual base `d73addf5`. GitHub's full-diff endpoint rejected the unusually
large diff; the reviewer read the local Git diff and source after verifying the
same commit identities. It made no implementation edits.

## Confirmed findings and closure

| Severity | Initial location | Trigger and impact | Fix and independent evidence |
| --- | --- | --- | --- |
| P2 | `internal/protocol/protocol.go:388-394` | Search primary application evidence plus replica acknowledgement failure produces APPLIED with a typed Failure. The validator rejected it, ending Route with Internal and losing positive evidence. | Accept APPLIED with a valid typed failure. The original failing independent validator test passes with race detection ten times; a production two-relay regression preserves both the evidence and the following write. |
| P2 | `routeclient/client.go:349-352` | An APPLIED Result is validated, then a request end or final transport success is lost. Record returned nil/error and discarded the known result. | Return the validated Result alongside the RPC error. The original real-gRPC peer regression passes with race detection ten times; incomplete or invalid business data is still rejected. The basic example reports received evidence before the transport error. |
| P2 | `internal/backend/mongodb/native.go:115`, `internal/store/runtime.go:455-458` | All singleton plans bypassed BackendTimeout. A Native MongoDB command stalled for 3 seconds completed despite the default 2-second budget and a longer caller deadline. | Native adapters enforce backend I/O budgets separately from response publication. Only Streaming plans bypass the scheduler batch deadline; Lua and scan execution retain it. The original independent MongoDB fault test now passes. |

The reviewer reproduced every finding before its correction. All confirmed
findings are closed; the fix-head review found no remaining P0–P3 issues.

## Final independent checks of the execution fixes

- Protocol, SDK evidence and Search I/O overlays: race detector, ten repetitions
  each, passed. Search I/O time accumulates across reads; blocked output does not
  consume or reset the allowance.
- Seven core packages: complete race tests passed. The deterministic CGO-disabled
  early-rejection test passed 100 times and retained authoritative gRPC status.
- Real MongoDB native timeouts: configured 100 ms count/qualification stopped in
  100.76/100.68 ms; default 2-second count stopped in 2.00053 seconds. Retained
  tasks/bytes and owned adapter connections returned to zero.
- Real Lua database timeouts: blocked Find returned NOT_APPLIED in 112.61 ms;
  blocked commit acknowledgement returned UNKNOWN in 100.44 ms. Independent
  database readback confirmed exactly one increment. No write was replayed.
- Real MongoDB, Elasticsearch and OpenSearch Route tests passed independently:
  exact 2 MiB records through two relays, Lua/scan/partial writes where supported,
  and record/native acknowledgement loss with persisted-data checks.

The original independent overlays are separate from implementation tests. Root
also reran complete offline Go tests/race, ordinary/integration vet and Darwin/
Linux integration-tagged compilation after the fixes. Actual backend tests are
explicit integration runs; compilation is not execution evidence.

## Independent slow-consumer rerun

The reviewer reran the actual 200/800 MiB memory acceptance after the fixes on
`7bc2bd8`. [Raw independent measurements](route-memory-independent.json) preserve
the generated report. One RPC and fixed 2 MiB records traverse two forwarding
nodes in separate processes; consumption pauses and then proceeds at 1 ms/frame.

| Measurement | 100 records / 200 MiB | 400 records / 800 MiB |
| --- | ---: | ---: |
| Time | 4.962 s | 18.985 s |
| Frames / failures | 3,400 / 0 | 13,600 / 0 |
| Entry / middle / executor peak heap, MiB | 15.81 / 17.66 / 42.17 | 19.59 / 17.53 / 42.25 |
| Entry / middle / executor peak RSS, MiB | 38.30 / 40.81 / 71.42 | 43.59 / 43.23 / 79.86 |

Every byte and request completion was verified. After GC, heap was below 1 MiB
per process, and every RPC, outstanding-ID, queue, retained-result, workspace and
publisher occupancy returned to zero. Relays retained one reusable connection.
The same record size and active RPC count with four times the total volume did
not produce linear retained-memory growth. Samples and post-GC object counts are
reported separately from active ownership counters.

The final delivery commit adds this report, raw evidence and the basic example's
evidence-aware error message. It is checked separately after publication; its
commit identity and final CI result are recorded in the PR description, avoiding
a self-referential commit hash in this file.

## Evidence limits

The 200/800 MiB memory acceptance uses a deterministic executor adapter fixture
and real TCP/HTTP2/gRPC in three OS processes. Actual database 2 MiB tests separately
validate backend encoders. These checks do not establish a hard RSS ceiling or
200 MiB behavior for arbitrary Lua programs. The in-process Lua VM has no hard
allocation quota and requires trusted programs. Process budgets are admission
accounting. The reviewer did not establish a historical performance comparison,
production behavior, or the omitted artifact/Kubernetes profiles.
