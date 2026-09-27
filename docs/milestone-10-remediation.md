# M10R: Mongo TLS before bounded wire inspection

Date: 2026-09-27. Baseline: clean local `main` at
`26d9e55921813154b6269ead086ad734a11bfc97`. Final commit identity is recorded in
`.testdata/m10r/final-state.log` and the completion handoff (a commit cannot
include its own SHA). This report covers only Go 1.27.0 / mongo-driver v2.9.1 /
MongoDB 8.0.32 on Darwin arm64, one direct non-sharded replica-set endpoint.
The original [M10](milestone-10.md) failed; its fixture-only results are not
retroactively production connection evidence. M10R supplies the missing evidence.

## Implementation and stable contract

`mongostore.Open` is the only adapter construction path, including conformance
integration tests. Static URI validation runs before CA, DNS or driver work.
The two accepted profiles are credential-free intranet Mongo and explicit
SCRAM-SHA-256 with TLS, username/password and authSource. Implicit auth negotiation,
SRV, multiple hosts, path databases, client certificates, other mechanisms,
unknown/duplicate options, retry overrides and insecure flags remain rejected.
The 4096-byte URI and fixed namespace bounds are unchanged. Passwords/CA are
immutable connection configuration; restarting requalifies them.

The order is raw TCP → Go TLS handshake/chain/SAN/expiry/SNI → pinned driver OCSP
verification → bounded decrypted Mongo frames → standard driver SCRAM/operations.
The driver's URI TLS config is retained in the dialer; setting its outer
TLSConfig to nil prevents a second TLS layer. It does not disable validation.
An explicit CA uses the same exclusive root-pool behavior as the driver, with a
bounded read; omission keeps Go's system roots. No custom SCRAM, OCSP parser,
module-cache edit, reflection, unsafe or external production TLS proxy exists.

Read, Put/Create/Replace/Delete, Bulk, Scan, Native and BackendExpression use that
one connection path. Core still receives opaque bytes; no document metadata was
added. Production JSON Decode/Open and direct/peer RPC tests cover every public
operation and missing/conflict/End/EOF/int64 preservation. ProgramTransform remains
UNSUPPORTED. No Weir ingress/peer TLS, authentication or authorization is restored.

## Pinned-source audit

Paths below are relative to `go.mongodb.org/mongo-driver/v2@v2.9.1` (or GOROOT).
These are version-coupled evidence, not an upgrade compatibility promise.

| Source | Verified behavior / retained contract |
| --- | --- |
| `x/mongo/driver/topology/connection.go:264,272` | Custom dialer runs before driver's outer TLS. This was the original layering defect. |
| `topology/connection.go:187–219` | Clone config, set hostname/SNI, standard handshake, then OCSP when certificate verification/revocation checking is enabled. M10R performs this sequence before the guard. |
| `x/mongo/driver/ocsp/ocsp.go:7–14,72–138` | Exported but explicitly unstable API. Verify uses the first verified chain and leaf/issuer, checks staple, then cache/responder; revoked status rejects. Reaudit on every driver upgrade. |
| `ocsp/ocsp.go:150–208,217–334` | Must-Staple, parsed signature/coverage, validity times, delegated signing EKU; responder fanout and unbounded ReadAll require local limits. Complete inconclusive responder data has the driver's soft-failure semantics. |
| `ocsp/cache.go:39–107` | Default map has no lifetime size bound. A fresh cache for one Verify call stores at most one leaf and dies with the handshake. |
| `topology/topology_options.go:147`; `auth/auth.go:33`; `auth/scram.go:95–97` | Explicit URI mechanism selects SCRAM-SHA-256; its Reauth always returns an unsupported error. No default-mechanism fallback can enter through ValidateURI. |
| `x/mongo/driver/operation.go:533–559,691,850–900,952–974,1010–1026` | Ordinary/adaptive retry budgets are disabled. Command code 391 resets/retries only after successful Reauth; SCRAM returns before that. WriteCommandError uses ordinary/adaptive policy, not the 391 reauth branch. Pre-send pool acquisition is distinct from business replay. |
| `operation.go:1093–1110`; `internal/mongostore/rmw.go` | Advancing a physical batch is not retrying it. RMW retains finite new-transaction recomputation versus same-session/transaction commit-only retries, and the deadline-free native-attempt cancellation bridge. |
| Go `crypto/tls/common.go:67–72`, `conn.go:672,1099–1113` | Before our post-handshake certificate limits: plaintext record 16 KiB; TLS 1.3 ciphertext 16 KiB+256; older ciphertext 16 KiB+2048; handshake 64 KiB, certificate handshake message 256 KiB. These library allocations are not claimed to be only our Mongo header size. |
| Go `net/lookup.go:303–377` | Dialer's lookup uses singleflight and can return before its DNS read exits. Per-attempt pure-Go resolver sockets inherit total deadline and close on cancellation; no shared global resolver/cache is introduced. |
| Go `crypto/tls/conn.go:1427–1487` | Close can wait 5 seconds for close_notify. The bounded connection owns raw TCP and closes it directly on failure/driver cancellation; Mongo frame completion is still required for success. |
| `topology/pool.go:1230–1240`; `server.go:840–853` | Driver pool and heartbeat wrap the entire connection (including Mongo hello/SCRAM) in the fixed connect timeout; our nested deadline never extends it. Pool shutdown cancels connection creation. |
| `topology/server.go:747` | Poll monitoring does not activate the streaming RTT monitor. Pool/maxConnecting and monitoring limits remain fixed. |

## Finite limits and explicit additional rejections

| Resource | Bound / owner |
| --- | --- |
| TCP + DNS + TLS + applicable OCSP | One original total 2-second context, shortened by caller; failed raw socket closes. OCSP cannot restart the budget. |
| Mongo selection / pool | 2 seconds; pool configured 1–32 (normal runtime 4), min 0, maxConnecting 2, direct member, poll monitoring. retryReads/writes false, adaptive 0, overload retargeting false, no compression. |
| DNS | Per connection attempt pure-Go resolver; standard A/AAAA, Go's finite DNS message/parser limits; cancellation closes DNS sockets even during a read. No host/DNS changes, SRV or discovery added. |
| CA file | Regular file, at most 256 KiB plus one sentinel byte; invalid/unavailable input yields sanitized failure. The deployment supplies an immutable local regular file. System root loading is Go/OS behavior, not a new CA manager. |
| Presented certificates | At most 8, each DER at most 64 KiB after standard TLS verification; earlier TLS certificate-message bound is 256 KiB. |
| OCSP staple / request state | Staple at most 64 KiB; one leaf cache per handshake. Fixed driver checks leaf status, not a new CRL/intermediate revocation implementation. |
| OCSP endpoints | At most one HTTP URL, at most 2048 bytes, no URL userinfo/fragment, HTTPS or other schemes outside this finite profile. Reject excess even if a good staple exists. |
| OCSP HTTP | Proxy nil (no environment proxy); no redirect, keepalive disabled, one connection per host; header 16 KiB/body 64 KiB+1 detection byte; HTTP/header/connect deadlines bounded by original context. Transport is disposed each verification. |
| OCSP failure policy | Oversize, redirect, I/O/header errors and expiry of context fail closed. This is deliberately stricter than driver soft failure for unavailable responders. A complete bounded malformed/unknown responder response still follows pinned driver soft-failure semantics; malformed/stale/revoked staples reject. No revocation-disable flags. |
| Wire inspection | Unchanged 48 MiB frame, 64 KiB non-document metadata, 4096 container nodes; driver receives zero header bytes on failed guard. Driver may separately allocate another bounded copy. |
| Close / sessions | Existing Adapter Disconnect 2s; raw TCP close avoids an additional TLS alert deadline. Existing Scan/RMW cleanup, runtime admission/result/session limits remain unchanged. |
| Owned fixture | 45s overall startup; admin commands 2s, primary poll 1s calls within 20s; client wait 15s within startup; clients max pool 4/maxConnecting 2. SIGTERM wait 5s, Kill wait another 2s. Registered clients each close within 2s; at most three constructed clients. |

At maxConnecting=2 plus one poll monitor, at most three Mongo connection attempts
per adapter can simultaneously enter TLS/OCSP; each has at most one responder
request and one short-lived leaf cache. Healthy pool connections do not retain
OCSP transports/caches. DNS and Go HTTP helper goroutines are finite per attempt
and are covered by cancellation tests, not claimed as an exact process RSS model.
These are connection bounds, not a multi-instance database-budget qualification.

## Evidence separation

1. **Real direct Mongo:** the generated owned mongod itself uses requireTLS/auth,
   a single-member replica set, and the application user has only find/insert/
   update/remove on `records` and listCollections on its database. hello/buildInfo
   qualification also succeeds. Admin is test-only for bootstrap/failCommand and
   independent read-back; the application URI always uses the minimal role.
2. **Negative production connections:** bad CA, hostname/SAN, wrong/missing
   password and insufficient privilege independently fail. URI/profile sentinel
   and config validation tests verify rejection before CA/DNS side effects.
   Expired certificates, DNS SNI, OCSP good/revoked/stale/malformed/missing
   Must-Staple, responder limits/redirect/stall are separately tested with fresh
   in-memory certificates and loopback standard TLS/OCSP fixtures. Successful
   mongod certificates without OCSP extensions do not prove revocation behavior.
3. **Synthetic encrypted wire boundary:** standard TLS sessions send malformed
   flags/short length, length above 48 MiB, 4097-element top-level writeErrors and
   truncated frames. The same production dialer/guard returns n=0 before the
   driver could receive its first length bytes. This is distinct from Mongo
   business behavior, and plaintext wire unit tests remain.
4. **Real committed effects + fault injection:** the test proxy terminates TLS
   only to observe/decrypt/re-encrypt fault traffic. It forwards SCRAM and commands
   to the real TLS mongod, records command name/session/txn/ack/digest, and drops
   actual successful replies. Production direct-to-mongod tests are separate.
   Ordinary batch insert, Native findAndModify and expression update remain one
   business command. A subsequent production Read reconnects/authenticates;
   SASL continuation counts are reported separately from business counts.
5. **391 evidence:** Mongo failCommand produces command-code 391 for ordinary,
   Native and expression calls. SCRAM never successfully reauthenticates: one
   command, unchanged record; mutation UNKNOWN, Native RESPONSE_INCOMPLETE where
   driver drops raw reply. A separate synthetic writeErrors[391] envelope
   exercises the WriteCommandError path (one command), not an actual server
   write-rejection claim. That synthetic envelope cannot be used as commit proof.
6. **RMW and lifecycle:** real update/replace/delete/recreate/missing-insert
   competition forces fresh transaction/evaluation; lost successful commit replies
   retain one evaluation and identical session+transaction across commit retries.
   Queue/execute/commit/response cancellation and drain, transport stalls,
   cursor/session/ledger release, repeated failures and pool rebuild are inherited
   real suites run on the new profile through Open, not manually built adapters.

Observed initial race evidence: conflict cases used attempts=2/evaluations=2/
commits=1. Single lost commit reply used two wire commits and APPLIED; all lost
replies used two within the 700ms deadline and UNKNOWN, evaluations=1 in both.
Twelve failed opens plus four healthy reopen/closes returned server connections
7→7 and goroutines 17→17. TLS stall tests now include twelve 20ms cancellations
and one hard 2s deadline; DNS cancellation has eight attempts/16 A+AAAA queries,
with goroutines returning 4→4. Final three-round logs are authoritative for exact
per-run timing and counts.

## Reproduction and logs

All logs below are in ignored `.testdata/m10r/`. Owned secure directories are
`.testdata/mongo-m10-<test-process-pid>-<sequence>/`, individually logged with
stopped port. Every successful/failed fixture retains `mongod.log` and owner
marker; only newly created, ownership-confirmed fixture data/key/CA files are
removed. Directory creation failure cannot adopt or clean a historical fixture.
Generated keys/passwords/raw authenticated URIs are never tool-read or printed.

```sh
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=1 -timeout=10m ./... -v
GOPROXY=off GOSUMDB=off go test -race ./internal/mongostore \
  -run '^TestMongo(TLS|DNS)' -count=3 -timeout=45s -v
# Critical real Mongo + peer paths, no concurrent failCommand suites:
GOPROXY=off GOSUMDB=off WEIR_M10_INTEGRATION=1 WEIR_MONGO_PROFILE=tls \
  go test -race -tags integration -p 1 -count=3 -timeout=10m \
  ./internal/mongostore ./internal/server ./internal/app \
  -run 'TestNativeRMW|TestCommit|TestRMW|TestCloseDuringCommit|TestAmbiguity|TestAcknowledgedOrdinary|TestMongoNativeReal|TestMongoExpression(ReplyLoss|Cancellation|NativeCompetition)|TestMongoSCRAMTLS|TestPeerRealAcknowledged|TestMongoTLSApplication' -v
scripts/mongo-local.sh start
GOPROXY=off GOSUMDB=off WEIR_INTEGRATION=1 \
  go test -race -tags integration -p 1 -count=1 -timeout=10m ./... -v
# Then stop all owned backends before final offline validation:
scripts/mongo-local.sh stop
GOPROXY=off GOSUMDB=off go test -count=1 ./...
GOPROXY=off GOSUMDB=off go test -race -count=1 ./...
GOPROXY=off GOSUMDB=off go vet ./...
GOPROXY=off GOSUMDB=off go vet -tags integration ./...
```

Final command results and supplemental affected Search regressions are recorded
in the completion table below. Last review removed the obsolete URI-auth boolean
and rejected non-regular CA paths before Open; `uri-final.log` and the final
production smoke recheck those changes. Fixture negative-client cleanup also
uses independent bounded contexts.

## Failure history retained

- `first-open.log`: real production TLS Read/write and independent negatives
  succeeded; fixture duplicate Disconnect cleanup failed. Cleanup now tolerates
  ErrClientDisconnected and every constructed client has bounded cleanup.
- `tls-mongo-first.log`: the migrated full suite expected plaintext Native 391's
  retained raw error response. Explicit SCRAM instead exits on Reauth error with
  no raw reply; corrected to RESPONSE_INCOMPLETE without changing one-command
  assertions. Other real Mongo cases passed.
- `tls-boundary-first.log`: test-only stalled HTTP responder did not exit merely
  by waiting on Request.Context; terminated the two owned test processes and
  added a one-second fixture fallback. No backend was owned by that unit run.
- `tls-dns-race3.log`: temporary field rename compilation error; corrected before
  executing the final checks.
- `tls-dns-final-race3.log`: cancellation returned while standard Go DNS socket
  reads lingered. Production per-attempt DNS sockets now inherit connect deadline
  and explicit cancellation close; `tls-dns-cancel-fixed-race3.log` passed, followed
  by final shared Mongo/OCSP raw-dial checks in `tls-boundary-final-race3.log`.
- Earlier successful `tls-mongo-race.log` / `tls-assembly-race.log` preceded final
  DNS/Close tightening; final full/race3 logs below supersede them.

## Completion table

| Check | Result / log |
| --- | --- |
| Full TLS/SCRAM integration race, `-p 1 -count=1` | PASS; `tls-full-final-race.log`, Mongo 140.651s, server 222.709s, app 24.581s, store 19.069s. Search real subtests intentionally skipped in this Mongo run. |
| High-risk real TLS Mongo/peer/application, `-count=3` | PASS; `tls-critical-race3.log`, Mongo 260.712s, server 57.344s, app 8.151s. Every named critical top-level test ran three times. |
| TLS/OCSP/encrypted wire/DNS cancellation, `-count=3` | PASS; `tls-boundary-final-race3.log`, 12.643s; 13 TLS stalls and 8 DNS cancellations per round. |
| Post-review real fixture / production Open / JSON assembly | PASS; `tls-post-review-smoke.log`; `uri-final.log` covers final admission simplification. |
| Entire legacy unauthenticated Mongo integration race | PASS; `legacy-full-race.log`, Mongo 21.152s, server 82.521s, app 9.531s. TLS opt-ins and unrelated Search live tests skip as documented. |
| Affected Elasticsearch 8.17.0 integration race | PASS; `search-elasticsearch-affected-race.log`, server 8.321s, app 28.150s. |
| Affected OpenSearch 2.19.0 integration race | PASS; `search-opensearch-affected-race.log`, server 7.862s, app 27.603s. |
| Backend stop verification | PASS; `stopped-backends.log`, `legacy-stop.log`, `search-*-stop.log`; all three fixed ports free, Mongo absent, both owned containers exited. |
| Default noncached test/race after backend stop | PASS; `stopped-default-test.log`, `stopped-default-race.log`; server 58.731s / 60.665s. |
| Default/integration vet after backend stop | PASS, both exit 0; `stopped-default-vet.log`, `stopped-integration-vet.log`. |
| Style/protocol/dependencies | AST check PASS (`style.log`), diff whitespace clean, no protocol/dependency changes (`unchanged-protocol-dependencies.log`). |

The affected Search run uses `WEIR_INTEGRATION=1` and, sequentially,
`WEIR_SEARCH_INTEGRATION=elasticsearch` / `opensearch`, with:

```sh
go test -race -tags integration -p 1 -count=1 -timeout=5m \
  ./internal/server ./internal/app \
  -run 'TestPeerRealFiveRPCs|TestPeerRealAcknowledgedReplyLossNoReplay|TestPublicExpressionUnaryBulkAndOpaquePeers|TestScanDualStoreIsolation|TestDualStoreRoutesAndSearchBulkOrder|TestIndependentWeirProcesses|TestEndpointIndependentProcessesDistributionReplacement|TestEndpointDNSAcrossProcesses' -v
```

Final cleanup: `fixture-cleanup-final.log` verifies all 509 secure fixture roots
referenced by this stage retained mongod logs and had zero generated CA/key/
password/data paths remaining. Historical roots were not inspected. Final process,
port/container and Git checks are in `resource-cleanup-final.log` and
`final-state.log`. The only deliberately terminated run was the early unit-test
HTTP fixture hang documented above; subsequent final commands all exited zero.

These cover actual ES and OS CRUD/Scan/Native/expression, dual-store behavior,
reply loss, shared fixture selection and independent CLI/DNS/endpoint replacement.
They are affected regressions, not a new full Search production qualification.
No production app/server/Search code changed. Full TLS and legacy runs skip the
standalone DNS-child helper outside its spawned-child environment and Search-only
live cases; a parent PASS with skipped children is not counted as live evidence.


## Remaining scope

This qualification cannot establish system-root success against a public remote
service (no external credentials/trust-store changes used), multi-node failover,
SRV, other auth mechanisms, HTTPS/multiple OCSP responders, client certificates,
Search authentication/TLS, fleet-wide budgets, Linux/Windows native execution,
Kubernetes, throughput/SLO or 24-hour soak. Those remain separately unqualified.
The fixed OCSP API is unstable and all connection/retry tests must run on upgrades.
No push, PR, release, deployment, existing-secret access or automation change was
performed. The coordinator independently reviews this bounded stage.

## Organization observations for the separate follow-up

This is limited to code touched here, not a repository-wide structure audit.
The original adapter and testAdapter separately constructed driver clients; that
let integration tests bypass the production profile. Tests now use Open, with
observation/fault injection in testmongo. Backend TLS/OCSP configuration belongs
beside mongostore's wire boundary; no such state was added to Core or app/server.
Testmongo currently owns real-process startup/credentials and wire fault injection
in separate files, while a test-only database-to-fixture registry lets existing
server/store/app suites select their profile. The many package-local integration
builders and cross-file helper dependencies are an observed organization concern
for a later dedicated cleanup; no directory or public API restructuring is part
of M10R.
