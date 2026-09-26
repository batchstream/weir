# Weir: Clean-Slate Architecture and Protocol

Status: proposed V1 design; milestone 1 implementation explicitly authorized on 2026-09-26.
Later stages still require separate authorization.

This is a new design, not a Sink migration plan. No Sink API, package boundary,
configuration format, deployment role, or storage metadata is a compatibility
constraint. The original design-only deliverable is preserved here. The authorized milestone 1
implementation and qualification evidence are documented in `milestone-1.md`; this
is not approval or completion of all V1 features. `architecture.zh-CN.md` is the
parallel Chinese translation, with matching section and reference identifiers.

## Milestone 1 Qualification Profile (2026-09-26)

The first implementation deliberately narrows the design: one loopback-only gRPC
listener, one Store, one pre-created ordinary collection on a MongoDB 8.0.32
replica set through a single direct connection target (no discovery/failover), raw BSON Read/Put/Create/Replace/Delete, and duplex Bulk. General
AtomicTransform and backend expressions return UNSUPPORTED. Native, Scan, search,
peers, TLS/authentication, control-plane features and deployment are not implemented.
User-supplied BSON must put its explicit matching `_id` first; the codec's documented
supported-type/depth/node profile is enforced rather than accepting lossy values.
All configured limits are qualification starting points, not production recommendations.

Three implementation findings refine, rather than weaken, the contracts below:

- GopherLua v1.1.1 fails per-invocation allocation, compile-cancellation and host-helper
  isolation. It is test-only; finite counter-transform transaction conformance is not
  a replacement general language runtime. Program execution remains disabled.
- MongoDB Go driver v2.9.1 makes commit retries unlimited under deadline/CSOT mode.
  The explicit commit loop preserves the original outer deadline/error and session,
  bridging parent cancellation into a deadline-free native context. The driver's
  socket listener requires Canceled (not DeadlineExceeded without a socket deadline);
  its native retry-once therefore bounds wire attempts without losing cancellation. Real-response-drop tests verify at most
  ten wire commits per logical RMW operation, one session/transaction, and no
  re-evaluation after ambiguity. This pinned-driver dependency requires requalification.
- Qualification correction: commit `37d1454` did not cover unary response sending
  with its server deadline; the earlier overall acceptance conclusion is withdrawn.
  gRPC v1.79.3 can finish a handler/context before queued DATA/trailers are sent.
  The repair uses Go 1.27 HTTP/2 stream write deadlines with the pinned gRPC
  ServeHTTP transport, not handler return or stats.End as proof of delivery.
  Unary input reads also have a progress-sensitive stall bound capped by the original
  deadline; decoding the unary frame ends input-stall accounting, not the lifetime.
  Unary lifetime includes sending; its response-stall budget also covers encoding,
  DATA and trailers. Native stream expiry resets the stream without requiring a
  client read. Bulk's existing input/send watchdog and connection write timeout
  may still close a connection, truncating co-resident RPCs. Missing mutation
  results remain UNKNOWN. See `unary-response-deadline.md` for reproduction and tests.
  ServeHTTP's eager request-body reader is explicitly credit-limited to one maximum
  gRPC frame, returning credits only for decoded messages; it cannot bypass Bulk
  backpressure. The ServeHTTP API is experimental and this pinned profile requires
  transport requalification when Go or gRPC changes.

The smaller initial layout uses `api/weir/v1`, `internal/protocol`, `internal/store`,
`internal/server`, `internal/mongostore`, and `internal/value`. StoreRuntime directly
owns the sole concrete adapter and compares only its opaque plan metadata; it never
imports BSON or inspects fields. No single-implementation interface is added just to
match the future layout in section 16. Extract a real adapter boundary when a second
implementation is actually authorized. Admission reserves conservative worst-case
result credits earlier than required (at admission, before dispatch).

## Decision Summary

Weir is a composable, synchronous storage data plane. One `weir` binary hosts
configured listeners, routes, local Stores, and remote Weir Services. Its primary
execution path is:

```text
Application
    |
Listener
    |
Route (logical Store identity only)
    |
Service
    +-- LocalStore --> StoreRuntime --> Scheduler --> Adapter --> Database
    |
    +-- RemoteWeir --> another Weir listener --> the same execution path
```

The cross-cutting invariants are **bounded resources, backend-native atomicity,
opaque documents, explicit mutation outcomes, and streaming backpressure**.

Decisions that intentionally differ from Sink:

- No Gateway, Engine, Worker, asynchronous acceptance, or durable delivery.
- One logical RPC/stream targets one Store and one Service; no Store fanout.
- One scheduler and one pending-work ledger per local Store, not one per RPC kind.
- Documents are media-type-tagged bytes. Only adapters and transform codecs decode.
- Record mutations expose execution evidence, not a `retryable` Boolean.
- MongoDB general transforms use transactions, never hidden revision fields.
- No portable query, count, revision, visibility, or durability abstraction.
- Four semantic operations: Read, Mutate, Native, Scan. A fifth RPC, Bulk, is only
  bounded framing for Read/Mutate, not another execution path.
- V1 Scan is a live, backpressured traversal, with cursor lifetime confined to one
  RPC. Cross-RPC resume tokens and portable snapshot promises are deliberately absent.
- Public and authenticated peer listeners use the same `weir.v1` RPCs. Forwarding
  adds trusted hop metadata, not another message protocol.
- Same-key ordering is scoped to one Bulk stream; independent Reads are not serialized.
- Scan cursor lifetime is bounded separately from backend fetch concurrency; Cmin is 1.
- Native reports response completeness, not normalized database mutation effects.
- Static validated configuration, immutable assembly, rolling restart for changes.

## 1. Scope, Principles, and Guarantees

### 1.1 Product boundary

Weir controls the Application -> Database path: routing, connection fan-in,
bounded admission and scheduling, micro-batching, local sequencing, adaptive
concurrency, overload protection, backend execution, atomic transforms, native
access, streaming, observability, and shutdown.

It is not a queue, delivery platform, ETL system, ORM, schema/index manager,
universal query language, SQL layer, cross-record transaction coordinator,
distributed lock service, workflow engine, service mesh, exactly-once processor,
or transparent database protocol tunnel. Applications may place their own queue
and consumers upstream; Weir neither knows nor manages that system.

### 1.2 Semantic guarantees

1. A recognized, admitted record operation has exactly one local terminal result.
   Delivery of that result is not guaranteed after transport failure.
2. A successful record mutation has the adapter's documented single-record
   atomicity. A Bulk stream or physical database batch is never a transaction.
3. No component automatically starts a new mutation attempt after the previous
   attempt might have applied. Backend commit resolution of the *same* transaction
   is not a new mutation attempt.
4. Routing, scheduling, accounting, forwarding, and congestion control do not
   inspect document fields or depend on BSON/JSON.
5. Every input, pending-work set, active execution set, retained result set,
   transform working set, and transport buffer has a finite bound.
6. Bulk preserves same-record frame order within that live stream. Independent
   calls have no shared ordering guarantee; endpoint affinity is only an optimization.
7. User documents contain no Weir-owned fields; no sidecar revision collection or
   durable deduplication ledger substitutes for hidden fields.

These are not global linearizability, global ordering, exact cluster concurrency,
or exactly-once delivery guarantees. Read consistency, acknowledgement durability,
and search visibility remain documented backend/Store properties. An acknowledged
write can still be lost under a backend's weak durability settings. V1 rejects
unacknowledged record writes because they cannot produce useful applied evidence.

### 1.3 Isolation and trust boundary

Each configured local Store has a separate scheduler, pending limits, adaptive
window, batch state, adapter-owned client/pool, and bounded transform cache. A
slow Store does not borrow another Store's queue or connections. Stores still
share process CPU, memory, network, and the OS; this is resource isolation, not
hard security or CPU-reservation isolation. Deploy separate processes for hard
tenant isolation.

TLS, authentication, and Store-level authorization are listener concerns. A
Store uses configured backend credentials, never credentials supplied in a
document or native URL. Core authorization is by Store and semantic operation
family, including each Read/Mutate inside Bulk. Backend command restrictions
belong to the adapter. Arbitrary field-level authorization and per-request
backend impersonation are outside V1.

## 2. Listener, Route, Service, and Assembly

### 2.1 Listener

Use gRPC, Protobuf, and HTTP/2. Application and authenticated peer listeners
expose the same `weir.v1` service and message schemas. V1 uses separate
listeners so their trust policies are explicit: public ingress rejects reserved
forwarding metadata; peer ingress requires an authenticated Weir identity and
valid hop metadata. Sharing a schema does not share authorization. There is no
mesh-specific RPC package.

Listeners own frame/metadata limits, transport connection limits, authentication,
deadline establishment, protocol validation, correlation, and bounded result
delivery. They do not implement storage semantics or own a work queue.

Use bounded connections and HTTP/2 concurrent streams *before* application
interceptors. A unary interceptor runs too late to be the only protection
against request decoding allocations. Bound both public and peer transports and
disable compression in V1; limits apply to the actual Protobuf message, not
compressed wire size. Reject excess application RPCs immediately rather than
queueing handlers.

### 2.2 Route and Service

A route is an exact `Store name -> Service reference` map. No regex routing,
resource-field inspection, longest-prefix policy, route rewriting, or fallback
Store is needed. Reject unknown Stores. There are two concrete Service variants:

| Variant | Owns | Does not own |
| --- | --- | --- |
| LocalStore | Reference to its one StoreRuntime | Another queue or execution algorithm |
| RemoteWeir | Bounded peer connections, endpoint selection, stream forwarding | Database pool, record scheduler, micro-batcher, adaptive DB controller |

Service is a small dispatch boundary with two real implementations, not a
dynamic plugin framework. Public and peer handlers both invoke that boundary. Do
not implement a parallel local fast path that bypasses admission when batching
is disabled; disabling batching only makes physical batches singleton.

### 2.3 One-request-one-Service rule

Read/Mutate/Native/Scan each contain one resource. Bulk begins with one canonical
Store-root URI; all operation resources must name that Store. Each node resolves
the stream once and pins the resulting Service and remote endpoint for its lifetime.
No request can switch Store, Service, endpoint, credentials, or route after execution
begins. Multiple physical batches within that Service are allowed.

A wrong-Store Bulk operation receives `INVALID_ARGUMENT + NOT_STARTED` without
execution; it does not undo earlier results. An invalid stream envelope terminates
the stream. Clients must not infer that unreported earlier operations failed.

### 2.4 Configuration and topology

Startup is: decode strict configuration -> validate the entire graph -> construct
immutable Services and StoreRuntimes -> open listeners -> become ready. Assembly
has one owner and unwinds already-created resources on partial failure. No global
registries, runtime providers, route watchers, or configuration generation protocol.

Conceptual configuration, not a committed configuration-file schema:

| Deployment | Routes in the same `weir` binary |
| --- | --- |
| Single MongoDB node | `mongo -> LocalStore(mongo-runtime)` |
| Single search node | `search -> LocalStore(search-runtime)` |
| One node, two backends | `mongo -> LocalStore(M)`; `search -> LocalStore(S)` |
| Two-hop | A: `mongo -> RemoteWeir(B)`; B: `mongo -> LocalStore(M)` |
| Mixed local/remote | A: `mongo -> LocalStore(M)`; `search -> RemoteWeir(B)` |

Multiple names must not alias the same local StoreRuntime in V1. Independent Stores
may deliberately target the same database, but then they have independent scheduling
and there is no shared ordering domain. Document this configuration hazard.

Each local Store has a bounded reusable driver pool. Applications primarily share
gRPC connections to Weir instead of multiplying database pools per application
process. This is connection fan-in, not an exact cluster-wide connection ceiling.
Account for driver monitoring connections and replica topology separately.

## 3. Canonical Resources and Opaque Data

### 3.1 URI rules

Canonical form is `weir://<store>[/<adapter-defined-path>]`.

Core enforces syntax, byte bounds, and extraction of Store; adapters enforce path
grammar and canonical backend identity. Core never infers collection/index names.
The protocol package must specify canonicalization independently of a language's
URL library:

- Scheme is exactly lowercase `weir`. Store is 1-63 lowercase ASCII characters,
  starts with a letter, and then uses letters, digits, or single hyphens; no final
  hyphen. No userinfo, ports, query, fragment, authority aliases, or implicit Store.
- Root has no trailing slash. Non-root paths have no empty or dot segments.
- Split on literal `/` first; decode each segment exactly once. Percent-encoded
  slash is segment data, never a second path separator.
- Segment text is valid UTF-8. Preserve Unicode code points exactly; do not normalize
  two differently spelled backend identifiers into one. Reject control characters.
- Emit unreserved ASCII and `:` literally; all other segment bytes use uppercase
  percent escapes. Reject escapes for bytes that must be literal and reject lowercase
  escapes. A parser accepts a spelling only if canonical re-encoding is identical.
- Adapters perform parse/format round-trip validation for typed keys. They must not
  admit two key spellings that compare equal under the backend's identity semantics.

Example adapter grammars:

| Adapter | Root/dataset/record examples | V1 identity restrictions |
| --- | --- | --- |
| MongoDB | `weir://mongo`, `weir://mongo/catalog`, `weir://mongo/catalog/products`, `weir://mongo/catalog/products/s:123` | String, ObjectId (`oid:` plus 24 lowercase hex), and canonical signed integral (`i:`) keys; numeric BSON widths share the one integral identity. No arbitrary object/array/decimal/nonintegral numeric IDs in record APIs. |
| Elasticsearch/OpenSearch | `weir://search/products/s:123` | Concrete index and exact string ID; default backend routing only. No alias, wildcard, data-stream rollover target, numeric-to-string convenience alias, or hidden custom routing in record APIs. |

Adapters validate case/collation rules and reject unsupported identity forms rather
than guessing. MongoDB record comparisons use simple identity semantics, not a
caller-selected linguistic collation. More exotic IDs, custom routing, and aliases
remain accessible through authorized Native calls, outside local record ordering.

The full canonical record URI is the record identity. A Bulk sequence key
combines that identity with a server-owned live stream identity, never the
client request ID. Physical BatchKey is a different, adapter-produced token.
Records may share a BatchKey without sharing a record or sequence key. Different
Stores never share a batch. Record guarantees exclude concurrent namespace
destruction/recreation or alias retargeting; those are not competing record
writes.

### 3.2 Document and options representation

`Document = { media_type: string, data: bytes }`. Media type belongs **per Document**,
including transform input and scan/read output. Request inheritance saves little
and creates ambiguous interpretation in mixed-representation streams. Store
capabilities declare allowed representations but do not silently fill missing tags.

Use lowercase `type/subtype`, bounded to 127 ASCII bytes; no parameters, whitespace,
aliases, or content sniffing in V1. Examples are `application/json`,
`application/bson`, and `application/cbor`. Canonical format validation belongs to
the protocol layer, while supported-type validation belongs to the adapter. New
media types do not change a central enum or Core switch.

Core may count document bytes and enforce limits; it may not parse fields.
Adapters may validate native encoding and identity consistency. MongoDB Put/Create/
Replace requires an explicit `_id` consistent with the URI and preserves all other
user fields. Weir does not add an ID, revision, timestamp, or metadata wrapper.
Search `_source` remains the user's JSON; no source field is treated as its ID.

The read representation is the Store's documented default unless `read_media_type`
requests a supported exact representation. No generic transcoder exists. A Store
may reject conversion that would lose backend-native types. SDKs serialize using
the caller's chosen encoder, never JSON-first conversion of an arbitrary struct.

Optional `adapter_options` is another small media-type-tagged opaque value. It is
for operation-specific backend choices such as search refresh behavior, not a
generic query language. Adapters reject unknown options; Core only bounds bytes.
Omitting options uses immutable Store defaults. Authorization cannot be overridden
through these options.

## 4. Execution Outcomes and Retry Policy

### 4.1 Record mutation result

An outcome is evidence about one logical record mutation, independent of the error
category. Every terminal mutation result includes a nonzero outcome:

| Outcome | Exact meaning | Automatic replay |
| --- | --- | --- |
| NOT_STARTED | No backend execution attempt for this operation began. Validation, admission, or queued cancellation stopped it. | Safe in principle; V1 Core does not retry it automatically. |
| NOT_APPLIED | Execution/observation began, but the adapter proves no committed effect from this logical mutation. Includes definite conflicts, aborted transactions, and transform Keep/Delete-of-observed-absence decisions that issue no write. | Only adapter conflict algorithms retry internally; caller policy may retry an eligible error. |
| APPLIED | Adapter has positive backend acknowledgement that the mutation completed under its execution/acknowledgement contract. This does not mean that bytes changed or all readers can see it. | Never replay automatically. |
| UNKNOWN | An effect may have committed and there is no conclusive terminal evidence. | Never replay automatically. |

`Failure` is optional and contains a bounded code/message, not a retry Boolean.
Codes include INVALID_ARGUMENT, UNAUTHENTICATED, PERMISSION_DENIED, NOT_FOUND,
PRECONDITION_FAILED, CONFLICT, UNSUPPORTED, RESOURCE_EXHAUSTED, UNAVAILABLE,
CANCELLED, DEADLINE_EXCEEDED, and INTERNAL. Proto zero/unspecified is invalid for
terminal states. A failure may accompany APPLIED, for example if effect is known
but a separately requested post-write visibility check fails. Never downgrade
known application to NOT_APPLIED because response serialization or delivery failed.

A transform Keep or Delete-of-observed-absence decision that issues no write
uses NOT_APPLIED with no Failure. Create-existing and Replace-absent are
NOT_APPLIED with PRECONDITION_FAILED. A missing Read is an ordinary `missing`
result. An acknowledged ordinary Delete is APPLIED even when the record was
already absent: it completed the requested delete contract. Do not read first
merely to distinguish those cases. An acknowledged Put of identical bytes is
also APPLIED. Neither outcome promises physical byte changes, and no
affected-row count is exposed. Definite item errors and ambiguous write
acknowledgements still require their actual evidence; a zero matched/deleted
count alone is not a failed or unexecuted Delete.

### 4.2 Evidence boundaries

```text
decoded -> validated -> queued -> dispatched -> backend evidence -> terminal
               |           |          |
          NOT_STARTED  NOT_STARTED    UNKNOWN unless stronger proof exists
```

The scheduler owns the queued/dispatched race under one state transition. Once a
backend send may have happened, absence of a reply, caller cancellation, failed
abort, or a gRPC deadline is not evidence of non-application. The adapter can still
prove NOT_STARTED/NOT_APPLIED at a later layer; Core cannot infer that from an
ordinary network error. For a physical bulk failure, classify each known item;
unknown items are UNKNOWN, not collectively failed/not-applied.

Lost results are inherently ambiguous. A client that sent a mutation but received
no complete terminal result treats it as UNKNOWN, even if the server probably
rejected it. An operation known never to have been sent is locally NOT_STARTED.
No status-query RPC or durable operation ledger is provided. Reading the current
document afterward does not in general prove which concurrent mutation ran.

### 4.3 Narrow retry matrix

| Situation | V1 behavior |
| --- | --- |
| Read transport failure before delivering a result | No default replay; an explicit SDK policy may retry within the original deadline. Data may be newer. |
| Partially delivered Scan/Native stream | No transparent restart, concatenation, or cursor recreation. |
| Definite ES conditional conflict | Adapter reads again and re-evaluates deterministic transform within its bounded attempt budget. |
| Definite MongoDB transaction abort/conflict | Adapter may begin a new transaction and re-run transform; see section 11. |
| MongoDB ambiguous commit | Resolve/retry the same transaction's commit only; never re-run transform under a new transaction. |
| Any ambiguous ordinary mutation send | Return UNKNOWN. No remote failover, retry middleware, or SDK automatic replay. |

Configure gRPC, HTTP clients, backend drivers, proxies, and SDKs consistently. No
configured gRPC retry/hedging or search-client mutation failover; no unbounded
wait-for-ready queue. A gRPC transport's documented transparent retry that proves
the application never received the call is not a replay after possible execution
[D3]. MongoDB ordinary retryable writes are disabled initially; transaction commit
resolution remains permitted and must use the driver's transaction identity [D1].
Do not assume `retryWrites=false` disables the driver's required commit retry.

Request IDs and Bulk indexes are correlation, not idempotency keys. Even Put/Delete
must not be silently repeated: an intervening writer can make an apparently
idempotent replay destructive.

## 5. Public Protocol: `weir.v1`

This section defines a proposed wire contract, not generated Protobuf or an SDK.
Field numbers below are proposed V1 allocations. Do not reuse numbers after
publication. Reject absent/unknown required semantic variants before execution;
do not treat an unknown mutation kind as a default Put. Additive unknown fields
must never be used to smuggle required semantics past an older node.
Future correctness-changing semantics require a new action/profile discriminator
that older nodes reject, or a new major service package, not an ignorable optional
field. Never reuse a runtime/profile name for changed semantics.

### 5.1 RPC surface

| RPC | Shape | Scope |
| --- | --- | --- |
| Read | unary request, unary result | Exactly one record, bounded document response. |
| Mutate | unary request, unary result | Exactly one record, one terminal mutation outcome. |
| Bulk | bidirectional Operation/Result stream | Many Read/Mutate operations, one Store; independent result delivery. |
| Native | bidirectional envelope/body/result stream | One stateless backend interaction, possibly streamed request and response. |
| Scan | unary request, server-streamed documents and terminal frame | One live traversal of one adapter-defined resource. |

Read and Mutate are genuinely small unary operations; no unnecessary server stream
for a single scalar result. Bulk addresses arbitrarily long logical input without
an unbounded repeated-operations message. Native needs duplex framing because HTTP
backends may produce an error before consuming the request body. Scan needs no
client stream. SDK convenience functions may wrap these shapes, but must not collect
unbounded results under an innocent-sounding method.

There is no generic Query or Count RPC. Use a native MongoDB command/search request;
retain its own filter, sort, projection, aggregate, count, and consistency semantics.
A shared ability to count does not make count results portable across visibility
models and backend query languages. Scan standardizes backpressured *delivery*, not
query meaning.

### 5.2 Core message inventory

`?` means optional; strings/bytes/repeated fields always have configured limits.
An error message is diagnostic, not a machine-readable backend error parser.

| Message | Fields (number: name, type) |
| --- | --- |
| Document | 1: media_type, string; 2: data, bytes |
| Failure | 1: code, FailureCode; 2: message, string |
| ReadRequest | 1: resource, canonical URI; 2: read_media_type, string?; 3: adapter_options, Document? |
| ReadResult | oneof: 1: document, Document; 2: missing, empty marker; 3: failure, Failure |
| MutateRequest | 1: resource, canonical URI; 2: adapter_options, Document?; oneof action: 10: put, Document; 11: create, Document; 12: replace, Document; 13: delete, empty marker; 14: atomic_transform, Transform |
| MutationResult | 1: outcome, MutationOutcome; 2: failure, Failure? |
| Transform | oneof: 1: program, ProgramTransform; 2: backend_expression, Document |
| ProgramTransform | 1: runtime, bounded versioned identifier; 2: source, bytes; 3: input, Document? |
| BulkOpen | 1: store, canonical Store-root URI |
| BulkOperation | 1: index, uint64; oneof: 10: read, ReadRequest; 11: mutate, MutateRequest |
| BulkRequestFrame | oneof: 1: open, BulkOpen; 2: operation, BulkOperation |
| BulkResult | 1: index, uint64; oneof: 10: read, ReadResult; 11: mutation, MutationResult |
| BulkEnd | 1: received_count, uint64; 2: result_count, uint64 |
| BulkResponseFrame | oneof: 1: result, BulkResult; 2: end, BulkEnd |
| NativeOpen | 1: resource, canonical URI; 2: descriptor, Document; 3: body_media_type, string? |
| NativeRequestFrame | oneof: 1: open, NativeOpen; 2: chunk, bytes |
| NativeHead | 1: metadata, Document?; 2: body_media_type, string? |
| NativeEnd | 1: completion, NativeCompletion; 2: failure, Failure? |
| NativeResponseFrame | oneof: 1: head, NativeHead; 2: chunk, bytes; 3: end, NativeEnd |
| ScanRequest | 1: resource, canonical URI; 2: selector, Document?; 3: read_media_type, string?; 4: fetch_items_hint, uint32 |
| ScanEnd | 1: document_count, uint64; 2: failure, Failure? |
| ScanResponseFrame | oneof: 1: document, Document; 2: end, ScanEnd |

`MutationOutcome` has unspecified=0, NOT_STARTED=1, NOT_APPLIED=2, APPLIED=3,
UNKNOWN=4. `NativeCompletion` has unspecified=0, NOT_STARTED=1,
RESPONSE_COMPLETE=2, RESPONSE_INCOMPLETE=3. It describes command
dispatch/response delivery, not database effects; section 13 defines the allowed
combinations with Failure. Counts and indexes must not overflow; close a stream
before the uint64 limit. Empty chunks are rejected to avoid zero-progress abuse.

Proposed FailureCode allocation is unspecified=0, INVALID_ARGUMENT=1,
UNAUTHENTICATED=2, PERMISSION_DENIED=3, NOT_FOUND=4, PRECONDITION_FAILED=5,
CONFLICT=6, UNSUPPORTED=7, RESOURCE_EXHAUSTED=8, UNAVAILABLE=9, CANCELLED=10,
DEADLINE_EXCEEDED=11, INTERNAL=12. An explicitly present Failure must not use
unspecified. For record results, a missing Failure means success/no-op, never
UNKNOWN. For ScanEnd it means complete traversal. For NativeEnd it means Weir
completed the native response transport; database success/failure remains in
that native response.

Put means insert-or-replace the complete record. Create means insert only if
absent. Replace means replace only if present. Ordinary Delete is successful if
already absent, reporting APPLIED on positive acknowledgement. These are
capability-checked backend record primitives, not merge/update operators.
Unsupported adapters reject them explicitly. There is no field patch DSL and no
portable client revision or precondition token.

No mutation response includes an automatically fetched post-image in V1. A later
Read may observe another writer and is not an atomic returned image. Native
`findAndModify` or equivalent retains its backend-native returned-image contract.

### 5.3 Stream grammar and completion

Bulk input is exactly Open, zero or more Operations, then client half-close.
Indexes must be consecutive starting at zero, so duplicate detection is one counter,
not an unbounded set. There is no maximum logical operation count below uint64,
but there are finite in-flight and lifetime limits. An empty Bulk is valid.

Bulk output is zero or more Results in completion order, then exactly one End and
gRPC OK. Success requires one result per received operation and matching counts.
End is final completion accounting, not durable acceptance. The server does not
send an acceptance acknowledgement per operation. A client may submit the next
operation while receiving previous results and must read and write concurrently.

A well-formed but invalid operation receives its indexed terminal error. A framing,
authorization, or connection failure may terminate without an End. Previously
received results remain authoritative; missing mutation results are UNKNOWN from
the client's perspective. A half-close does not cancel accepted work. Stream
cancellation does. No buffering to restore input order is allowed.

Native input is Open, body chunks, half-close. Native output is an optional Head,
body chunks, then one End. Preflight rejection can produce only End. A backend
may reply before input half-close. Scan output is Documents followed by one End;
an empty traversal still produces End. A non-OK transport status or missing End
means truncation, not a successfully empty or complete response.

For application-level failures whose terminal envelope can still be sent, return
that envelope with gRPC OK. Transport/authentication/framing failures may use gRPC
status directly. Never reinterpret a bare DEADLINE_EXCEEDED or RESOURCE_EXHAUSTED
status as mutation non-application. This conservative rule also covers rejection
before a request can be decoded and assigned an indexed result.

### 5.4 Metadata and deadlines

Use standard gRPC deadline/cancellation and W3C trace context. A bounded
`weir-request-id` metadata value is generated once at ingress if absent and preserved
through forwarding. Bulk identity is `(request_id, index)`; a unary operation uses
index zero. IDs are for diagnostics only; clients cannot authorize or deduplicate
work by choosing another client's ID. Do not return document data in trace baggage.

At initial ingress, establish an effective deadline as the earlier of the caller's
deadline and the method's configured maximum lifetime; use a finite default when
the caller supplies none. Each forward subtracts elapsed local time when propagating
gRPC's remaining timeout [D4]. It never starts a new timeout budget. The same
deadline covers queueing, transform attempts, backend work, and result delivery.

### 5.5 Wire-feature necessity review

| Feature | Must survive a process boundary? Why it stays, or why it is absent |
| --- | --- |
| Canonical resource and Store-root binding | Yes: determine destination and reject mixed-Store work. |
| Document media type/bytes | Yes: prevent lossy or incorrect interpretation. |
| Read representation and adapter options | Yes: affect backend interpretation and visibility; remain opaque. |
| Mutation action and transform spec/input | Yes: execution intent must reach the final adapter unchanged. |
| Record outcome, Native response completeness, Weir failure | Yes: distinguish record execution evidence from complete/incomplete native transport without interpreting arbitrary command effects. |
| Bulk index/counts and terminal stream frames | Yes: correlate out-of-order results and detect incomplete delivery. |
| Native descriptor, body chunks, response metadata | Yes: preserve stateless native semantics and bounded bodies across hops; database results remain native. |
| Scan selector and fetch hint | Yes: backend traversal intent and bounded fetching; a hint is not a query DSL. |
| Deadline, cancellation, trace/request context | Yes, but existing gRPC/metadata mechanisms carry them; no redundant timestamp fields. |
| BatchKey, ordering map, queue position, concurrency window | No: local scheduling details; never serialized. |
| Acceptance state, retry Boolean, idempotency promise | No: either misleading or absent from the product. |
| Portable revision, generic query/sort/count, returned-image flag | No: unnecessary or falsely portable. |
| Transform source digest/declaration registry | No: receiver can hash bounded source; a stored-program protocol is unnecessary. |
| Dynamic capabilities/control plane | No V1 protocol: configured adapter profiles and explicit UNSUPPORTED are enough. |

## 6. StoreRuntime and Adapter Contract

### 6.1 StoreRuntime ownership

StoreRuntime owns one scheduler state machine, its pending ledger, execution
window/controller state, in-flight work, bounded live-stream accounting,
metrics, and the lifecycle of one adapter. The adapter exclusively owns its
backend clients, pools, driver sessions/cursors, and backend capability state.
StoreRuntime constructs it once and calls Close once after drain; it does not
close its clients separately. No PoolManager is introduced. Batch formation and
adaptive concurrency are algorithms inside the runtime, not independently queued
services.

One internal work item references validated request data and contains resource,
request/index identity, optional Bulk sequence key, operation class, accounted
input bytes, deadline, response-credit reservation, and an opaque adapter plan.
The plan contains record identity and local compatibility/BatchKey information.
It is not a second public API and is never marshalled across peers. Work carries
references rather than copies of whole Protobuf requests.

### 6.2 Adapter responsibilities and shape

The implementation should expose the following small conceptual contract. These
are operations and obligations, not final Go interface signatures:

| Adapter operation | Responsibility |
| --- | --- |
| Construct/Close | Exclusively own and close bounded backend clients/pools; maintain the configured capability profile. |
| Prepare(work) | Parse and canonical-check resource, validate media/options/action, produce record identity and physical compatibility plan; no backend I/O or unbounded work. |
| Execute(batch, bounded emitter) | Execute a compatible bounded record batch; return one terminal result per item and backend congestion feedback; no hidden scheduler or fanout. |
| ExecuteNative(work, source, sink) | Execute one bounded stateless exchange; retain its dispatch permit until backend exchange/cleanup finishes; report transport completeness, not native effect normalization. |
| FetchScan(work, cursor, page budget) | Under a dispatch permit, open or advance one cursor and return one bounded page plus native completion/error evidence; do not wait on client sends. |
| CloseScan(cursor) | Release an adapter-owned cursor with a bounded cleanup context; no new application work or admission. |

Core chooses singleton versus batch using the plan; adapters do not create their
own batch queues. Native exchanges and Scan fetch steps use the same scheduler,
not private execution queues. Scan session state outlives individual fetch
permits. A single concrete implementation per backend is sufficient. Shared
Elasticsearch/OpenSearch wire logic can live in one package with explicit tested
capability differences, not a dynamic Provider system.

The local prepared plan supplies: canonical record key when known, one bounded
opaque compatibility token incorporating BatchKey, whether batching/streaming is
supported, input/output framing bounds, and execution class. The adapter can
parse document bytes to validate native constraints; preparation must stay
bounded and expensive transform compilation runs only after execution admission.
Core never interprets the plan's backend-specific values.

Capabilities include allowed record actions, input/read media types, supported
Native descriptors, Scan support, transform runtimes/codecs, and backend-expression
formats. AtomicTransform is not one unconditional claim: arbitrary program transforms
on MongoDB additionally require a transaction-capable deployment; search requires
working OCC and a supported `_source`. Unsupported combinations fail before writes.

Checks requiring fresh backend metadata run under an execution permit before any
write, not inside Prepare. A backend read used for that check may make the terminal
outcome NOT_APPLIED rather than NOT_STARTED. Bounded startup capability discovery
is separate from per-request scheduling and never supplies a capacity estimate.

An execution permit covers one sequential execution: a record batch, complete
atomic-transform attempt loop, Native backend exchange, or one Scan open/fetch
step. A live Scan cursor does not hold a permit between fetches. The adapter may
split a physical request only sequentially and must not launch hidden parallel
data work. Driver checkout and server-selection waits have finite timeouts and
are bounded by admitted executions, not another application backlog. Lifecycle
cleanup has a short separate deadline and is bounded by already-owned active
executions/cursor sessions; it must not wait for new admission. C bounds data
execution, not startup/cleanup I/O.

Results and congestion feedback are different. Outcome says what happened to data;
feedback says whether the backend is congested. A write concern timeout may both
mean UNKNOWN and reduce concurrency. A deterministic transform error means
NOT_APPLIED and supplies no congestion signal.

## 7. The One Store Scheduler

### 7.1 State and admission

Use a small lock-protected state machine and one wakeup/timer mechanism per
Store. State is: a bounded FIFO of operation/Scan-continuation entries, Bulk
sequence-key head/in-flight indexes, pending/reserved operation-byte counters,
active execution counts, live-long-session count, and the adaptive controller.
The key index references the same entries as the FIFO; it is not another queue
with separately retained payloads.

Admission is a nonblocking state transition after validation:

1. Check the process overload latch once for this new operation.
2. Check Store `max_pending_operations` and `max_pending_bytes` and the connection's
   bounded outstanding/result-delivery budget.
3. For Native/Scan, also reserve a bounded live-session slot; if unavailable, reject
   without starting backend work rather than create a session wait queue.
4. Insert one entry, charge its encoded bytes and fixed per-entry accounting
   allowance once, and signal the scheduler. A Scan retains that entry/input
   reservation across fetches; continuation is not new logical admission.

If it cannot fit, unary calls receive NOT_STARTED with RESOURCE_EXHAUSTED. A Bulk
reader normally stops reading its next frame until capacity is available, holding
at most one validated current frame. A current frame that can never fit is rejected;
it must not wait forever. Every live stream is itself bounded by transport/session
limits, so these waiting readers cannot multiply without bound.

Decoded but not yet admitted frames are bounded by listener/session limits, not
quietly omitted from the memory model. On overload, stop accepting new operation
frames and new RPCs; finish already admitted work. An admitted Native
operation's body chunks, admitted Scan continuations, and existing
result/cleanup paths may continue within their bounds; blocking them could
prevent admitted work from finishing. Never re-admit at batching, Scan
continuation, or adapter conflict retry.

### 7.2 Selection and stream-scoped sequencing

V1 uses oldest-*eligible* selection, not strict head-of-line FIFO. An entry is
eligible when its deadline is live, any predecessor in its Bulk sequence is
complete, there is output credit, and it is ready for backend work. A blocked
sequence or stalled client cannot prevent unrelated eligible work from
progressing. An O(P) scan of the finite pending set is acceptable; do not add
lanes or heaps without evidence.

Within one Bulk stream, Read and Mutate operations for the same record execute
in frame order. The sequence key is `(server-owned live stream identity, record
URI)`. Different streams and unary calls do not share that key. Client-supplied
diagnostic request IDs cannot merge sequencing domains. This bounded per-stream
index is required for the Bulk contract; it is not an optional cross-client
optimization.

Independent Reads may run concurrently, including Reads of the same record. V1
adds no global per-record Read lock and no cross-request mutation chain. The
database provides atomicity for competing mutations. Cross-request mutation
serialization or read coalescing would require measured benefit and a separate
change; neither is a V1 configuration option. Native and Scan do not join record
chains.

Release a Bulk sequence key on local terminal completion, without waiting for
the client to receive the reserved result. UNKNOWN permits local completion but
the backend may still finish later: the next item cannot rely on database
ordering in that case. Successors are independent commands, not a conditional
workflow. Clients needing success-dependent execution must await a definite
predecessor result before sending the next command. Read-after-write still uses
native read/visibility semantics. A Bulk stream closing releases its sequence
index after bounded active-work cleanup.

### 7.3 Micro-batch algorithm

Select the oldest eligible entry as the seed. Gather eligible distinct-key entries
with exactly the same adapter compatibility token, within operation, encoded
request-byte, and reserved-response-byte limits. Compatibility includes BatchKey,
action compatibility, media handling, acknowledgement/refresh options, authorization
context where applicable, and output semantics. Core compares tokens; it does not
decode documents to calculate them.

Dispatch when a configured maximum is reached, the oldest eligible entry's short
collection window expires, or remaining deadline slack requires immediate dispatch.
Start the collection window when the entry first becomes eligible and never reset
it on subsequent arrivals. Do not form a batch and put it into another ready queue.
Wait in the one pending set until both a dispatch slot and batch conditions exist.

Batching neither folds independent mutations into one commit nor merges
same-record transform programs. Those optimizations obscure per-operation
acknowledgement, failure, and returned-state boundaries. A physical batch has at
most one item per canonical record key; a later operation in the same Bulk
sequence waits for its predecessor. Independent same-key calls may execute in
different concurrent batches; this is not global record serialization. Atomic
program transforms are singleton in V1. Compatible simple reads/mutations may
use native multi-get/bulk APIs. Backend batch size is independent of application
request boundaries.

Batch eligibility requires adequate per-item evidence for the **promised**
result. MongoDB aggregate matched counts cannot prove which conditional Replace
succeeded; use qualified per-item results or singleton Replace in that case [S5,
D13]. Ordinary Delete does not promise an affected-row count or distinguish
already-absent records. A complete successful acknowledged Delete bulk with no
item/write-concern errors can therefore report APPLIED for every item without
verbose deleted counts. Mixed success/error replies still require item-level
command evidence; unknown items stay UNKNOWN. Definite ordered-bulk
short-circuit evidence may identify NOT_STARTED items, but a network failure
does not. Never fall back to singleton replay after a batch may have run. Only a
definite unsupported-command rejection proving no effects can permit a fresh
capability fallback.

Before dispatch, reserve bounded result slots/bytes for every included operation.
Read reservations use the maximum permitted document-result size, not a guessed
average. Mutation terminal results have a small fixed maximum. If there is not
enough response capacity, reduce the batch or leave that client's work pending.
Do not execute a whole batch and then discover there is nowhere to put its results.

### 7.4 Deadlines and shared batches

Recheck each item immediately before dispatch; expired items are NOT_STARTED.
A dispatched bulk request cannot generally cancel just one backend item. Use an
execution context bounded by the configured backend-call maximum, shutdown deadline,
and the latest still-relevant participant deadline. Do not let the earliest caller
deadline abort unrelated participants. Remove a cancelled caller as an interested
recipient; cancel shared I/O when all participants are gone or its execution bound
expires. Retain input/result reservations until backend completion or bounded cleanup.

This does not extend a caller's RPC deadline or allow a new attempt after it. It
acknowledges that already-dispatched effects can occur after a caller stops waiting.
A caller whose result is lost reports UNKNOWN; a surviving participant can still
receive its actual result. Keep backend and caller-wait timeout metrics separate.

### 7.5 Stream lifetime is not database concurrency

Native/Scan reserve a small fixed live-session budget independently of C. This
budget bounds cursors, upload pumps, retained pages, and idle clients; it is not
a second adaptive controller or work queue. Start with one live Native/Scan
session per Store. Cmin is 1, and a Store with Cmax=1 may still support
Scan/Native.

A Scan keeps one charged continuation entry for its lifetime. While fetching or
emitting its current page, that entry is ineligible; it does not create another
payload queue or pending reservation. To fetch, reserve a bounded page/output
budget and dispatch one open/fetch step through the common scheduler. On
completion, release the execution permit before waiting on client sends. The
adapter retains only the bounded page/cursor under the live-session budget. When
the page drains and output credit is available, move the same continuation to
the FIFO tail and make it eligible again. This yields between pages, without
parallel prefetch or cursor migration.

A Native HTTP exchange may couple upload, backend processing, and response
delivery. Keep its execution permit while that exchange is outstanding; never
pretend client stall has released the backend resource. At C=1 it can
temporarily block short work. That is an explicit bounded limitation, not a
promise of a reserved short-work slot. Input/send-stall, backend, and lifetime
deadlines terminate stalled exchanges. Bulk connections consume no database
permit except for dispatched record work.

All sessions have finite lifetimes and retain their reservation until bounded
cleanup finishes. Cancellation stops further fetches. During drain, admitted
Scan continuations may finish within the drain deadline, then close cursor
state. Cleanup cannot require fresh admission. Lowering C does not revoke
existing permits. There is no Cmin >= long-sessions + 1 rule, priority lane, or
guaranteed short-request latency.

## 8. Adaptive Database Concurrency

### 8.1 Minimal explicit-feedback AIMD

Retain feedback from real backend work, but keep V1 to one small AIMD controller
inside StoreRuntime. It owns neither admission tickets, another queue, synthetic
probes, nor retries. Let C be the dispatch window, Cmin=1, Cmax the local hard
ceiling, and A active executions. Start at Cmin; dispatch while A < C and
outside cooldown. Cmax is bounded by the tested driver/CPU budget, not divided
by a replica count.

Use one adjustment epoch to avoid reacting repeatedly to the same old flight:

1. Tag each dispatch with its controller epoch. Confirmed backend overload,
   backend-attributed timeout, or temporary unavailability in the current epoch
   halves C, rounded down with a floor of 1; advance the epoch and clear growth credit.
2. Pause new dispatch for one short randomized cooldown, initially 100-300 ms.
   Do not add exponential recovery history or synthetic recovery requests. Already
   queued distinct requests may run after cooldown; this never replays a mutation.
3. Old-epoch completions release permits but cannot adjust C. A physical batch
   generates one congestion sample, not one reduction per failed item.
4. With eligible demand and a saturated window, at least max(4, C) healthy executions
   from the current epoch permit growth by one, no more often than once per 250 ms.
   Advance the epoch and reset credit on growth. Idleness supplies no growth credit.

A Scan fetch is an execution sample; an idle cursor or emitted document is not.
Native supplies a healthy sample only after its exchange finishes, and
congestion only when the adapter can positively attribute the failure to the
backend. Ignore ambiguous consumer-stall timeouts. Transform retries supply at
most one sample for the enclosing execution. Deterministic errors,
preconditions, record conflicts, caller cancellation, and Weir's own limits are
not database congestion.

Measure backend-call duration separately from queue wait, transform CPU/backoff,
encoding, and client send waits for observability and qualification. V1 does
**not** use latency buckets, dual EWMAs, moving baselines, or latency-triggered
reductions. Successful latency-only degradation may therefore be detected less
aggressively; this is a declared limitation, not justification for an
unvalidated controller. Add latency-driven adaptation only after mixed-workload
measurements demonstrate its benefit and stability over this explicit-feedback
baseline.

### 8.2 Limitations and why no coordination

The window bounds locally tracked executions, not database work that might continue
after a timed-out network call. Use supported native execution deadlines/cancellation
and reject detached background commands; do not claim cancelled work vanished.
Replicas compete independently; transient overshoot, oscillation, and conservative
underutilization are possible. Jitter and old-flight suppression damp synchronization,
but this is not a proof of globally optimal or exact concurrency. Separate Stores
sharing one database also have independent controllers. Benchmark under realistic
load mixes and do not advertise an exact global connection/work ceiling.

Redis, leases, leader election, distributed semaphores, synthetic Ping capacity
tests, and replica-count quota division solve a stronger problem Weir does not
promise. They are absent. Startup topology checks may inspect backend capabilities;
they do not generate capacity samples. Basic health checking has the same separation.

## 9. Resource Bounds and Backpressure

### 9.1 Explicit finite limits

Limits are deployment configuration with validated relationships. The illustrative
profile below is a qualification starting point, not a memory sizing guarantee or
a commitment that every adapter accepts those exact maxima.

| Bound | Illustrative initial profile | Owner |
| --- | --- | --- |
| Protobuf frame, uncompressed | 5 MiB including envelope | Every public/peer transport |
| Record document | 4 MiB, smaller than frame after metadata | Listener and adapter |
| URI / request metadata / error text | 4 KiB / 16 KiB / 1 KiB | Protocol/listener |
| Native body chunk / opaque descriptor or options | 64 KiB / 64 KiB | Protocol/adapter |
| Transform source / input / output | 64 KiB / document limit / document limit | Transform profile |
| Concurrent transport streams/connections | Finite per connection and process-wide connection limits; example total application session ceiling 64 | Listener |
| Store pending | 4,096 operations and 32 MiB accounted bytes | Scheduler |
| Store execution window | Cmin=1, Cmax=32; permits cover record work, Native exchanges, or Scan fetches | StoreRuntime |
| Live Native/Scan sessions | 1 per Store initially, independent of C; retain one bounded page/cursor state per Scan | StoreRuntime session accounting and adapter |
| Physical batch | 128 operations, 8 MiB encoded native request, 1 ms collection | Scheduler plus adapter exact backend size checks |
| Bulk outstanding/result delivery | 32 operations, 16 MiB result credits per stream | Session result delivery |
| Streaming read-ahead | One backend fetch batch and at most two output frames | Adapter/stream relay |
| Scan retained page | Adapter-qualified finite native response/decoder bound, initially targeting 16 MiB plus bounded framing/decoder overhead | Adapter and live-session reservation |
| Native monolithic input | Finite per-adapter command limit, e.g. 4 MiB for BSON command | Adapter |
| Lifetimes | Finite method maximum, backend call timeout, stream idle/send-stall timeout, drain deadline | Listener/runtime |
| Transform resource usage | Instruction, allocation-byte, node, depth, stack, source/compile and wall-time limits; fixed profile | Transform runtime |

A concrete initial lifetime profile is 30 seconds for unary record calls, 15 minutes
for Bulk, 5 minutes for Native/Scan, 10 seconds for each backend network call/fetch,
30 seconds for a stream send/input stall, 30 seconds for graceful drain, and 2 seconds
for final resource cleanup. Caller deadlines can only shorten these. A live traversal
uses repeated bounded fetches, not one backend-call timeout for its entire lifetime.
The HTTP Native streaming profile initially caps a logical upload at 1 GiB; it does
not allocate that amount. All values require workload qualification before release.

An item must fit as a singleton in every applicable bound, including adapter native
framing overhead, or fail before writing. All hops enforce their own limits; a smaller
downstream limit is a normal failure, never permission to split one atomic document
into multiple writes. Raising MongoDB support toward its native maximum also requires
raising the Weir frame/envelope limit explicitly; it is not inferred from the backend.

Backend drivers may materialize a full native response batch. Adapters must document
that bound, cap cursor fetch counts/response sizes, or use incremental parsers with
limits enforced *before* token allocation. A check after `ReadAll`/full JSON decode
is not a memory bound. If a driver cannot provide bounded behavior for an operation,
do not advertise that operation as streaming. Large individual records can be rejected
even during a Scan; already delivered records then form a partial traversal.

The pending byte ledger counts encoded retained input plus a fixed per-entry
allowance, not speculative heap precision. Operations remain charged once while
waiting for collection, key sequencing, or concurrency. For record/Native work,
dispatch moves input ownership from pending to bounded active execution;
completion leaves only bounded result delivery. A Scan keeps its one input/entry
reservation through all fetches, and moves each fetched page to bounded
session/result ownership before releasing its permit. It is never charged a
second pending entry. Plan/parser allocations must have finite expansion limits.

### 9.2 Bounded-memory argument

For fixed deployment limits, retained application memory is bounded in shape by:

```text
transport connections/streams * bounded transport/frame buffers
+ sum(Store pending-byte and pending-entry budgets)
+ sum(Store Cmax * bounded active batch/decoder/transform working sets)
+ sum(bounded sessions * bounded result-delivery buffers)
+ sum(live Scan sessions * bounded retained page/cursor state)
+ bounded backend pools, caches, and diagnostics
```

This is a structural argument, not an exact resident-memory formula. Allocator/GC
overhead, TLS, driver buffering, and kernel socket memory need measured headroom.
Do not introduce `max_active_heap_bytes` or claim exact enforcement from document
length. Validate that configured concurrency times maximum payload sizes is plausible
for the process budget, then qualify peak memory with slow consumers and maximal
payloads. Bounded does not automatically mean small enough for a particular container.

### 9.3 Process overload guard

Measure process RSS (or cgroup memory for a dedicated container) against an explicit
effective budget, preferring the lower of configured and applicable container limits.
Document the source and unsupported-platform fallback. A Go-runtime-only metric is
a degraded fallback, not proof of total memory usage.

Sample approximately every 100 ms and latch overload at 80%, clearing at 70%.
High watermark stops *new admission*, including new operations on existing streams;
low watermark resumes it. There is no process-wide waiting queue or second adaptive
database controller. Continue dispatching/draining already admitted work to release
memory. Do not deadlock cleanup or result sends by subjecting them to new admission.

The latch cannot prevent every sudden allocation or guarantee survival under arbitrary
misconfiguration. Frame/session/pending/concurrency/transform bounds are primary;
watermarks provide additional protection. CPU is primarily bounded by these same
execution limits and transform instruction budgets; no speculative CPU scheduler.

### 9.4 End-to-end flow control

```text
database overload/attributable failures rise
  -> local dispatch window shrinks
  -> Store pending budget fills
  -> local Bulk receiver stops Recv
  -> each remote relay stops Recv when its one-frame buffer fills
  -> HTTP/2 flow-control windows fill
  -> upstream application Send blocks
```

In the opposite direction, a slow reader fills result credits. Its further work
becomes ineligible, or its Scan/Native fetch stops. A physical batch already in
flight has reserved result space, so other clients in that batch can complete
without being coupled to the stalled client's socket. Never run blocking gRPC
Send from the shared scheduler loop.

Use finite/static HTTP/2 connection and stream windows and finite write buffers;
unbounded automatic window growth is not a substitute for a resource limit.
Application receive/send loops still need explicit bounded queues: HTTP/2 flow
control alone does not bound what application code has already consumed [D5].
Native and Bulk SDKs and relays must pump both directions concurrently to avoid
duplex flow-control deadlocks. Bounded session count makes the small fixed number
of per-session pump goroutines acceptable; one goroutine per pending operation is not.

Stream stalls are cancelled after the configured timeout, releasing local retained
state after bounded cleanup. A cancelled database mutation remains UNKNOWN unless
the adapter has stronger evidence. There is no detached unbounded drain spool.

## 10. AtomicTransform and Structured Values

### 10.1 Two explicit transform forms

An AtomicTransform targets exactly one record. Its semantics are: observe current
record/absence, evaluate a deterministic transform, and atomically apply its selected
record effect using the backend's concurrency mechanism. Competing record writes
may require repeated evaluation. It cannot access a second record, emit events,
make network requests, or return an application-controlled side effect.

The two wire forms avoid pretending an arbitrary program can be compiled into a
database update language:

1. **ProgramTransform**: versioned runtime, bounded source, optional encoded input.
   Current document and input go through an appropriate TransformCodec into the
   shared value model. The adapter owns the read/validate/conditional-write loop.
2. **Backend expression**: media-tagged adapter-native deterministic single-record
   expression. The adapter can validate and execute it as a native atomic update.
   It is not a portable transform DSL and uses no Core document interpreter.

V1 does not attempt arbitrary Lua-to-MongoDB or Lua-to-Painless lowering. A known
safe adapter-native expression is the fast path; a general program is the general
path. Native remains the escape hatch for commands outside this deliberately narrow
single-record transform contract. An adapter may support either form or neither.

Program evaluation receives `current = Missing | Present(Value)` and optional
`input = Missing | Present(Value)` and returns exactly one of:

- Replace(Value): complete replacement, or creation if originally absent;
- Delete: delete the observed record, or successful no-op if absent;
- Keep: no effect;
- Reject(bounded message): definite NOT_APPLIED with PRECONDITION_FAILED.

Missing is not Null. Keep/Reject are decisions about the observed version, not a
promise that a predicate is still true when the caller receives the result. In
particular, a read-only/no-op transaction must not be described as locking a record
against concurrent writers. Clients needing a write-conditioned invariant must
actually request a backend-supported conditional effect.

### 10.2 Minimal Weir Value model

The value model is an internal transform contract, not the representation all
requests must use and not another public Protobuf document type:

| Value | Semantics |
| --- | --- |
| Null | Explicit null, distinct from missing |
| Bool | Exact Boolean |
| Int32 | Exact signed 32-bit integer; width-preserving checked arithmetic |
| Int64 | Exact signed 64-bit integer; width-preserving checked arithmetic |
| Float64 | IEEE binary64, preserving meaningful bits; no implicit integer narrowing |
| String | UTF-8 bytes |
| Bytes | Uninterpreted byte sequence |
| Array | Ordered sequence of Values |
| Object | Ordered sequence of `(string, Value)` fields, not a map |
| Extended | Versioned namespaced type identifier and opaque bounded bytes |

Ordered Object is necessary for faithful BSON/native ordered documents and stable
traversal. A codec can preserve duplicate fields; a name-based transform access to
duplicates fails as ambiguous rather than silently choosing one. Backend validation
may still reject such documents. Limits apply to depth, field/element count, string
length, and decoded allocation as well as encoded bytes.

Do not promote Decimal128, dates, UUIDs, ObjectIds, regexes, or binary subtypes
into portable primitives without evidence. For example, codecs preserve them as
`mongodb.bson.objectid.v1`, `mongodb.bson.decimal128.v1`, or typed binary
Extended values. BSON int32 and int64 decode to Int32 and Int64 respectively,
never Extended; encoding preserves their widths even when an Int64 value fits in
32 bits. This is an internal transform rule, not a Core document parser [D12]. A
codec must preserve other unsupported numeric forms/bit patterns as Extended or
reject them, never round through float64. JSON integers in the exact signed
64-bit domain decode to Int64; other unsupported JSON numbers may retain their
token as encoding-specific Extended.

Both integer types support checked arithmetic. Same-width addition/subtraction/
multiplication preserve that width; overflow fails before a write. Mixed-width
arithmetic requires an explicit checked conversion, and integer/float conversion
must be explicit. The runtime profile supplies exact typed
constructors/conversions, so incrementing a BSON Int32 uses an Int32 one, not a
Lua floating-point coercion. Unmodified fields retain their original numeric
tags; narrowing Int64 to Int32 requires an explicit range check. Typed
arithmetic is part of the pinned language profile and its conformance vectors,
not an adapter-specific Lua/BSON bridge.

Unknown Extended values can be copied/moved/deleted as opaque values. They cannot
be forged, implicitly converted to JSON, or used in numeric/string operations.
New values unsupported by the target codec fail *before the write*. Cross-encoding
transcoding is not implied. Unchanged values must survive codec round-trips without
type loss; byte-for-byte textual whitespace identity is not promised for transformed
JSON. Opaque operations never require this structured round-trip.

### 10.3 TransformCodec and runtime separation

```text
adapter-native opaque current/input
    -> codec decode with limits
    -> Weir Value
    -> deterministic runtime
    -> action + Weir Value
    -> codec encode with limits
    -> adapter validation and native atomic write
```

Codecs belong to the backend/transform boundary, not the scheduler. They only depend
on the value model, not Lua. Runtimes only depend on the value model, not BSON/JSON.
N codecs and M runtimes therefore need N+M integrations, not N*M special bridges.
The opaque adapter interface remains usable without either a codec or a runtime.

V1 can ship one pinned Lua language/runtime profile. This is not a Lua extension
of StoreRuntime and not an arbitrary stored-procedure service. Hide filesystem,
network, OS, clock, randomness, module loading, native pointers, locale, and
unrestricted debug facilities. Use a fresh invocation state, deterministic
object iteration, precise Int32/Int64 operations and typed constructors,
explicit arrays/null/missing, and checked conversions. No conversion of an
integer to Lua floating point merely for convenience; mixed-width arithmetic
follows section 10.2.

Enforce instruction fuel including host helper work, allocation-byte/node/depth
budgets, stack/call limits, source/compile limits, bounded encoded output, and a
wall-time watchdog. A runtime whose standard library can allocate unbounded memory
or block outside these controls is not approved for in-process V1 use. Compile
after Store execution admission, not in an unlimited pre-admission CPU path.

Cache immutable compiled programs by source digest plus runtime/profile version in
a per-Store byte- and entry-bounded cache. Every request still supplies source; no
digest-only remote registration, deployment, or global mutable registry. Caller
timestamps/seeds, if needed, are explicit input and remain identical across retries.

The wall-time watchdog may produce different failures under load, but successful
evaluation must be deterministic for identical inputs. Conformance tests must
exercise Int32 increments/overflow, unchanged integer-width round-trips, extreme
Int64 values, explicit mixed-width conversions, Extended values, duplicate
fields, nested structures, and resource exhaustion; do not assume an existing
Lua engine is a safe sandbox.

## 11. MongoDB: Atomic RMW Without Owned Document Metadata

The MongoDB adapter uses acknowledged native record operations. Put/Create/Replace/
Delete obey the URI identity and native uniqueness/immutable-ID rules. No
`_weir_revision`, `_sink_metadata`, revision side collection, synthetic document
hash-CAS, distributed record lock, or whole-document equality filter provides
concurrency correctness. In particular, matching a previous document's fields is
not a substitute for native concurrency control.

MongoDB record identity also requires a qualified collection layout. V1 admits
unsharded collections, or ranged sharded collections whose complete shard key is
`_id`, with simple identity/index collation. Reject other layouts for record APIs
rather than assume `_id` is globally unique: MongoDB documents that the default
`_id` index only guarantees per-shard uniqueness when `_id` is not the shard key
[D7]. Native/Scan may still operate on those Stores under their own native semantics.
Collection-layout checks run during bounded startup validation or under an execution
permit; any resource-capability cache is entry/byte bounded. Do not add a shard-key
DSL or synthesize a global unique index. Concurrent changes to layout/index collation
are outside the validated record profile and require revalidation/restart.

### 11.1 Native-expression fast path

The adapter may accept an explicitly typed, deterministic MongoDB update/update-
pipeline expression for one record. It fixes the exact record predicate from the
URI and rejects expressions that change identity, access another collection, invoke
external effects, or introduce unbounded/non-deterministic behavior. Backend options
remain native and validated. A supported expression executes as one atomic native
update; there is no client-side read/write gap.

Fast-path expression support is a finite tested subset, not an optimistic compiler
for arbitrary programs. Unsupported expressions fail before execution. The same
database's wider update syntax is available through Native without being mislabeled
as a portable deterministic transform. Native query evaluation remains in MongoDB,
not Core.

The initial expression profile is
`application/vnd.weir.mongodb-update.v1+bson`: an ordered native update document
whose only top-level operators are `$set`, `$unset`, and `$inc`. At least one operator
must be present. Reject changes to `_id`/its descendants, positional `$` paths,
array-filter requirements, duplicate operator/ambiguous field entries, and any
unsupported operator; validate native operand types and all size/depth limits.
It targets an existing record (`upsert=false`); absence is NOT_APPLIED with
PRECONDITION_FAILED. Pipelines and broader operators are Native-only in V1. This
delivers a useful atomic native fast path without a speculative compiler or an
unspecified general expression whitelist.

### 11.2 General program path

Require a transaction-capable deployment and supported topology/storage configuration.
Use the official driver's sessions and transaction semantics [D1]. A default profile
uses primary reads, transaction snapshot read concern, and majority commit write
concern; validate any different profile rather than claiming the same durability.
Keep all operations within one transaction sequential and on its session.

```text
acquire one Store execution permit
  start transaction with fresh attempt identity
  read one record or absence
  decode current/input; evaluate deterministic program
  validate replacement/output size and unchanged identity
  replace/delete the observed record, or insert if previously absent
  commit transaction
  produce terminal evidence
release permit
```

Concurrent native clients may freely replace, update, or delete the document. A
conflicting transactional write/commit must not overwrite their intervening committed
write. The adapter uses transaction conflict/abort evidence to start a fresh snapshot
and re-evaluate, not a Weir-managed revision [D1]. An insert race on a missing `_id`
can be retried only after a definitive no-commit/abort outcome; an unrelated unique
index failure is not automatically classified as a record conflict.

### 11.3 Transaction state machine

| Event | Permitted next step | Terminal evidence if stopping |
| --- | --- | --- |
| Read/decode/program validation fails before commit | Abort/close bounded session; do not commit. | NOT_APPLIED, with the appropriate error. |
| Keep or Delete of observed absence | Finish without a write; close transaction. | NOT_APPLIED, success about that observation. |
| Transaction conflict with definite abort / `TransientTransactionError` under the documented driver contract | Start a new transaction, read current state, run program again. | NOT_APPLIED + CONFLICT if the bounded attempt budget expires while all attempts are known not committed. |
| Commit acknowledged under configured concern | Finish, no further execution. | APPLIED. |
| Commit reports `UnknownTransactionCommitResult` or loses its response | Retry/resolve commit using the **same session and transaction number** under native driver rules; do not re-read or re-run program. | UNKNOWN if the original deadline/commit-resolution budget expires. |
| Definite final commit rejection proving the transaction did not commit | Abort/close; classify according to evidence and native labels. | NOT_APPLIED. |
| Cancellation/shutdown after commit may have been sent | Bounded same-attempt cleanup; no new mutation. | UNKNOWN unless acknowledgement already proves APPLIED. |

Bound total transaction attempts (initial proposal: five), conflict backoff (small
jitter with a strict cap), aggregate program fuel, and commit-resolution time by the
original deadline. Each attempt does not receive a fresh full timeout or fuel budget.
Keep the Store permit throughout; do not requeue the operation or recursively acquire
another permit. Conflict sleeps are short and bounded.

Once commit is ambiguous, an ordinary later error must not clear the ambiguity
latch. Only definitive native resolution can. A failed abort is not evidence that
an already-committed transaction rolled back. Do not call a convenience transaction
API blindly if its callback retries cannot enforce these rules; use a reviewed
driver-native transaction loop. Driver-required retries of the same commit are
allowed even when ordinary retryable writes are disabled [D1].

Do not interpret a no-op replacement/read-only transaction as an exclusive lock.
For Keep/Reject/identical-output no-ops, document that the decision concerns the
observed snapshot, not the state at response delivery. For state-changing results,
the native transaction must validate the read/write dependency. Integration tests
must cover conflicts with direct native writers, deletion/recreation of the same
ID, and missing-record insertion races.

### 11.4 Unsupported deployment and operational limits

A standalone deployment or unsupported transaction configuration can still support
opaque CRUD, Native, and validated single-update expressions. It cannot advertise
arbitrary program AtomicTransform. Reject it with UNSUPPORTED + NOT_STARTED rather
than inventing schema metadata or implementing a non-atomic read/replace.

Collection/index creation, sharding configuration, and transaction preparation are
operator responsibilities. Weir does not create indexes/collections in the transform
algorithm to make an unsupported request succeed. Transaction limits, driver session
pinning, and bounded server selection must be qualified against supported versions.

## 12. Elasticsearch and OpenSearch Atomic RMW

For a supported concrete index with native optimistic concurrency enabled and a
usable stored `_source`, the adapter uses `_seq_no` and `_primary_term` internally.
Elasticsearch documents these as the conditional-write pair [D2]; OpenSearch exposes
the corresponding conditional parameters [D6]. Validate each backend/version rather
than assuming every Elasticsearch-compatible configuration supports them.

Record-write qualification also covers effective ingest behavior. An ordinary
index request can inherit a default ingest pipeline that changes its destination
or source; this is separate from a client calling Weir Native [D8]. The initial
adapter profile uses direct record writes: reject caller-supplied
pipeline/routing overrides, bypass default pipelines with the qualified
backend's explicit no-pipeline option, and require no effective final pipeline
for source-writing operations. Elasticsearch's `pipeline=_none` bypasses the
default but not the final pipeline [D9]. Apply this to each physical bulk item
and every AtomicTransform write attempt, not just unary Put.

This is a deliberately narrow V1 record-write profile, not a ban on pipelines or
on Native within the authorized Store. A configured default pipeline may remain
in place for native writers; Weir does not edit index settings or pipeline
definitions. Read, Delete, and Scan are not disabled merely because a pipeline
exists. An effective final pipeline makes this initial source-write profile
unsupported; it is not claimed that every final pipeline retargets records.
Elasticsearch specifically rejects final-pipeline attempts to change `_index`
[D8]. A future qualified non-retargeting pipeline profile needs its own
source/transform contract, not a Core pipeline interpreter.

Validate effective settings during bounded startup or under an execution permit
before writing; unable-to-verify means UNSUPPORTED, not optimistic success.
Capability caches are bounded. Concurrent changes to the qualified
ingest/routing/source configuration are outside this profile and require
revalidation/restart, just as collection-layout changes do for MongoDB.
OpenSearch's exact bypass/update behavior must be qualified independently [D14].
Pipelines on update/expression paths that cannot be bypassed must be absent;
never assume an Index API option applies to an Update API.

```text
GET exact record -> source and native sequence/primary-term tokens
decode -> deterministic program -> validate output
conditional index/delete using those tokens
  acknowledged -> APPLIED
  definite version conflict -> read again, re-run program, retry within bounds
  ambiguous send/response -> UNKNOWN, stop
```

If GET reports absence and the transform produces a document, use create-only
semantics, not an unconditional index/upsert. A definite create conflict restarts
the read/transform loop. If it produces Keep or Delete, absence is a successful
no-op observation. Other backend validation errors are terminal, not transform
conflicts. An ordinary Replace must also avoid creating a concurrently deleted
record; use conditional native behavior, not unchecked index-as-upsert.

The same bound on attempts, aggregate fuel, deadline, and short conflict backoff
applies. No generic retry-on-network-failure or blind `retry_on_conflict` setting
substitutes for the adapter's evidence-aware algorithm. Supported backend scripts
can implement the expression fast path, but Weir does not transpile arbitrary Lua
or claim that Painless is a portable runtime.

Native tokens do not enter Core or become generic client-visible revisions. They
remain available in Native replies. `_source` disabled, incompatible
synthetic-source behavior, source pruning that loses fields, unavailable OCC, or
unsupported routing, index, or ingest configurations disable the general
transform capability; do not reconstruct a lossy current document from stored
fields. This includes the documented Elasticsearch configuration that disables
sequence numbers [D2]. Namespace deletion/recreation during an operation is
outside the record concurrency contract; a stale token is never treated as
globally unique across index generations.

For clarity, V1 ordinary Replace can read once and issue one conditional complete
replacement; a definite conflict returns NOT_APPLIED + CONFLICT rather than entering
a new generic retry loop. The general transform path is the operation that explicitly
permits re-evaluation on conflicts.

The initial search backend-expression profile is
`application/vnd.weir.search-update.v1+json`: the native update body with exactly one
`doc` object, targeting an existing record, with `doc_as_upsert=false` and no scripts,
server-generated timestamps, or caller-provided retry count. Native backend merge
semantics apply; absence is NOT_APPLIED with PRECONDITION_FAILED. Broader scripts
remain Native-only. General program transforms still use explicit GET/OCC rather
than translating into this expression profile.

By default, APPLIED means backend acknowledgement, not search refresh. A caller
can request native refresh behavior via adapter options when supported. It
affects batch compatibility and observed latency, but not a separate
adaptive-control bucket. A transport timeout while waiting for a combined
index/refresh reply may still be UNKNOWN. Only separate positive write evidence
permits APPLIED plus a later visibility error. There is no generic
`WAIT_UNTIL_VISIBLE` claim covering every read path or replica.

## 13. Native and Scan Execution

### 13.1 Native is stateless access, not a tunnel

Native executes one adapter-defined interaction against the configured Store. Its
descriptor is typed, bounded, and opaque to Core. Initial adapter descriptor profiles:

| Adapter profile | Descriptor/body contract |
| --- | --- |
| `application/vnd.weir.mongodb-command.v1+protobuf` | Descriptor identifies an ordered BSON command body; database comes from the URI. Entire command is bounded and validated before execution. |
| `application/vnd.weir.search-http.v1+protobuf` | Descriptor contains method, resource-relative path, encoded native query parameters, and repeated allowed headers; body is a native byte stream. Response metadata retains native status and allowed repeated headers. |

The descriptor schemas belong to adapter documentation/modules, not a Core switch.
For the MongoDB V1 descriptor, no extra fields are needed: its media type identifies
the profile and descriptor data is an empty Protobuf message; the body carries the
command. For search V1, reserve fields 1 method:string, 2 path:string, 3 query:string,
4 headers:repeated Header, with Header fields 1 name:string and 2 values:repeated
string. Search response metadata uses fields 1 status_code:uint32 and 2 headers:
repeated Header. Body media types are explicit; metadata is not smuggled into user
documents. These profiles are native, not new portable database semantics.

Adapters validate credentials, destination/resource scope, supported operation class,
body limits, and statelessness. Never trust a client `read_only` flag. Unknown command
effects are conservatively may-write. HTTP verbs alone are not sufficient because
a POST can be a read query and a native command can have embedded writes.

For HTTP backends, fix the upstream host/authentication from configuration; reject
absolute URLs, authority overrides, traversal escapes, redirects requiring a new
upstream request, hop-by-hop/auth/Host headers, and remote-fetch commands that could
create an SSRF path. Return redirects as native replies without following them.
Native can touch multiple records/datasets within its authorized Store when the
backend permits, but never another Weir Store or arbitrary remote endpoint. It does
not gain a cross-record atomicity guarantee.

For MongoDB, preserve ordered BSON commands and native response bytes. Reject
client-managed sessions, transaction control across RPCs, `getMore`/cursor ownership
that escapes the call, watch/change streams, and other stateful operations. Weir's
internal session for one AtomicTransform is different from exposing a client session.
Use Scan for supported cursor traversal. Native data-plane updates need not preserve
any Weir metadata, because none exists. V1 excludes schema/index/cluster administration
and unbounded server-side background jobs rather than becoming an operational tunnel.

### 13.2 Native request and response bounds

MongoDB commands that must be materialized have a hard total command cap. They are
not made arbitrarily scalable merely by splitting the input into chunks. Reject an
oversized monolithic command before any backend write. Large record workloads use
Bulk, and live query output uses Scan.

A supported HTTP Native operation may stream a body larger than one frame using
backpressured chunks. The adapter must validate its descriptor before forwarding
and enforce a configured maximum logical body/lifetime where required. If native
item validation needs body inspection, validate each bounded item before forwarding
that item; do not claim whole-command validation without buffering the whole command.
Previously forwarded native items may already have applied when a later item or
chunk is invalid. Either support those explicit partial semantics or reject the
streaming profile; never silently buffer the entire upload.

Replies preserve native body bytes, native errors, and ordered chunks. Inspect
only what is required for bounded framing, authorized scope, cleanup, and
positively known backend congestion; do not add a command-effect result
interpreter. A complete HTTP reply, including a 200 response, is transport
completion, not a promise that every native item succeeded. The caller
interprets native status/body and per-item errors.

### 13.3 Native response completeness, not effect normalization

NativeEnd uses NativeCompletion to report only Weir-observable transport facts:

| Completion | Meaning | Failure field |
| --- | --- | --- |
| NOT_STARTED | The requested native backend command was never dispatched. | Required: preflight/admission/cancellation failure. |
| RESPONSE_COMPLETE | The entire native response and its required framing were received and forwarded before End. | Absent; backend errors remain in native status/metadata/body. |
| RESPONSE_INCOMPLETE | Dispatch may have happened, but no complete native response can be delivered. | Required: bounded Weir transport/framing/limit/cancellation error. |

Missing End or a non-OK transport termination is incomplete from the receiver's
perspective, even if a downstream node observed a complete response. Do not
synthesize NOT_STARTED from an ordinary timeout/disconnect. A backend's early
complete rejection can be RESPONSE_COMPLETE even before the upload half-closes;
pumps must stop the remaining upload safely under the native protocol rules.

A complete native error response is not a Weir Failure and a complete native
bulk reply is not proof that all effects succeeded. Do not provide Native
APPLIED, NOT_APPLIED, PARTIALLY_APPLIED, or read-only effect enums; record
MutationOutcome is a different contract. Read-only classification may remain
internal for command authorization/capability checks, never as a client-supplied
safety assertion.

Weir never automatically replays Native. After possible dispatch, partial side
effects or commit ambiguity are interpreted by the caller using backend-native
semantics. Response completeness neither authorizes retries nor turns an
uncertain effect into non-application. A relay forwards NativeEnd without
parsing the native body.

### 13.4 Minimal live Scan

Scan standardizes traversal delivery: resource, optional native selector,
representation, bounded fetch hint, streamed documents, and a terminal count/error.
An adapter defines the selector format and record representation. MongoDB may use
an ordered BSON find/read-only aggregate selector; search may use a JSON selector.
Weir does not provide portable projection, sorting, count, pagination, or filters.
Selectors that mutate data (such as output stages) are not Scan.

MongoDB Scan emits each native result document in BSON. Search Scan emits each
native hit as JSON, retaining its native hit metadata; this is not necessarily the
same shape as Read's `_source` document. The adapter profile documents that difference.
No wrapper or metadata is inserted into stored user data. Native response semantics
are intentionally not flattened into a fake universal search-row representation.

The completeness rule applies to every Scan adapter. MongoDB Scan rejects
`allowPartialResults=true`, keeps partial results disabled, and treats any
`partialResultsReturned` indication or cursor/shard error as traversal failure,
including on later getMore replies [D16]. It cannot silently return the
available shards as a complete result. Cursor controls and live/tailable modes
are adapter owned or rejected, not unchecked selector options.

The adapter owns a cursor/PIT only for the live RPC, pinned to its
Store/endpoint. One scheduler-dispatched open/fetch step obtains at most one
bounded page; release the execution permit before client emission, retaining the
page under the live-session budget. Fetch another page only after it drains and
output credit is available, using the same reserved continuation entry (section
7.5). Native state closes on completion, cancellation, or expiry. A fetch hint
is capped by adapter count/byte limits. Do not collect all hits, run count
first, or retain an unbounded array of resume positions.

Search Scan must establish **complete traversal of its declared selector**, not
just extract hits from successful HTTP replies. V1 uses a qualified
hit-traversal profile: query/projection and supported ordering remain native,
but adapter-owned pagination, PIT/cursor state, fetch size, and completeness
controls cannot be overridden. Reject selectors requesting partial results,
early termination, caller-owned pagination, or non-hit outputs such as
aggregations that this stream would silently discard.

On PIT/cursor creation and every fetch, disable partial results wherever
supported (`allow_partial_search_results=false` for qualified search requests),
inspect the full response envelope, and require successful shard participation,
`timed_out=false`, no unexpected early termination, and intact response framing
[D10, D15]. The exact native fields vary by backend/version and belong in
adapter conformance tests. Missing or unverifiable required evidence is failure,
never a successfully empty page. Do not mistake ordinary query-skipped shards
for failed/missing shards.

Retain and validate at most one bounded page before emitting its hits, so
metadata that follows the hits cannot escape inspection. A failed page ends the
stream with a nonempty ScanEnd.failure; previously emitted pages remain partial
results. The count is only the number actually delivered, not proof of
completeness. Timeouts map to DEADLINE_EXCEEDED, temporary shard failures to
UNAVAILABLE, malformed/contradictory responses to INTERNAL; do not silently
retry/restart traversal. For these failures, use gRPC OK only when delivering an
explicit terminal failure envelope; otherwise terminate with non-OK transport
status. ScanEnd without Failure is reserved for verified native exhaustion.
Native continues returning its full native envelope and does not inherit this
Scan-specific completeness contract.

V1 deliberately omits cross-RPC resume tokens. A portable token would raise snapshot,
expiry, authorization, mixed-key pagination, replica affinity, and cleanup questions
without a shared backend answer. A live MongoDB cursor avoids inventing unsafe `_id`
keyset pagination across mixed BSON types. Search can retain PIT/search-after state
inside the same live call when its qualified profile supports it. These do not make
Weir an application session service.

No portable snapshot guarantee: each adapter documents its native visibility and
concurrent-modification behavior. Any resource/snapshot expiry or connection failure
ends the traversal with partial results and a failure; never silently restart or
merge a new traversal. An application requiring resumability or snapshot export must
use documented backend-native capabilities, or restart and handle duplicates/misses
itself. A future resume feature needs a separate requirement and protocol review.

## 14. Peer Forwarding and Remote Services

### 14.1 Reuse the public RPCs on authenticated peer listeners

RemoteWeir calls the same Read, Mutate, Bulk, Native, and Scan RPCs in
`weir.v1`. Unary stays unary and streaming preserves its original shape.
Public/peer listeners use the same messages and validation/execution paths;
there is no ForwardOpen, ForwardRequestFrame, ForwardResponseFrame, or
`weir.mesh.v1` service to maintain.

The only extra forwarding state is trusted gRPC metadata [D11]:

| Context | Rule |
| --- | --- |
| `weir-remaining-forwards` | Exactly one canonical unsigned decimal value, 0-8, required on peer calls; reject duplicates/malformed/missing values. |
| Public ingress | Reject this reserved metadata from applications and create the configured initial budget internally. |
| Peer ingress | Authenticate a Weir identity, authorize Store and semantic operation family, and preserve/decrement the received budget; never initialize a fresh one. |
| Request ID, tracing, deadline | Use the existing bounded metadata/deadline mechanisms; IDs remain diagnostic, not sequencing or authorization identities. |

A peer trust policy is not inferred from the presence of a header. V1 uses
separate application/peer listeners and mTLS or equivalent peer authentication.
Forward only approved context, not original authorization headers or arbitrary
baggage. Each peer revalidates requests and authorizes each operation inside
Bulk. Data follows the same Service boundary and terminal result rules as a
direct application call.

### 14.2 Deadline, hop, and error rules

Application ingress creates the hop budget (proposal: four remote forwards,
configuration maximum eight). Ordinary clients cannot supply peer hop metadata.
Before each remote dispatch, require a positive budget and decrement it exactly
once. At zero, local execution is still allowed but another forward is rejected.
Do not reset the budget at a new listener or infer safety from network topology.
No unbounded visited-node list, distributed route discovery, or consensus is
needed.

Each hop maintains the original effective deadline by propagating remaining time,
including time already spent waiting locally. Client cancellation cancels downstream
receives/sends and backend contexts where possible, but never proves rollback.
A peer disconnect after a mutation may have been delivered maps to UNKNOWN unless
a complete stronger result arrived first. Do not convert a received APPLIED into
NOT_APPLIED because the outer response later failed.

A forwarding-only node applies the process overload latch to each new operation
and bounds live relays/outstanding I/O. It has no local Store pending queue or DB
admission controller. On pressure it stops consuming new operations; it must still
forward the bounded body/results of an already-admitted Native exchange.

Forward streaming frames immediately with at most one bounded frame per
direction. Unary forwarding retains only its bounded request/result and their
gRPC status. Preserve NativeCompletion and ScanEnd.failure unchanged. Do not
restore order, aggregate an entire stream, re-batch at remote-only nodes, or
parse opaque document/ native bytes. Only the final local StoreRuntime batches.

### 14.3 Endpoint selection and affinity

RemoteWeir has a finite configured list of stable endpoint identities, bounded
channels, basic health/connectivity state, and simple bounded reconnect backoff.
Use ordinary DNS resolution for configured names; this is not a dynamic route
Provider or control plane. No Envoy-style outlier scoring, hedging, or ring ownership.

For single-record Read/Mutate, choose the highest rendezvous score over
canonical URI bytes and eligible endpoint identity (fixed documented hash and
byte framing). This is a locality optimization, not serialization. Native/Scan
can hash the resource; Bulk hashes the bounded request ID and pins one endpoint
for the entire stream. Bulk intentionally cannot promise record affinity for
every record in a multi-record stream without introducing fanout. No public
affinity hint is necessary in V1.

Select an available endpoint before dispatch. If connection establishment fails
with proof that no semantic request was sent, another endpoint may be selected
within the same deadline. Once a mutation or any part of a stream may have been
forwarded, do not switch endpoints and replay it. Read replay remains an explicit
policy only before any result has been delivered. Basic health checks can help
select *future* calls; they are not proof about an in-flight mutation.

All endpoints in a RemoteWeir Service must represent the same logical Store,
authorization policy, adapter semantics, and supported protocol profile. Weir
does not reconcile inconsistent replicas. Validate deployment configuration and
compatibility during rollout; UNSUPPORTED is preferable to silently changing intent.
Endpoint loss or membership change may destroy affinity but cannot invalidate
database-native correctness. Typical deployments remain one or two Weir hops.

## 15. Graceful Lifecycle, Health, and Observability

### 15.1 Shutdown state machine

Runtime states are Constructing -> Serving -> Draining -> Closed. Draining is
monotonic and begins from one process-owned barrier:

1. Mark readiness NOT_SERVING and reject new application/peer calls and new Bulk
   operations. Stop new connection acceptance, preserving existing result paths.
2. Continue dispatching already admitted finite pending work while deadlines and
   the global drain deadline permit. Flush collection windows rather than wait
   merely to make a larger batch. Do not require peer/client half-close to finish
   draining an unlimited Bulk producer.
3. A received but not admitted operation can receive NOT_STARTED + UNAVAILABLE.
   Do not read an unbounded input stream merely to manufacture such results. Finish
   known accepted results, then close draining streams; callers conservatively
   classify unreported sent mutations as UNKNOWN.
4. Existing Native uploads/Scan cursors belong to an admitted operation. Allow Scan
   fetch continuations on their existing reservation while normal/drain deadlines
   permit; they are not new admission. No cursor is migrated.
5. At the drain deadline, remove undispatched work with NOT_STARTED if it can still
   be reported, cancel active I/O, and close cursors/sessions with a short finite
   cleanup allowance. Never turn cancellation into presumed rollback.
6. Allow bounded result delivery, close peer connections, and call each adapter's
   Close exactly once to close its backend clients; then close diagnostic listeners. Join only bounded goroutines and force-close
   after the shutdown cap; do not wait indefinitely on a malicious client.

The common phrase "stop scheduling, then drain" is misleading here: stopping all
dispatch would strand admitted work. Stop **new admission**, continue scheduling
the admitted set, then stop execution at the drain deadline. No requeue to another
node, background mutation retry, persistence of accepted work, or queue settlement.
Process crash loses in-memory work; database effects and missing results may be
ambiguous. SIGKILL is not a graceful-drain protocol.

### 15.2 Health

Liveness means the process is operational, not that every database is healthy.
Readiness means validated assembly can serve requests and is not draining; routine
Store saturation, controller cooldown, or a brief memory latch must not cause
readiness flapping/restart cascades. Expose Store-specific health/capability state
separately so one failed Store does not hide all other useful routes.

Prefer recent execution outcomes and basic transport health. Bounded administrative
connectivity checks may exist independently, but no synthetic Ping increases the
database concurrency window. Permanent startup authentication/topology errors fail
that required Store's construction clearly. Partial-Store startup is not a hidden
fallback; V1 validates all configured local Stores before serving.

### 15.3 Metrics and traces

Use Prometheus and OpenTelemetry integration, not a custom telemetry platform.
Bounded labels are listener/method, configured Service/Store/adapter, operation
class, finite error/outcome, and finite controller-reduction reason.

Record:

- Pending/reserved operations/bytes; active executions and live stream sessions;
  Scan fetch/emit state; output-credit use;
  admission rejection reason; queue wait and collection delay.
- Physical batch size/bytes, fill ratio, application-operations-per-backend-call,
  singleton fallback, and completion latency.
- C/current active work, increases/decreases/cooldowns, backend-only latency,
  backend error class, conflict attempts, and unresolved mutation outcomes.
- Stream bytes/frames, NativeCompletion and Scan failures, input/output stall time,
  truncation/cancellation, cursor cleanup, and remote hop latency/failures.
- Process memory budget/usage/latch, backend pool use/waits, transform limits/cache
  use, lifecycle state, drain duration, and forced termination count.

Never label metrics by record ID, arbitrary resource URI, query, transform source
or digest, document fields, request ID, endpoint churn, or error text. Fixed endpoint
identity may be logged under a bounded configured set, not turned into an arbitrary
metric dimension. Aggregate histogram buckets are configured finite sets.

Trace spans separate queue wait, backend calls, transform time, emit wait, and remote
forwarding. Request IDs and operation indexes may appear in sampled structured logs
and traces, not metric labels. Payloads and secrets are not logged by default; error
messages are bounded and redacted. A final server-side APPLIED log is diagnostic,
not a durable client-accessible operation receipt.

## 16. Proposed Repository and Module Layout

Start with one repository and one Go module, `github.com/batchstream/weir`. Do not
create another protocol repository merely because Sink has one. Public schema and
generated types remain cleanly separated so SDKs can consume them without importing
the server; a later repository split is packaging, not a V1 architecture dependency.

```text
cmd/weir/                       CLI, process signal ownership
proto/weir/v1/                  public schema source
protocol/weir/v1/               generated public messages/client service stubs
resource/                      canonical URI syntax/build/parse, no backend grammar
internal/
  app/                         validated assembly and lifecycle
  config/                      strict static decoding/defaults/validation
  transport/                   public/peer handlers, trusted metadata, bounded pumps
  service/                     exact routes, LocalStore and RemoteWeir variants
  store/                       StoreRuntime, Work/result contract, scheduler,
                               batch selection, adaptive algorithm, adapter contract
  backend/
    mongodb/                   owned client/pool, URI grammar, codec, transaction RMW
    search/                    shared tested ES/OpenSearch mechanics and profiles
  transform/                   value model, codec/runtime contracts, bounded Lua profile
  overload/                    process memory sampler and hysteresis latch
  observability/               bounded metrics/tracing/log integration
docs/                          architecture and later adapter/operator contracts
```

This is a layout proposal only; the listed code directories are not created by
this task. `app` imports concrete backends to construct them. Backends depend on
the small execution contract in `store` and the value/runtime contract in
`transform`; `store` does not import concrete backends. Transport maps generated
messages into the small semantic work/result types and imports no BSON/JSON
packages. RemoteWeir uses generated public RPC clients without creating local
backend plans. Adapters own their clients; StoreRuntime owns adapter
construction/drain/Close, not driver internals.

Use real interfaces only at the two Service variants, backend adapters, codec/runtime
boundaries, and standard external dependencies. Do not add function-variable test
hooks, a generic retry package, lane framework, plugin manager, provider registry,
or thin wrapper layers with one implementation. Test scheduling with deterministic
fake adapters/clock inputs at these genuine boundaries, not production functions
assigned to replaceable globals.

## 17. Evidence-Based Sink Component Review

### 17.1 Inspected snapshots and method

The design follows source inspection of both requested repositories:

- `batchstream/sink` at `a08a1c53c5de2045176be3910197f73d5fe139b9`.
- `batchstream/sink-protocol` at `31943c4a6984468bc57aca348723ab812d26b842`.

These are pinned snapshots, not claims that a branch can never advance. Inspection
covered schemas, URI codecs, routing/forwarding, batch/scheduler code, feedback
control, memory guard, storage interfaces, Mongo metadata/native-write handling,
search OCC, Lua execution boundaries, streaming, and assembly/lifecycle. Repository
tests were inspected selectively; no application tests or backend experiments were
run for this design-only task. Implementation feasibility and backend qualification
gates remain explicit in section 20.

The source, rather than stale prose, resolves disagreements. For example,
`sink/docs/architecture.md` describes record responses in request order, whereas
the pinned protocol explicitly permits out-of-order indexed results [S1, S2].
Weir takes the bounded out-of-order contract, not the obsolete ordered explanation.

### 17.2 Disposition matrix

"Preserve mostly" refers to a sound behavior/algorithm or reusable test insight;
it is not a promise to copy Sink packages unchanged.

| Existing component/evidence | Disposition | Would invent today? Decision and changed ownership |
| --- | --- | --- |
| Strict URI parser/formatter and typed key round-trip tests [S1] | Preserve mostly | Yes: one canonical identity prevents aliasing. Rename scheme, specify cross-language escaping, keep backend key grammar out of generic routing. |
| Logical identity distinct from storage BatchKey [S4] | Preserve mostly | Yes: locality must not redefine the record. Preserve invariant and add adapter-alias tests. |
| Real-work feedback, congestion reduction, stale-flight epoch protection [S3] | Preserve algorithm, move ownership | Yes: StoreRuntime owns one controller; delete its independent admission queue and role interfaces. |
| Comparable latency buckets and exclusion of emit waits [S3] | Defer controller complexity; preserve measurement separation | Backend I/O and consumer stalls differ. V1 uses explicit-feedback AIMD; no latency buckets/dual EWMAs without evidence. |
| Process memory high/low watermark [S8] | Preserve mostly, strip roles | Yes: simple hysteresis is useful; once per new operation per process, including new frames of a long-lived Bulk. |
| Bounded cross-call batch formation [S4] | Preserve algorithm, rewrite scheduling | Yes: coalesce small work. One FIFO/ledger, no per-RPC batching architecture or secondary ready-ticket queue. |
| Shared-batch caller-deadline handling [S4] | Preserve lesson, rewrite contract | Yes: one caller must not cancel unrelated work; retain result credits and distinguish stream lifetime from Scan fetch permits. |
| Normalized storage error categories [S4] | Rewrite | Yes to stable error classes, no to `retryable` as safety. Separate outcome evidence and congestion feedback. |
| Public completion modes, central encoding enum, batch-native repeated requests [S1] | Rewrite | No to durable acceptance/central enum/unbounded logical input. Media types, unary record calls, bounded Bulk framing. |
| Seven public RPCs including Query and Count [S1] | Rewrite/delete | Four semantic operations plus Bulk framing; backend query/count stays Native. |
| Gateway route grouping, mixed-Store fanout, result aggregation [S2] | Delete | No: applications compose multiple Store calls themselves. RemoteWeir pins one Service/endpoint. |
| Gateway/Engine/Worker roles and role-specific assembly [S2, S8] | Delete | No: configuration chooses topology, never program architecture. |
| Private Gateway-to-Engine protocol [S2] | Delete separate message protocol | Reuse public RPCs on authenticated peer listeners with trusted hop metadata. No Forward wrappers or Engine service. |
| Rendezvous affinity [S2] | Preserve algorithm, move ownership | Yes as optional locality in RemoteWeir; never record ownership or correctness. |
| Dynamic route snapshots, discovery refresh machinery [S2] | Delete from V1 | No requirement; static endpoint sets, ordinary DNS and rolling restart suffice. |
| Core revision-based merge orchestration [S5] | Rewrite | No portable CAS layer: move atomicity and retries into adapters; Core only schedules AtomicTransform. |
| Mongo hidden revisions and native-write revision protection [S5] | Delete | No: documents belong to users; transactions/native expressions provide correctness. |
| Search native sequence/primary-term conditions [S7] | Preserve mechanism, move ownership | Yes: tokens stay inside adapter, never a fabricated universal revision. |
| Per-record merge/Put folding [S5] | Delete from V1 | No: independent operation acknowledgement and ambiguous-outcome boundaries are more important than one fewer write. |
| Bounded Lua instructions, stack, output, immutable program caching [S6] | Preserve lessons, rewrite boundary | Yes to bounded pure evaluation; shared Value model replaces runtime-specific BSON/JSON bridges. |
| Separate Lua JSON/BSON bridges [S6] | Rewrite | No N*M coupling: codec-to-Value plus runtime-to-Value. |
| Native adapters and incremental search response decoding [S7] | Preserve bounded transport; remove effect normalization | Native preserves raw replies and reports completeness. Scan separately validates each bounded page before emitting hits. |
| Query/count/page collection and portable Scan continuation shape [S1, S7] | Delete/reduce | No universal paging. Scan owns native cursor state only for one bounded live call. |
| Bounded metrics and centralized lifecycle [S8] | Preserve mostly, simplify | Yes: use Store/Service dimensions, not fixed roles or Kafka settlement. |
| Kafka Publisher/Consumer, Worker, topics, retries, DLQ, offsets, queue codec [S9] | Delete | No: entirely outside the product. No replacement Queue interface. |
| Generic retry manager, distributed ordering, lane framework, xDS/Provider framework, complex outlier detection | Do not introduce | These are rejected concepts, not a claim that every one exists in the inspected source. None protects a promised V1 property. |

## 18. Abstraction and Portability Audit

### 18.1 Five-question review of retained concepts

Each row answers: problem, protected invariant, why the next-simpler approach fails,
introduced complexity, and whether removal preserves the required properties.

| Abstraction | Problem and invariant | Why simpler is insufficient | Introduced complexity | Removable? |
| --- | --- | --- | --- | --- |
| Listener | Transport/auth/framing; finite externally controlled input | Direct database calls cannot enforce the shared data-plane boundary | gRPC handlers and limit configuration | No; a transport boundary is required, but a custom protocol is unnecessary. |
| Route -> Service | Local/remote composition; one Store target per request | Fixed roles require separate program architectures and prevent mixed topology | Exact map and two concrete variants | No under required multi-hop/local composition. |
| StoreRuntime | Isolate queue/window/pool/batch state per Store | A process-global executor allows one Store to consume all another Store's capacity | One runtime object/state machine per Store | No for local Store isolation. |
| Work/adapter plan | One scheduling path with opaque backend compatibility | Separate method queues duplicate admission and cannot coordinate keys | Small internal tagged work/result and opaque plan | Cannot remove common scheduling representation; it need not be a public API or large framework. |
| Scheduler/Bulk sequence index | Bounded work and required within-stream same-key order | Goroutines waiting on semaphores hide queues; global Read serialization creates hotspots | One ledger/FIFO, one charged Scan continuation, stream-scoped sequence references | Retain for the promised Bulk order; no V1 cross-client key lock. |
| BatchKey/collection algorithm | Automatic physical batching without identity collapse | Application-only batches miss concurrent small requests | Compatibility token comparison and short timer | Required batching would be lost; no standalone queued BatchBuilder is needed. |
| Adaptive window | Respond to explicit backend congestion | Static concurrency ignores observed overload | AIMD, one epoch and short cooldown; Cmin=1 | Keep the adaptive baseline; latency buckets/dual EWMAs and distributed quotas are unnecessary in V1. |
| Process overload latch | Shared process exhaustion beyond Store isolation | Per-Store limits alone do not react to aggregate RSS/driver overhead | One sampler and hysteresis bit | Required overload response would be lost; predictive heap model unnecessary. |
| Bounded session/result delivery | Slow readers and duplex flow control; finite retained results | HTTP/2 alone does not bound already consumed work/results | Finite credits, one/few frames, small fixed pump count | No without risking unbounded buffering or blocking shared executors. |
| Opaque Document/media identity | Preserve caller representation across all hops | JSON-first or implicit encoding silently changes types/tags | Two fields and bounded canonical media validation | No without losing encoding correctness; central format enum unnecessary. |
| Value/TransformCodec/runtime boundary | Lossless usable transforms independent of encoding/runtime | Opaque Int32 cannot support ordinary numeric transforms; direct Lua/BSON bridges couple layers | Small tagged model including Int32/Int64, checked arithmetic, lossless codecs | Needed for general transforms; absent entirely on opaque-only adapters. |
| AtomicTransform | Atomic single-record RMW despite independent writers | Local locks cannot serialize native clients/other nodes | Adapter-native transaction/OCC loops and evidence classification | No for required atomic transform capability; no distributed locks needed. |
| Native descriptor/stream | Preserve native semantics without tunneling client sessions | A query DSL or native-effect interpreter duplicates backend semantics | Adapter-defined descriptors, bounded bytes and response completeness | Keep the escape hatch; remove command-effect normalization. |
| Scan | Deliver complete native traversal with bounded cursor lifetime | Flattened hits hide partial-response metadata; holding a permit for idle clients mixes resource lifetimes | One bounded session/page and shared-scheduler fetch continuation | Keep the live traversal scope; no resume service, separate queue, or reserved short-work floor. |
| Peer forwarding/hop bound | Preserve intent/results/deadline across process boundaries | Authentication and loop bounds are needed, but another schema is not | Existing RPCs on a peer listener plus trusted metadata | Delete mesh-specific message grammar; keep authentication and hop enforcement. |
| Immutable assembly/lifecycle | Unwind startup and drain finite work consistently | Scattered constructors/cleanup leak resources and obscure ownership | One constructor owner and monotonic lifecycle states | Some ownership is necessary; dynamic configuration machinery is not. |

Within-Bulk same-key sequencing is a specified contract and cannot be removed
while claiming the same protocol semantics. Cross-request serialization is not
implemented in V1. Compile caching and rendezvous affinity are optional
performance choices; neither changes record correctness or receives an
ownership/control-plane protocol.

### 18.2 Backend-independent versus native contracts

| Contract | Actually portable? Boundary |
| --- | --- |
| Opaque representation, bounded delivery, resource routing | Yes: byte/framing/execution properties independent of query language. |
| Per-record Read/Create/Replace/Put/Delete intent | Portable only as explicit capabilities with adapter-defined key/record/acknowledgement semantics; never emulate unsupported atomicity. |
| Mutation effect evidence | Yes: knowledge about execution is portable; the evidence is backend-specific. |
| AtomicTransform intent | Yes for capable Stores; transaction/OCC/expression implementation remains native. |
| Value model | A small transform interchange, not a universal native type system; Extended and rejection prevent loss. |
| Native | A bounded envelope and response-completeness contract for nonportable commands; effect interpretation remains native. |
| Scan | Delivery/lifecycle plus verified traversal completion; selector, ordering, visibility and snapshot meaning remain native. |
| Visibility/durability/revision/query/count | Not universal: no Core abstraction pretending otherwise. |

## 19. Final Simplification Pass

This pass is part of the proposal, not a future TODO.

| Risk searched for | Final resolution |
| --- | --- |
| Duplicated work queues | One Store pending set; batch/key indexes hold references only. Remote nodes have bounded I/O frames, not a second database queue. Driver checkout cannot accumulate beyond active permits. |
| Duplicated admission | One logical Store admission; conflict retries and reserved Scan continuations never re-admit. Live-session budgets bound state, not a second DB scheduler. Peers protect their own processes. |
| Role-specific logic | No runtime mode. LocalStore/RemoteWeir is a genuine routing destination choice. |
| Accidental compatibility | New URI scheme, API, outcomes and configuration; no Sink translators, modes, fields, import dependencies, or migration shims. |
| Core/encoding coupling | Core validates media spelling/counts bytes only; adapter/codec owns documents and options. |
| Hidden data ownership | No injected metadata, revision collection, side ledger, hidden ID rewriting, or native-write repair. |
| Artificial portable semantics | Query/Count/revision/completion visibility removed; backend options remain explicitly native. |
| Distributed coordination | No locks, leases, leaders, replica quota division, consensus, global ordering, or global capacity promise. |
| Speculative extension points | Static adapters/runtime profile, no hot reload, Provider, xDS, plugin loading, program registration, or SDK-wide retry framework. |
| Unnecessary protocol fields | No public BatchKey, local execution plan, Lua registry, affinity hint, client read-only flag, Native effect enum, or mesh-specific wrappers. Hop budget uses trusted metadata. |
| Features outside App -> Database | Async delivery, Kafka health/topics/offsets/DLQ/settlement, schema/index management, and background jobs are absent. |
| Hidden unbounded state | Finite sessions, frames, operation/result credits, pending set, keys tied to entries, active permits, cursor fetch, parser expansion, program cache, logs and shutdown waits. |
| Duplicated execution paths | Public/peer, unary/Bulk, and batching-on/off converge on one runtime. Scan fetch continuations use its scheduler while idle cursor state stays outside the execution window. |

Retain Bulk correlation, Native response completion, Scan terminal completeness,
and trusted peer hop metadata because each protects a named requirement. Remove
Native effect normalization, mesh-specific message framing, cross-client Read
chains, Delete affected-row distinctions, latency-bucket control, portable Scan
resume, operation folding, returned images, program lowering, and public
capability discovery. Adapter ownership is explicit; no extra pool or
session-manager framework is required.

## 20. Staged Plan After Architecture Approval Only

The original design task authorized no implementation. The 2026-09-26 milestone 1
request now authorizes the narrowed baseline, feasibility checks and single-node
MongoDB Read/Mutate/Bulk closure above. The table remains the larger staged plan,
not authorization to implement its remaining stages.

| Stage | Scope | Approval/exit evidence |
| --- | --- | --- |
| 0. Ratify contracts | Resolve design choices below, pin supported backend/runtime profiles and exact limit defaults | Written architecture approval; no implementation starts merely because this document exists. |
| 1. Protocol and semantic test vectors | Public schemas, URI/media rules, record outcomes, NativeCompletion, ScanEnd, peer metadata | Invalid variants/states; stream completion; Native database errors versus transport failure; public-header spoofing and malformed/missing peer hop tests. |
| 2. Single-node runtime | Static assembly, one scheduler/ledger, stream-scoped sequencing, explicit-feedback AIMD, adapter-owned pools | Dispatch/cancellation races; concurrent independent Reads; ordered same-key Bulk; Cmin=1 and Scan reservations; single Close; no duplicate charges; bounded results; offline `go test ./...`. |
| 3. Mongo opaque CRUD and general transforms | Native primitives, Delete batching without affected-row distinctions, transaction RMW, lossless codecs/runtime | Native writer conflicts, insertion races, commit ambiguity; Int32 arithmetic/overflow and width preservation; bounded attempts/fuel; standalone rejects general transforms. |
| 4. Search adapter | Qualified ES/OpenSearch identity, ingest/source profiles, native OCC/Create/Replace | Retargeting default pipeline bypass, effective final-pipeline rejection for initial source-write profile, Native unaffected; conditional conflicts/transport loss; source/sequence rejection; independently test both products. |
| 5. Streaming surfaces | Bulk, raw Native responses, complete-page Scan validation, bounded session state and shared-scheduler fetches | Partial-shard/timeout/early-termination failures; failed page not emitted; C=1 stalled Scan permits short work; Native stall deadline; early native errors, cursor cleanup, RSS plateau. |
| 6. Remote composition | Reused public RPCs, peer authentication/metadata, deadlines, affinity/basic health | Identical direct/forwarded semantics; unary remains unary; bounded streaming; spoofed/missing/duplicate hops; zero-hop local versus forward; lost results; no replay. |
| 7. Qualification and operations | Metrics, drain, packaging, secure listeners, documented adapter profiles | Explicit-feedback/stale-flight load tests with multiple controllers; no latency-only reduction; drain across Scan states; memory/CPU ceilings; adapter closes exactly once; bounded metrics. |

The specified native-expression fast paths ship only after their deterministic
whitelist/validator is proven; unsupported expressions are rejected meanwhile.
Do not implement speculative Lua lowering to satisfy a benchmark. If in-process
Lua cannot enforce allocation/helper/fuel isolation, leave program transforms
disabled pending an explicitly reviewed runtime decision, rather than quietly
shipping a weaker sandbox.

### 20.1 Required failure/conformance scenarios

| Scenario | Expected invariant |
| --- | --- |
| Pending limit or process latch before execution | NOT_STARTED, no backend attempt. |
| Unary request stalls before a complete decoded frame | No-data, partial-prefix and partial-body waits are bounded by the original deadline and input-stall budget. Actual progress may refresh only the stall budget; completed decoding must not time out later backend work as an input stall. |
| Unary handler returns while response is flow-controlled | Server lifetime and response-stall limits still cover encoding, DATA and trailers; no late complete OK. Delivery and RPC-processing ownership both finish before releasing the application slot. |
| Cancellation races scheduler dispatch | One state transition; no false NOT_STARTED after a possible send. |
| Mixed caller deadlines in a shared physical batch | One expiry does not cancel unrelated live callers; no replay of expired mutations. |
| One slow result consumer in a cross-client batch | Reserved bounded output; other clients can complete; no global Send blockage. |
| Mongo commit response dropped after commit | Same-transaction resolution only; UNKNOWN if unresolved; mutation applied at most once by Weir's retry algorithm. |
| Mongo conflict with native replace/update/delete | Fresh transaction and re-evaluation; no lost update or hidden metadata requirement. |
| ES conditional conflict vs connection loss | Conflict can re-read/re-evaluate; connection loss cannot trigger new mutation replay. |
| Native bulk has mixed database successes/errors | Preserve native response without effect normalization; RESPONSE_COMPLETE only for full delivery; no automatic replay. |
| Native disconnect/early backend rejection | Missing/truncated response is incomplete; a complete early native error stays RESPONSE_COMPLETE; never infer rollback. |
| Transform increments Int32 or edits a different field beside it | Checked Int32 arithmetic; unchanged Int32/Int64 retain widths, including Int64 values fitting 32 bits. |
| Int32 overflow, mixed-width operands, int64 > 2^53, Extended | Overflow fails before write; conversions are explicit; no JSON/float coercion or Extended arithmetic. |
| Malicious transform/selector/deep document | Allocation, fuel, depth, output and execution bounds enforce before uncontrolled allocation/work. |
| Cross-Store frame midway through Bulk | Reject that operation without fanout or rolling back earlier results. |
| Same key after an UNKNOWN predecessor | No promised database ordering; explicit client sequencing is required for conditional workflows. |
| Backend slows for minutes; upstream keeps producing | Pending/session memory plateaus; HTTP/2 backpressure reaches the client. |
| Scan consumer stalls at C=1 | Retained page/cursor consumes session budget, not execution permit; short work dispatches; no extra Scan entry or prefetch; stall closes cursor. |
| Native consumer stalls at C=1 | Exchange may occupy the sole permit until bounded cancellation; no false promise of reserved short capacity. |
| Search Scan page has timed_out/shard failure/early termination | Do not emit failed page; earlier pages remain partial; terminal Failure required, even on HTTP 200. |
| Mongo Scan requests partial results or loses a shard on getMore | Reject partial-result options before execution; cursor/partial-result failures cannot produce a successful ScanEnd. |
| Search record write meets retargeting default/effective final pipeline | Bypass the default or reject before write; initial source-write profile rejects effective final pipeline; Native policy is unchanged. |
| Ordinary Delete bulk includes missing records | Fully acknowledged successful items are APPLIED; no per-item deleted counts or pre-read required; ambiguous items stay UNKNOWN. |
| Independent same-key Reads versus same-key Bulk sequence | Independent reads can overlap; Bulk frame order remains mandatory within its stream, even with duplicate diagnostic request IDs. |
| Adapter construction failure or shutdown | Each constructed adapter closes once; runtime never separately closes its driver pool. |
| Peer cycle, forged/missing hop metadata, or expired deadline | Reject invalid trust context; finite forwarding with no hop/deadline reset. |
| Shutdown during queue wait/upload/transaction/commit/result Send | Finite drain; correct evidence; no detached replay or forever-waiting goroutine. |

Offline tests must not open browsers, contact production, or run long external
workflows. Live backend/fault-injection suites must be explicit opt-ins and use
isolated data. Compilation and mock tests are not evidence of native transactional
or streaming correctness. No production rollout/release is part of this task.

### 20.2 Decisions to ratify, not unresolved correctness holes

Recommended V1 choices are specified: four semantic operations plus Bulk; live
Scan with bounded per-fetch scheduling and complete-page validation; shared
public/peer RPCs; stream-scoped sequencing; Int32/Int64 transforms; direct
search source-write profile; raw Native semantics with response completeness;
explicit-feedback AIMD; static configuration; no post-images, resume tokens, or
automatic mutation replay. Approval may reduce capabilities further but must not
weaken outcome, atomicity, or bounded-memory invariants.

Qualification must select supported MongoDB/ES/OpenSearch versions and topology
profiles including ingest and partial-search behavior, a Lua implementation
satisfying typed arithmetic/resource limits, validation of native-expression
subsets, and measured defaults. These are release gates, not permission to fall
back to non-atomic writes or unbounded buffering.

## Appendix A. Source Register

Repository paths in this appendix are relative to the named pinned repository,
not to the new Weir project. The references support the review and backend facts;
all Weir behavior in this document is a proposed design, not a claim about existing
code. Official backend documentation was read directly or through its official
source repository; some documentation website fetches were unavailable, so the
MongoDB driver specification and Elasticsearch source documentation were used.

### Sink source groups

For S2-S9, the source root is
`https://github.com/batchstream/sink/tree/a08a1c53c5de2045176be3910197f73d5fe139b9`.

| ID | Inspected evidence and useful anchors |
| --- | --- |
| S1 | `batchstream/sink-protocol` at `31943c4a6984468bc57aca348723ab812d26b842`: `proto/sink/sink.proto:11` (RPCs), `:28` (encoding enum/document), `:41` (completion modes), `:65` (out-of-order results), `:199` (Failure); `uri/address.go:41` (strict parser), `uri/key.go:41` (typed-key formatting). |
| S2 | `docs/architecture.md:10`; `proto/forward/forward.proto:7`; `internal/gateway/routes.go:31`; `internal/gateway/record_stream.go:15` (Store/endpoint fanout); `internal/gateway/connections.go:81` (bounded transport/retry configuration); `internal/gateway/discovery.go:210` (affinity). |
| S3 | `internal/backpressure/controller.go:42`, `:275`, `:350` (window/epoch/latency/reduction); `internal/backpressure/storage.go:207` (emit waits and backend sampling); `internal/backpressure/admission.go` (ticket queue); `internal/backpressure/cold_start_regression_test.go:9`. |
| S4 | `internal/service/batcher.go:112`, `:235`, `:377`, `:474` (submission/selection/shared deadlines); `internal/service/stream.go:105`; `internal/storage/storage.go:14` (BatchKey and Storage), `:36` (error classification). |
| S5 | `internal/service/write_group.go:31` (revision-based merge loop); `internal/service/write.go:124` (Lua-specific merge parsing); `internal/service/convert.go:17` (central encoding conversion); `internal/storage/mongodb/bson.go:134` and `:180` (metadata injection); `internal/storage/mongodb/write.go:288` (revision predicate); `internal/storage/mongodb/native_writes.go:13` (native-write revision protection); `internal/storage/mongodb/conditional_bulk.go:19` (per-item versus aggregate conditional results). |
| S6 | `internal/merge/merge.go:23` (program/input contract); `internal/merge/lua_engine.go:19` and `:92` (resource defaults/cache/compile); `internal/merge/json_bridge.go`; `internal/merge/bson_bridge.go`; `internal/merge/lua_allocation.go`; `internal/merge/result_budget.go`. |
| S7 | `internal/storage/search/write.go:250` (native conditional fields); `internal/storage/search/revision.go`; `internal/storage/search/client.go:70` (endpoint/retry behavior); `internal/storage/search/stream.go:11` (incremental hits); `internal/storage/mongodb/native.go`; `internal/storage/mongodb/native_query_validation.go`; `internal/storage/scan.go`. |
| S8 | `internal/capacity/guard.go:72` (once-per-context admission), `:113` (hysteresis), `:128` (memory sample); `internal/metrics/store.go:13` (bounded labels); `internal/app/lifecycle.go:17` (role-dependent lifecycle), `:83` (Close); `internal/app/app.go`; `internal/config`. |
| S9 | `internal/queue`, `internal/queue/kafka`, `internal/worker`, `internal/service/publish.go`, `internal/app/kafka.go`, and the async completion fields in S1; all are outside Weir's scope. |

S1 source root:
`https://github.com/batchstream/sink-protocol/tree/31943c4a6984468bc57aca348723ab812d26b842`.
These anchors are investigation entry points, not authorization to reuse old package
boundaries. The architectural review distinguishes observed code from proposals.

### Official backend/transport references

- **D1 - MongoDB transaction specification.**
  `https://github.com/mongodb/specifications/blob/f5ba7f7aaa0417e35671a3ed7a8cd26be3a8e5ba/source/transactions/transactions.md`.
  Especially commitTransaction, transaction identity, retryable commands inside
  transactions, and error-reporting sections. This supports the distinction between
  restarting a definitely aborted transaction and resolving an unknown commit.
- **D2 - Elasticsearch optimistic concurrency control.**
  `https://github.com/elastic/elasticsearch/blob/a68c26a6a64a994ef98a2465dc25453df78a39f1/docs/reference/elasticsearch/rest-apis/optimistic-concurrency-control.md`.
  Native sequence/primary-term conditions and the sequence-number-disabled limitation.
- **D3 - gRPC retry.** `https://grpc.io/docs/guides/retry/`.
  Configured retry, transparent retry, replay/commit boundaries; do not mistake
  disabling an application retry policy for a universal transport no-retry claim.
- **D4 - gRPC deadlines.** `https://grpc.io/docs/guides/deadlines/`.
  Explicit deadlines and propagation of remaining timeout rather than fresh budgets.
- **D5 - gRPC flow control.**
  `https://github.com/grpc/grpc.io/blob/516c96d2c1c04a141a619b9ffa3c7573ebee4f72/content/en/docs/guides/flow-control.md`.
  Write buffering, application consumption, and duplex deadlock considerations.
- **D6 - OpenSearch update document API.**
  `https://docs.opensearch.org/latest/api-reference/document-apis/update-document/`.
  Native `if_seq_no`/`if_primary_term` condition parameters. The version-specific
  adapter conformance suite must still test the supported deployment profile.
- **D7 - MongoDB unique indexes and sharded collections.**
  `https://github.com/mongodb/docs/blob/4da4cc66f81e1aa701fb55ca8b6940851e409d0a/content/manual/manual/source/core/index-unique.txt`.
  The sharded-collection uniqueness restrictions require a qualified URI-to-record
  identity profile; a record key cannot omit part of a required shard identity.

- **D8 - Elasticsearch ingest behavior and index settings.**
  `https://www.elastic.co/docs/manage-data/ingest/transform-enrich/ingest-pipelines`
  and `https://www.elastic.co/docs/reference/elasticsearch/index-settings/index-modules`.
  Default/final pipeline selection and the prohibition on final-pipeline `_index` changes.
- **D9 - Elasticsearch Index API.**
  `https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-index`.
  `pipeline=_none` bypasses the default, not a configured final pipeline.
- **D10 - Elasticsearch Search API.**
  `https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search`.
  Partial-result controls, timeouts, shard failures, and response-envelope evidence.
- **D11 - gRPC metadata.** `https://grpc.io/docs/guides/metadata/`.
  Custom call metadata can carry bounded forwarding state; authentication remains
  a separate listener responsibility, not a property of an untrusted header.
- **D12 - MongoDB Go BSON encoding.**
  `https://www.mongodb.com/docs/drivers/go/current/data-formats/bson/`.
  BSON Int32/Int64 widths must survive structured transform round-trips.
- **D13 - MongoDB Go bulk operations.**
  `https://www.mongodb.com/docs/drivers/go/current/crud/bulk/`.
  Aggregate collection results differ from qualified verbose per-operation results;
  ordinary Delete need not expose affected-row distinctions.
- **D14 - OpenSearch Index Document API.**
  `https://docs.opensearch.org/latest/api-reference/document-apis/index-document/`.
  Pin and test the supported product/version's pipeline and conditional-write behavior.
- **D15 - OpenSearch Search API.**
  `https://docs.opensearch.org/latest/api-reference/search-apis/search/`.
  Qualify partial-result controls and complete-response checks independently of ES.
- **D16 - MongoDB find command and cursor responses.**
  `https://www.mongodb.com/docs/manual/reference/command/find/`.
  `allowPartialResults` can affect find and later getMore; `partialResultsReturned`
  must not be mistaken for complete Scan results. Native cursor batches have bounded
  document counts/bytes, with separate framing and driver allocation headroom.

## Appendix B. Required-Deliverable Coverage

| Brief item | Design location |
| --- | --- |
| 1. Product scope/non-goals | 1.1 |
| 2. Core principles | 1.2-1.3 |
| 3. Minimal architecture | Decision Summary; 2 |
| 4. Listener model | 2.1 |
| 5. Route/Service | 2.2 |
| 6. LocalStore/RemoteWeir | 2.2; 14 |
| 7. Single-node deployment | 2.4 |
| 8. Multi-node deployment | 2.4; 14 |
| 9. One-request-one-Service | 2.3; 5.3 |
| 10. StoreRuntime | 6.1 |
| 11. Scheduler | 7 |
| 12. Local sequencing/limits | 7.2 |
| 13. BatchKey/micro-batching | 3.1; 7.3-7.4 |
| 14. Adaptive concurrency | 8 |
| 15. Pending bounds | 7.1; 9.1 |
| 16. Process overload | 9.3 |
| 17. Public protocol | 5 |
| 18. Read/Mutate/Native/Scan evaluation | 5.1 |
| 19. Query/Count decision | 5.1; 18.2 |
| 20. Canonical URI | 3.1 |
| 21. Opaque documents | 3.2 |
| 22. Media types | 3.2 |
| 23. TransformCodec | 10.3 |
| 24. Value model | 10.2 |
| 25. AtomicTransform semantics | 10.1 |
| 26. Runtime boundary | 10.3 |
| 27. MongoDB atomic RMW | 11 |
| 28. ES/OpenSearch atomic RMW | 12 |
| 29. Record outcomes and Native completion boundary | 4; 13.3 |
| 30. Retry rules | 4.3; 11.3; 14.3 |
| 31. Native execution | 13.1-13.3 |
| 32. Request/response streaming | 5.1-5.3; 9; 13 |
| 33. Bounded memory | 9.1-9.2 |
| 34. End-to-end backpressure | 9.4 |
| 35. Peer protocol reuse and trust boundary | 2.1; 14.1 |
| 36. Multi-hop/loops | 14.2 |
| 37. Endpoint selection/affinity | 14.3 |
| 38. Graceful shutdown | 15.1 |
| 39. Health/observability | 15.2-15.3 |
| 40. Adapter contract | 6.2 |
| 41. Package/module layout | 16 |
| 42. Sink preserve/move/rewrite/delete review | 17; Appendix A |
| 43. Complexity review | 5.5; 18; 19 |
| 44. Post-approval implementation plan | 20 |

The design remains explainable as Listener -> Route -> Service -> local
StoreRuntime/Scheduler/Adapter or remote Weir, with bounded execution and explicit
failure semantics. No additional runtime roles or external coordination service
are required.
