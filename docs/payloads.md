# Store payload contracts

Configured MongoDB, Elasticsearch and OpenSearch Stores accept typed Read/Mutate
streams and one Scan or Native Command per Execute RPC. Execute emits typed
Events. Unknown protobuf fields and missing oneof variants fail validation.
Document content types identify native bytes. Scan projection is a public type; Native carries an opaque request Document interpreted by the owning adapter. New Stores do not require a new public request variant.
BSON and JSON retain their native semantics.

The outer `store_name` selects the adapter. Resource paths are canonical relative
paths: percent escaping must round-trip canonically, and full `weir://` resources
are invalid on the wire. Each Read/Mutate request is validated before its backend effect. The public protobuf source defines field shapes.

| Capability | MongoDB | Elasticsearch / OpenSearch |
| --- | --- | --- |
| Record target | `db/collection/s:string`, `db/collection/i:canonical-int64`, or `db/collection/oid:lowercase-hex` | `index/s:string`, string ID 1–512 bytes |
| Read output content type | `application/bson` | `application/json` |
| Put/create/replace document | raw BSON, explicit first `_id` matching the target | JSON object; target supplies the document ID |
| Scan target | `db/collection` | one concrete `index` |
| Scan filter | native BSON filter object without an outer wrapper | native JSON query object without an outer wrapper |
| Scan projection | typed INCLUDE or EXCLUDE field paths; complete `_id` supported, `_id.*` unsupported | typed INCLUDE or EXCLUDE field paths applied to `_source` |
| Scan output | native BSON records, including types outside the Lua value model | business JSON `_source`, identical to Read |
| Backend expression content type | `application/vnd.weir.mongodb-update.v1+bson` | `application/vnd.weir.search-update.v1+json` |
| Expression structure | bounded `$set`, `$unset`, `$inc` operator documents with validated paths | exactly one `doc` object, with validated fields |
| Lua input | optional BSON document | optional JSON document |
| Native request Document | `application/bson`: complete raw BSON command | `application/http`: complete HTTP/1.1 request, including headers and body |
| Native target | `db/collection` | one concrete `index` |
| Native request limit | one complete BSON command, at most 4 MiB | 8 MiB for the complete HTTP message; headers consume part of that budget |

Records are at most 2 MiB. Ordinary BSON follows MongoDB native nesting rules;
ordinary JSON follows its syntax rules without an additional Weir node quota. Scan filters and backend expressions are at most 16 KiB.
Lua source is at most 16 KiB; its input/current/result typed trees are at most
2 MiB by default, with process-wide `lua.values` byte/depth/node budgets. Lua remains an in-process trusted-program
facility with the transaction/CAS and allocation limitations in the architecture.

Projection has one mode and distinct dot-separated paths. INCLUDE publishes only
listed fields; EXCLUDE publishes all other fields. Paths cannot overlap, contain
wildcards or empty segments. MongoDB additionally rejects dollar-prefixed path
segments; Search can project a literal dollar-prefixed business field. A projection has at most 128 paths, each
at most 512 UTF-8 bytes and 8 KiB in total. MongoDB always reads the original `_id`
for keyset pagination but removes it before publication when it is not selected.
Search keeps PIT sort and hit identity internally. These pagination fields need
not be present in business results. Changing filter or projection invalidates a
continuation token.

Database names follow MongoDB's native nonempty, less-than-64-byte limit and
cross-platform forbidden characters. Collections may contain dots, numeric
prefixes and Unicode; system collections and `$` are unsupported, and the full
`database.collection` namespace is at most 255 bytes. Search indices are lowercase,
nonempty and at most 255 bytes, with the native forbidden characters and no
wildcard, index list or traversal syntax. Each concrete index is escaped as one
HTTP path segment. Collection/index structure must stay stable while the Store is
open; adapters do not create targets or modify backend settings.

A missing record is successful `ReadResult.missing`. A missing collection/index
is `TARGET_NOT_FOUND`; an unsupported view or structure is `UNSUPPORTED`.
Replace and backend expressions require an existing record and return
`NOT_APPLIED/PRECONDITION_FAILED` when it is absent. Deleting an absent record and
Lua keep/delete on a missing record satisfy their semantics with `APPLIED`, even
though no write is needed. `APPLIED` can include a Failure when application is
confirmed but replica acknowledgement fails. `UNKNOWN` retains ambiguity even
when a later error has a definite classification.

Lua source must return exactly one function, called as
`function(current, incoming)`. Missing current or omitted Input is Lua nil.
Objects and arrays are ordinary Lua tables: fields can be read, assigned and
deleted with nil; arrays support `#`, iteration, insertion, sorting and filtering.
The callback must return exactly one object to create/replace the record, or an
explicit `weir.keep()`, `weir.delete()` or `weir.reject(message)` action. Nil,
missing returns, multiple returns and scalar results are errors. Reject returns
`NOT_APPLIED/PRECONDITION_FAILED`. The previous global-value/helper API is removed.

`weir.object()` and `weir.array()` construct explicitly typed empty containers;
an unmarked empty `{}` is an object. Unmarked nonempty tables with dense integer
keys starting at 1 are arrays; tables with string keys are objects. Mixed keys,
sparse arrays, cycles, functions and non-document values cannot be written.
`weir.null()` represents an explicit BSON/JSON null without deleting a field.
Input tables retain their container types even when emptied.

Lua 5.4 integers preserve all 64 bits, while floats remain distinct. Existing
MongoDB int32 fields remain int32 when assigned an in-range integer and promote
to int64 outside that range; newly created integer fields are int64. Arithmetic
uses Lua integer overflow and float-promotion rules. Binary, BSON extended values
and exact JSON number spellings remain typed leaves when carried through the
script. See [Lua transforms](lua.md) for constructors, libraries and examples.

`weir.time.now()` returns a UTC RFC3339Nano string captured once per operation;
confirmed transaction/CAS retries reuse it. MongoDB restores the resource `_id`;
Search uses the resource ID. Concurrent first creation retries only a confirmed
transaction/CAS conflict from a fresh read. Uncertain writes never replay the
business callback. Programs run in the main Weir process.

MongoDB Native supports `count` and `findAndModify`, with the command's collection
matching the target. Its bounded allowlist validates query/sort/fields/update,
booleans and count limit/skip/hint types. Caller session, lifecycle, unacknowledged
write and JavaScript options are rejected. Native replies are raw BSON, at most
4 MiB, emitted as ordered chunks with a NativeEnd completion result.

Native uses a Store-neutral `request` Document, with at most 8 MiB of opaque
payload, and an optional response `metadata` Document, with at most 64 KiB.
The shared protocol checks content-type syntax and byte bounds. Each adapter
interprets its own format and rejects unsupported types or operations. Empty
payloads are allowed by the envelope but need not be valid for a particular Store.
MongoDB Native responses omit metadata and publish BSON body chunks.

Search Native accepts `application/http`: an HTTP/1.1 origin-form request-line,
headers terminated by CRLFCRLF, and its body. Headers are limited to 64 KiB before
HTTP parsing; the entire message is limited to 8 MiB, so the body must fit after
subtracting the headers. A nonempty body requires exactly one Content-Length
matching its byte length. An empty body may omit Content-Length. Chunked encoding,
compression, trailers, Expect, absolute request targets and trailing bytes are
rejected. Host is syntactic metadata and never selects the backend: Weir rebuilds
the request using the configured Store and resource index.

Supported operations are POST `/_bulk` with `application/x-ndjson` and GET
`/_doc/<unreserved-id>` with an empty body. Canonical query options are `refresh`
for bulk or `realtime` for GET. Option headers are restricted to `accept`,
`content-type` and `x-opaque-id`, with bounded values. Each bulk item is validated
before sending, scoped to the resource index and bounded to 256 KiB. Response
metadata is `application/http` containing only a status-line and headers followed
by CRLFCRLF. Raw entity body bytes follow as chunks, at most 8 MiB, then NativeEnd.
HTTP/backend errors remain native data rather than normalized mutation outcomes.
The SDK provides optional standard HTTP encoding/parsing helpers without adding
backend-specific fields to the public wire contract.

A record stream returns one indexed result per input record. A scan emits one finite page of zero or more
Document Events and exactly one ScanEnd containing that page's matching document
count and optional failure. `ScanRequest.page_size` defaults to 128 when zero and
cannot exceed 256. Successful ScanEnd has exactly one of a nonempty
`next_continuation_token` or `exhausted=true`; failure carries neither. The next
ScanRequest repeats the same resource, filter and projection, with the token
in `continuation_token`. Start it in a new Execute RPC to allow another Weir instance
to process the page. Tokens are opaque, bounded and validated before backend work;
they are not authentication credentials.

Only accept a page checkpoint after the matching document count, ScanEnd and final gRPC OK. On an interrupted page, reuse the previous checkpoint and handle
repeated documents in the consumer. MongoDB uses an ascending `_id` keyset and
has no cross-page snapshot. Search uses a backend PIT and fails if its 60-second
keep-alive expires; it never restarts an expired snapshot automatically. A PIT
already passed to the client remains alive until that timeout, including after
exhaustion or a failed page, so losing the final page response does not immediately
invalidate the client's previous checkpoint.
A native request emits NativeHead before chunks if it starts, then exactly one
NativeEnd. The head may omit its metadata Document; a rejected unstarted request
can emit NativeEnd without a head.
Business failure Events still require final gRPC completion. Overall streaming
success requires the expected terminal Event and final gRPC OK; record-stream success
requires a validated complete response. Transport termination never acknowledges
that an unfinished write did not occur.
