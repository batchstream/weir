# Store payload contracts

Configured MongoDB, Elasticsearch and OpenSearch Stores accept typed Read/Mutate
batches and one Scan or Native Command per Execute RPC. Execute emits typed
Events. Unknown protobuf fields and missing oneof variants fail validation.
Adapter document and descriptor formats use the explicit media profiles below.
BSON and JSON retain their native semantics.

The outer `store_name` selects the adapter. Resource paths are canonical relative
paths: percent escaping must round-trip canonically, and full `weir://` resources
are invalid on the wire. The whole Read/Mutate batch is validated before any
backend effect. The public protobuf source defines field shapes.

| Capability | MongoDB | Elasticsearch / OpenSearch |
| --- | --- | --- |
| Record target | `db/collection/s:string`, `db/collection/i:canonical-int64`, or `db/collection/oid:lowercase-hex` | `index/s:string`, string ID 1–512 bytes |
| Read representation | omitted or `application/bson` | omitted or `application/json` |
| Put/create/replace document | raw BSON, explicit first `_id` matching the target | JSON object; target supplies the document ID |
| Adapter options | must be absent | must be absent |
| Scan target | `db/collection` | one concrete `index` |
| Scan selector | BSON document containing only document-valued `filter`, `sort`, `projection`; sort omitted or `{_id:1}` and projection preserves original `_id` | JSON object containing only object-valued `query` |
| Scan output | BSON records | native JSON hits, including hit metadata |
| Backend expression media | `application/vnd.weir.mongodb-update.v1+bson` | `application/vnd.weir.search-update.v1+json` |
| Expression structure | bounded `$set`, `$unset`, `$inc` operator documents with validated paths | exactly one `doc` object, with validated fields |
| Lua runtime | `lua.v1`, BSON input | `lua.v1`, JSON input |
| Native descriptor media | `application/vnd.weir.mongodb-command.v1+protobuf`, empty descriptor data | `application/vnd.weir.search-http.v1+protobuf`, encoded `weir.search.v1.Request` |
| Native target | `db/collection` | one concrete `index` |
| Native body | one complete BSON command, at most 4 MiB | at most 8 MiB; POST bulk uses NDJSON, GET has no body |

Records are at most 2 MiB. The Mongo codec and Search JSON validator also bound
nesting and node counts. Selectors and backend expressions are at most 16 KiB.
Lua source is at most 16 KiB; its input/current/result typed trees are at most
256 KiB with separate depth/node bounds. Lua remains an in-process trusted-program
facility with the transaction/CAS and allocation limitations in the architecture.

MongoDB Native supports `count` and `findAndModify`, with the command's collection
matching the target. Its bounded allowlist validates query/sort/fields/update,
booleans and count limit/skip/hint types. Caller session, lifecycle, unacknowledged
write and JavaScript options are rejected. Native replies are raw BSON, at most
4 MiB, emitted as ordered chunks with a NativeEnd completion result.

Search Native's descriptor is the explicit protobuf schema in
[weir-protocol's http.proto](https://github.com/batchstream/weir-protocol/blob/v0.4.0/api/weir/search/v1/http.proto). It supports POST `/_bulk` with
`application/x-ndjson` and GET `/_doc/<unreserved-id>` with an empty body. Canonical
query options are `refresh` for bulk or `realtime` for GET. Headers are restricted
to `accept`, `content-type`, `x-opaque-id` with bounded values; they cannot override
connection credentials or select another host. Each bulk item is validated before
sending, scoped to the resource index and bounded to 256 KiB. Native replies are
raw HTTP metadata/body, at most 8 MiB total, followed by NativeEnd. HTTP/backend
errors remain native data, rather than normalized mutation outcomes.

A record stream returns one indexed result per input record. A scan emits one finite page of zero or more
Document Events and exactly one ScanEnd containing that page's matching document
count and optional failure. `ScanRequest.page_size` defaults to 128 when zero and
cannot exceed 256. Successful ScanEnd has exactly one of a nonempty
`next_continuation_token` or `exhausted=true`; failure carries neither. The next
ScanRequest repeats the same resource, selector and representation, with the token
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
A native request emits metadata before chunks if it starts, then exactly one
NativeEnd; a rejected unstarted request can emit NativeEnd without metadata.
Business failure Events still require final gRPC completion. Overall streaming
success requires the expected terminal Event and final gRPC OK; record-stream success
requires a validated complete response. Transport termination never acknowledges
that an unfinished write did not occur.
