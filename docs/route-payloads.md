# Route payload contract

Every configured MongoDB, Elasticsearch or OpenSearch Store accepts protobuf
`weir.v1.Call` with `version=1` and emits length-delimited `weir.v1.Event` with
`version=1`. Unknown versions, unknown protobuf fields at any envelope depth and
missing oneof variants fail validation. There is no version negotiation or legacy
fallback. An incompatible adapter document/descriptor schema must use a new
explicit media profile; its version is part of the media type below. Plain BSON
and JSON retain their native format semantics within Call version 1.

The outer destination selects the adapter. Call resources are canonical relative
paths: percent escaping must round-trip canonically, and a full `weir://` resource
is invalid on the wire. A request cannot select a different Store inside its body.
The proto source is the field schema; validation takes place before scheduling or
before a validated native item reaches the database.

| Capability | MongoDB | Elasticsearch / OpenSearch |
| --- | --- | --- |
| Record target | `db/collection/s:string`, `db/collection/i:canonical-int64`, or `db/collection/oid:lowercase-hex` | `index/s:string`, string ID 1–512 bytes |
| Read representation | omitted or `application/bson` | omitted or `application/json` |
| Put/create/replace document | raw BSON, explicit first `_id` matching the target | JSON object; target supplies the document ID |
| Adapter options | must be absent | must be absent |
| Scan target | `db/collection` | one concrete `index` |
| Scan selector | BSON document containing only document-valued `filter`, `sort`, `projection` | JSON object containing only object-valued `query` |
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
`api/weir/search/v1/http.proto`. It supports POST `/_bulk` with
`application/x-ndjson` and GET `/_doc/<unreserved-id>` with an empty body. Canonical
query options are `refresh` for bulk or `realtime` for GET. Headers are restricted
to `accept`, `content-type`, `x-opaque-id` with bounded values; they cannot override
connection credentials or select another host. Each bulk item is validated before
sending, scoped to the resource index and bounded to 256 KiB. Native replies are
raw HTTP metadata/body, at most 8 MiB total, followed by NativeEnd. HTTP/backend
errors remain native data, rather than normalized mutation outcomes.

A record emits exactly one Result. A scan emits zero or more Document Events and
exactly one ScanEnd containing the matching document count and optional failure.
A native request emits metadata before chunks if it starts, then exactly one
NativeEnd; a rejected unstarted request can emit NativeEnd without metadata.
Business failure Events still require the outer empty end frame. Every ID has its
own ordered byte sequence; different IDs can interleave. Overall success requires
all outer ends, input half-close and final gRPC OK. No transport termination is an
acknowledgement that an unfinished write did not occur.
