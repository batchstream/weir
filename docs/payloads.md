# Store payload contracts

Configured MongoDB, Elasticsearch and OpenSearch Stores accept typed Read/Mutate
streams and one Native Command per Execute RPC. Execute emits typed
Events. Unknown protobuf fields and missing oneof variants fail validation.
Document content types identify native bytes. Native carries an opaque request
Document interpreted by the owning adapter. BSON and JSON retain their native
semantics.

The outer `store_name` selects the adapter. Resource paths are canonical relative
paths: percent escaping must round-trip canonically, and full `weir://` resources
are invalid on the wire. Each Read/Mutate request is validated before its backend effect. The public protobuf source defines field shapes.

| Capability | MongoDB | Elasticsearch / OpenSearch |
| --- | --- | --- |
| Record target | `db/collection/s:string`, `db/collection/i:canonical-int64`, or `db/collection/oid:lowercase-hex` | `index/s:string`, string ID 1–512 bytes |
| Read output content type | `application/bson` | `application/json` |
| Put/create/replace document | raw BSON, explicit first `_id` matching the target | JSON object; target supplies the document ID |
| Backend expression content type | `application/vnd.weir.mongodb-update.v1+bson` | `application/vnd.weir.search-update.v1+json` |
| Expression structure | `$set`, `$unset`, `$inc` operator documents with validated paths | exactly one `doc` object, with validated fields |
| Lua input | optional BSON document | optional JSON document |
| Native request Document | `application/bson`: complete raw BSON command | `application/http`: complete HTTP/1.1 request, including headers and body |
| Native target | `db`, or empty with explicit `$db` | optional canonical path prefix; empty selects the backend root |
| Native request size | native MongoDB command boundary | native backend HTTP boundary |

Weir imposes no size or count quotas on documents, expressions, Lua source,
typed values, or Native requests, replies and response bodies. BSON follows
MongoDB native nesting rules and JSON follows its parser's syntax rules. Caller
cancellation, schema validity, the Node memory account and native backend limits
apply. Lua is an in-process trusted-program facility with optional VM execution
budgets; see [Lua transforms](lua.md).

Database names follow MongoDB's native nonempty, less-than-64-byte limit and
cross-platform forbidden characters. Collections may contain dots, numeric
prefixes and Unicode; system collections and `$` are unsupported, and the full
`database.collection` namespace is at most 255 bytes. Search indices are lowercase,
nonempty and at most 255 bytes, with the native forbidden characters and no
wildcard, index list or traversal syntax. Each concrete index is escaped as one
HTTP path segment. Typed Read and Mutate require existing collections/indices.
Search accepts single-index aliases without filters or routing overrides and
binds each operation to its resolved physical index. CRUD and Lua support
multiple primary shards. Native forwards administrative commands, including target creation,
using the configured backend identity.

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
`NOT_APPLIED/PRECONDITION_FAILED`.

Returning an unchanged ordinary document succeeds without a replacement write.
MongoDB compares the encoded BSON, including its value types and field order;
Search compares the losslessly encoded JSON. A missing document still requires
creation, even when the returned object is empty. Search query-visible completion
retains its visibility barrier when no replacement is needed.

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

Native forwards backend operations without command, method, path, query, header
or option allowlists. Backend permissions and native errors remain authoritative.
The Store selects the configured backend endpoint; requests cannot select another
server through HTTP Host or an absolute request target. Native never automatically
replays requests, including reads, because Weir does not classify opaque commands.

MongoDB accepts `application/bson` containing any valid ordered BSON command.
The resource contains one database segment or is empty. An explicit string `$db`
selects the database on the configured backend and is transmitted exactly once.
Without `$db`, the resource supplies the database. An empty resource requires
`$db`. Missing, duplicate or nonstring database routing metadata is invalid
framing. The command owns collection names, session/transaction parameters and
aggregation stages. Native uses the shared backend pool without adding implicit
sessions, inferred read preferences or deadline-derived `maxTimeMS`; cancellation
still follows the original caller lifetime.
Native does not require an existing collection or insert revision metadata.
Native command errors retain the backend BSON reply rather than becoming
normalized mutation results.

Search accepts `application/http` with an HTTP/1.1 origin-form request line,
headers and body. An optional canonical resource prefix is prepended to the native
request path; an empty resource addresses the backend root. Escaped paths, query
strings, arbitrary methods and end-to-end headers retain their native meaning.
Fixed Content-Length framing is required for nonempty request bodies; chunked
framing, upgrades and trailers are outside this finite request envelope. Response
status, headers, native error bodies and arbitrary entity bytes are preserved.

`MutateRequest.options` carries `MutationOptions` for ordinary mutations, backend
expressions and Lua transforms. An absent or empty options message defaults to
`visibility=APPLICATION_ACKNOWLEDGED`. With `visibility=QUERY_VISIBLE`,
successful completion additionally confirms visibility to subsequent normal
queries through the adapter's query path. Search uses `refresh=wait_for` for
physical writes and a refresh barrier for successful no-op results. A visibility
failure preserves any known APPLIED evidence and reports a Failure; it is not
permission to replay the write.
MongoDB's majority-acknowledged writes and primary reads satisfy the option
without a refresh operation. Compatible record batches keep
visibility waits separate from default application-only work.

A record stream returns one indexed result per input record. Queries, counts and
traversal use Native and retain the backend's own pagination, cursor and response
semantics.
A native request emits NativeHead before chunks if it starts, then exactly one
NativeEnd. The head may omit its metadata Document; a rejected unstarted request
can emit NativeEnd without a head.
Business failure Events still require final gRPC completion. Overall streaming
success requires the expected terminal Event and final gRPC OK; record-stream success
requires a validated complete response. Transport termination never acknowledges
that an unfinished write did not occur.
