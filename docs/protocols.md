# Protocol

The public contract is [store.proto](https://github.com/batchstream/weir-protocol/blob/v0.15.0/api/weir/v1/store.proto),
package weir.v1. Clients connect to a Weir application address and name the
Store in each request. A server that does not host the named Store fails the RPC
with NotFound.

| RPC | Meaning |
| --- | --- |
| StoreService.Execute | Exchange typed requests and indexed results on a finite bidirectional stream |

Execute accepts typed records, open Native forwarding and wait-until-visible
mutations on one finite stream.

ExecuteRequest contains store_name, a consecutive one-based index and Command.
Commands select one ReadRequest, MutateRequest or NativeRequest; a
stream fixes one Store and one kind. Each record is validated before its effects.
The input half-close ends the logical request sequence, which has no total item
or byte limit. Document.content_type identifies the actual backend representation;
Read does not negotiate alternative output encodings. NativeRequest carries
a resource and an opaque request Document; NativeHead carries optional metadata
and a body content type. Shared validation checks envelopes while each adapter
interprets its content type and bytes.

ExecuteResponse contains index and Event. Each record result is emitted as soon
as its record completes, so results arrive in completion order and clients match
them by index. A record the server cannot admit, such as one its adapter rejects
or one sent while the Store drains, receives its own NOT_STARTED result. A
request that fails validation, changes the Store or kind, breaks the index
sequence or cancels a record not yet sent ends the stream with INVALID_ARGUMENT,
after the results of records already admitted. `timeout_micros` bounds one
record from when the server reads it: a record still queued when it expires
fails with DEADLINE_EXCEEDED, NOT_STARTED for a mutation. A request carrying
only `store_name` and `cancel_index` cancels that earlier record alone; the
record still receives its own result. One goroutine reads requests while another
queues records in stream order, so a full Store queue never delays a cancel
request, and a record still waiting to be queued reports at once. Each stream
has room for the protocol's 32 records without results; the server reads one
record beyond that and then waits for a result to free room. Native emits typed
events at index 1 and requires a valid terminal Event plus final gRPC OK.
Receiving and publication run concurrently. The Store scheduler aggregates
compatible records within and across clients. Clients send and receive
concurrently to permit backpressure in both directions.

Request envelopes reject unknown fields, enum values and operation variants,
because ignoring an unrecognized write condition or command option could change
its effects.
Shared validation checks every decoded message, including unknown fields.
Response readers reject unknown fields and undeclared failure codes, and check
result evidence, event kinds and completion rules against the current schema.

Read and Mutate are the core batched operations. Querying, counting and traversal
use Native with the backend's own request and response formats. Weir does not
interpret their pagination, result counts or cursor lifetimes.

Mutate is not an atomic transaction. Same-resource mutations execute in stream
input order; an invalid later request does not undo earlier writes. Confirmed item
results remain evidence after a transport error; missing acknowledgements remain
unknown and are never replayed automatically. MongoDB Lua record groups can share
an internal short transaction with group rollback on database write error; Search
uses per-item version conditions. Neither path adds document metadata.

The application listener exposes only StoreService.

[weir-protocol](https://github.com/batchstream/weir-protocol) owns canonical public
schemas, generated Go bindings and shared validation. Both Weir and
[weir-go](https://github.com/batchstream/weir-go) depend on it; it depends on neither.
The SDK has no dependency on the server, and the server binary does not use the
SDK; only server tests consume it. Clients in other languages can generate the
public schema and implement load balancing and typed completion handling. Weir
uses protocol v0.15.0 and Go SDK v0.17.0.
