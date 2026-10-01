# Unified Route redesign and acceptance

One finite bidirectional Route RPC carries strictly increasing positive IDs to one logical Store. The first valid envelope fixes the Service and downstream instance; each inbound RPC owns one outbound RPC. Half-close propagates only to downstream sending, while remaining responses continue upstream. Peer connections are reused; deadlines and the existing forwarding budget only decrease.

Call version 1 is a strict protobuf schema selected by Store configuration. Its target is relative to the Store. Record reads/mutations, scans and bounded native exchanges share one Store admission and scheduler. Backend transaction/CAS and per-item outcome verification remain adapter responsibilities. Lua evaluates in the main Weir process. Incompatible operations execute singly through the same ledger.

Response payload is a length-delimited stream of version 1 Event messages. Route frames carry at most 64 KiB; an empty terminal end marks request completion. Callers require all request terminals and final gRPC OK. Transport failure leaves uncompleted writes indeterminate; IDs are association only and no writes are replayed.

Bounds: complete input Call at most 9 MiB, native body at most 8 MiB, record at most 2 MiB, eight in-flight operations and 16 MiB input charges per RPC. Execution reserves bounded result and decoding workspace before dispatch. One input read-ahead message, protobuf/codec copies, HTTP2 buffers and connections are counted separately. Counts and bytes are released as requests finish; no historical ID set or whole-batch result table exists.

Acceptance: default offline tests, vet and race; valid/invalid envelopes and incomplete responses; parallel completion and required same-record order; safe cross-RPC batching and partial outcomes; two forwarding nodes, hop/deadline/half-close/cancellation/disconnects; stalled sends and queue limits; Lua atomicity; shutdown and resource recovery. Independent three-process slow-consumer tests must transfer 200 and 800 MiB with identical record size and active RPC count and record live objects, heap, RSS and bounded queues for each layer. New performance evidence must state backend, semantics, batching, concurrency, throughput, latency, CPU, memory and failures. Historical measurements are not new Route results.

The delivered PR requires a new Agent review after creation, confirmed issue fixes, and review of the final commit. No merge, release or deployment is authorized.
