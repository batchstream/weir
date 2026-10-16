# Deterministic before and smallest repair proposal

Production remains clean21 (21d92ef4c9daf5dc97437b095ab3c6cdba6566ef). This reproduces the possible failure mechanism with a real Adapter, Runtime, shared Node ledger and HTTP transport. It does not establish the exact instant or branch in the original real-backend round0 failure. The one real diagnostic run did not reproduce that failure.

## Before evidence

- Four actual Native GET /hold exchanges reached the httptest server and were held before headers. Snapshot: Starting=4, native working=100663296, Resident=InputReserved=33554432, wire=0, rejections=0.
- B=135268088 is fixed from Q33554432 + four24MiB working + publication growth518140 + original typed known input/64KiB270076 + headroom262144. The test now derives known input independently from Plan.WorkingBytes, so a future added ACK reservation cannot silently raise B.
- The single bulk applies all42 unique IDs exactly once. Before replying Used=135071480, free196608; valid complete ACK is7166 bytes, jsonDecodeWorking1111034. Source preflight requires13312. Header and body allocation peaks fit the pre-paid64KiB response stage. The final full-JSON reserve cannot fit.
- Outcomes: 0 APPLIED,42 UNKNOWN+RESOURCE_EXHAUSTED; global BudgetRejections=1. Actual go test race exit1 is the expected desired-behavior assertion failure; no race report. Original raw and command remain preserved.

## Repair, pending approval

1. In Search PrepareRecord, retain the actual known-input estimate and add a normal-success ACK working contribution per mutation to Plan.WorkingBytes. Add the fixed bulk envelope contribution once through the existing IOReserve maximum. Runtime already sums the former and takes max(IOReserve), so a batch without its whole normal ACK workspace stays in its existing bounded queue while initial Native IO converges. No new waiting or ledger behavior.
2. Derive normal ACK bounds from an explicit complete standard template, not a guessed64KiB per record. Cover longest action/result, int64 digit widths, _shards integer fields, _version, forced_refresh, and optional _type. Physical index uses the existing255-byte validation maximum, not the possibly shorter alias. ID uses its actual known byte length, with six-times JSON escaping. Include outer errors/took/items and optional ingest_took once. Existing valid ES/OS success fixtures are a subset.
3. For wire bound W and structural-node bound N, reserve5*W+1024*N per item: current jsonDecodeWorking is3W+1024N+8192, and unknown/chunked bodies can retain capacity below2W (plus fixed minimum4096). Outer8192, body minimum4096 and outer nodes/bytes occur once; the existing64KiB header stage remains separate. The attached calculation with _type:_doc, maximum numeric fields, max255-byte physical index and6-byte IDs yields39051 per item and21944 once, total1662086 for42. Compatibility type-name allowance can conservatively include its expected supported wire width; no new response field/size rejection is introduced.
4. Keep recordMemory.base as actual known input/framing rather than counting the new forecast twice. Add one numeric planned floor equal to the initial whole-batch reservation. recordMemory.set reserves max(plannedFloor,max(actualBase,frame)+sources+scratch). The floor already promises the parser; adding scratch on top would double-charge it.
5. nativeHeaderMemory directly calls owner.SetWorking, bypassing recordMemory.set. Give only typed exchanges that same numeric minimum and use max(minimum,actual header bound); Native GrowIO remains unchanged. Body growth, preflight, metadata reads and request framing all retain the same floor.
6. Keep the promise through request JSON validation AND positional/per-item bulk validation in bulkResults/visibility processing. The existing Execute cleanup SetWorking(0), after adapter references are gone, releases it. Normal ACK parsing never has to compete again after send. Oversized or unrecognized extra response data still uses actual checked growth and, if its acknowledgement is lost, remains UNKNOWN without replay; do not impose a new public quota.

## Required after assertions

- Same fixed B and four held Native requests:42 typed tickets remain queued, backend bulk calls/effects=0, hard rejections=0. Release one Native and drain its output; existing memory wake must dispatch one bulk,42 APPLIED, independent per-ID effect count exactly1, no replay.
- Verify literal complete normal ES/OS ACK fixtures, escaped/max ID and max physical index, forced_refresh and optional _type. Verify promised bound dominates actual jsonDecodeWorking plus retained body capacity.
- Exercise known-length and chunked bodies; normal header parser/body initial/grow/settle must not shrink below planned floor. An independent second batch must not steal that promise between body EOF and full bulk validation.
- Preserve unknown/lost ACK semantics, malformed full-response no partial success, confirmed APPLIED small terminal, reads and legitimate large-document capacity behavior.

No production files, DB/container, budget thresholds, retries, pools, public API, commits or PRs were changed.
