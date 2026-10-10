# Lua transforms

Each transform is a Lua 5.4 source chunk that returns one function. Weir calls
that function with the current document and the optional input document:

```lua
return function(current, incoming)
  current = current or weir.object()
  incoming = incoming or weir.object()
  if incoming.title ~= nil and incoming.title ~= "" then
    current.title = incoming.title
  end
  current.updated_at = weir.time.now()
  return current
end
```

Missing documents and omitted input are nil. Objects and arrays are real Lua
tables. Read and assign fields normally, delete a field by assigning nil, and
use `pairs`, `ipairs`, `#`, loops and table operations to implement business
rules. The callback returns exactly one object to create or replace the document,
or one explicit action:

| Result | Effect |
| --- | --- |
| Object table | Create or replace |
| `weir.keep()` | Keep, including a missing document |
| `weir.delete()` | Delete, including a missing document |
| `weir.reject(message)` | NOT_APPLIED/PRECONDITION_FAILED |

Nil, missing/multiple return values, arrays and scalar results are invalid.
Source must return the function itself. The current and input documents are
callback arguments; they are not globals.

This entry point, result actions, value conversion, helper meanings and restricted
environment are the `weir.v1` Lua contract. Runtime implementation updates must
preserve these script behaviors. New helpers may be additive; changes to existing
semantics require an explicit public contract change, rather than interpreting
the same source differently after an upgrade. There is one runtime contract and
no per-request language selector.

## Document values

| Document value | Lua representation |
| --- | --- |
| Missing | nil |
| Null | `weir.null()` |
| Boolean/string | boolean/string |
| int32/int64 | Native 64-bit Lua integer |
| float64 | Native Lua float |
| Object/array | Ordinary table retaining its original container type |
| Binary/extended scalar | Typed opaque leaf |

`weir.object()` and `weir.array()` construct empty containers. An unmarked empty
`{}` is an object. A nonempty unmarked table with only string keys is an object;
one with dense integer keys from 1 is an array. Mixed keys, sparse arrays and
cycles are rejected. Existing containers retain their kind even when emptied.
Assigning nil removes a field; assigning `weir.null()` keeps an explicit null.

Integers are exact through the full signed int64 range, including comparison and
`tostring`. Lua integer arithmetic wraps on overflow; operations mixing integers
and floats follow Lua's float-promotion rules. Do not convert an exact identifier
to a float. Existing int32 fields and array positions keep int32 when assigned an
in-range integer, and promote to int64 when needed. New integer fields are int64.
Floats remain floats, including integral float values and negative zero.

Ordinary JSON decimals and exponents become native floats when float64's shortest
decimal representation preserves their numeric value. An unchanged source table
field or array position retains its original spelling, including trailing zeros
and negative zero. Changed or copied numbers use the native number encoding.
Higher precision decimals and numbers outside the native range remain exact
typed leaves.

Binary and BSON extended leaves retain their native encoding when copied through
tables. `weir.kind(item)` identifies a document
value; `weir.data(item)` reads the payload of an opaque leaf.
`weir.bytes(data)` and `weir.extended(type, data)` construct explicit leaves;
the backend validates whether it supports their encoding. Search rejects binary
and BSON-only leaves. MongoDB preserves its supported BSON types and prevents
changes to the resource `_id`.

## Libraries and time

The environment provides iteration, type inspection, assertions, errors,
protected calls, deterministic math, and bounded string/table operations.
The base functions are `assert`, `error`, `type`, `tostring`, `tonumber`, `pairs`,
`ipairs`, `next`, `pcall`, `xpcall`, `select`, `rawequal` and `rawlen`; `_VERSION`
is `Lua 5.4`.
String functions are `len`, `sub`, `upper`, `lower`, `reverse`, `byte`, `char` and
`format`; table functions are `insert`, `remove`, `sort` and `concat`.
`string.upper/lower` use Lua's ASCII case rules and preserve other UTF-8 bytes.
Files, processes,
network access, dynamic loading, debug access, random state and
coroutines are unavailable. Library tables and globals are fresh for every
evaluation, including concurrent evaluations.

`weir.time.now()` returns a UTC RFC3339Nano string captured once per operation.
Multiple calls and confirmed database conflict retries reuse that timestamp.
It remains a string in both JSON and BSON; native BSON datetime values can be
carried through the typed extended-value bridge.

## Examples and execution bounds

[product.lua](../examples/lua/product.lua) replaces a nonempty title, forms a
stable union of tags and stamps the update time. Its rules are ordinary Lua code,
so a script can instead choose incoming values first, retain only selected
history entries, deduplicate objects by a business key, normalize strings, or
delete fields. Weir imposes no built-in merge policy.
The [complex product fixture](../internal/luaengine/testdata/product_merge.lua)
demonstrates history deduplication by nested values, tail limits and incoming-first
offer priority. It explicitly constructs empty arrays and recognizes typed leaves
while retaining the original business rules.

The source is at most 16 KiB. Process `lua.values` configures input/current/result
conversion, defaulting to 32 levels, 4096 nodes and the protocol 2 MiB document
bound. Process `lua.vm` exposes optional instruction, call-depth and stack-slot budgets.
The default instruction budget is unlimited; zero call/stack settings use the
VM native defaults. Compilation, top-level execution and the callback inherit
the caller context. Evaluations run concurrently inside the main Weir process without a fixed
concurrency cap or evaluation timeout.
Each evaluation creates its own VM, globals, library tables and string metatable.
The process loads the allowed standard-library functions and constants once;
evaluations copy those immutable values into fresh tables, then bind their own
conversion limits and observation time.
Programs must be trusted: these bounds do not impose a hard heap quota on
arbitrary temporary Lua objects. Deployment memory limits still apply.

Confirmed MongoDB transaction and Search compare-and-swap conflicts can trigger
a bounded fresh read and reevaluation. Ambiguous write or commit outcomes never
replay the business callback. A callback error or invalid document does not write.
