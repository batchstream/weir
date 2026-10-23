# Weir allocation ownership for the Lua 5.4 runtime

This directory contains the public production packages from
`github.com/iceisfun/golua v1.1.1`. `WEIR_UPSTREAM.json` records the unmodified
upstream file hashes, the selected module checksum, and the final patched/new
file hashes. Examples, command-line tools and upstream tests are omitted.

Weir runs Lua inside its main process. A single `budget.Budget` spans source
parsing, compilation, the VM, the allowed native functions and Weir's value
bridge. Its reservation callback updates the Node execution-memory ledger before
allocating.

The budget records a conservative cumulative allocation upper bound for one
evaluation. It does not measure the Go collector's live heap. Reusing an existing
table slot, register, slice capacity or builder capacity consumes no additional
byte credit. A new object, backing block, map entry or copied payload is prepaid.
Old backing blocks remain included in the cumulative charge. Weir owns the scope
until the VM and any result references covered by that credit have been released
or transferred to the backend's result ownership.

`Require` accepts an already calculated byte reservation. `RequireAllocation`
adds Go 1.27 size-class/page-rounding headroom to one payload;
`RequireArray` checks multiplication before applying the same bound. Controlled
`Append` and `Builder` helpers choose capacity before allocation. Hash insertions
include conservative Go map group, directory and growth-copy space; overwriting
an existing key does not charge again.

The runtime covers all string concatenation paths, table constructors and direct
SETLIST/rehash growth, closure/upvalue/constant-value caches, variable arguments,
return buffers, Lua registers/call frames, protected-call snapshots and the
allowed string/number/error helpers. All nine native/closure frame-push entries
prepay Go stack headroom before calling a callee, including native-only nested
`pcall`/`xpcall`. The initial entry and each additional maximum observed call
depth reserve a 32 KiB payload plus allocator headroom. Repeating calls within
that peak does not consume additional stack credit. This includes Weir's
`table.sort`, which uses Go's `sort.Sort`: nested comparator calls add both a
native frame and a Lua frame. Four-target VM and Go sort frame measurements set
the bound.

Capacity refusal, integer overflow and caller cancellation are sticky. Their
typed panic bypasses every VM protected-call/error-handler recovery layer; a Lua
`pcall` or `xpcall` cannot turn a failed allocation into a successful action.
The outer Go boundary converts only this typed termination into an error,
preserving `errors.Is` on the original reservation or cancellation cause.
Closing a terminated budgeted VM clears references without running Lua handlers
or allocating diagnostic snapshots.

This patch protects the Lua environment that Weir exposes. Weir does not expose
`load`, `string.rep`, patterns, arbitrary metatable mutation, coroutines, package
loading, file/network/process APIs or other native modules. The upstream
unbudgeted constructors remain available for Weir's finite startup library
template; evaluation code uses the VM's owned constructors.
