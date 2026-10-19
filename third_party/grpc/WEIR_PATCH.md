# Weir receive memory patch

This is the public Go source of `google.golang.org/grpc v1.83.2`, at the exact
upstream module hash recorded in `WEIR_UPSTREAM.json`. Upstream tests, testdata,
certificates, private keys and example assets are excluded. The upstream license
is preserved. `go.mod` uses one local replacement; there is no version upgrade or
module-cache mutation. `WEIR_UPSTREAM.json` records every original file hash and
the explicit changed/new source hashes. `WEIR_PATCH.diff` is the upstream diff.

The native HTTP/2 transport and protobuf protocol remain the only receive path.
`grpc.ReceiveMemory` binds allocation ownership to the server's existing Node
execution ledger:

* A message reserves its actual 5-byte-header length before `Read(length)` can
  expand its stream receive window. Only that RPC reader may wait for memory.
* Each DATA buffer, including small nonpooled buffers, charges backing capacity
  and bounded owner/queue metadata before entering the receive queue. Its final
  native `Free`, including all split/slice references, returns that charge.
* Both unary and streaming decode paths retain the decoded source until the
  application transfers it to execution or the handler discards it. Receive
  queue/backlog/reader-tail storage is drained on cancellation and completion.
  RPC fixed metadata retires only after handler completion and native stream
  deletion have both occurred.
* Native BDP can grow the connection receive window. Unadmitted streams keep
  their initial 64KiB window; only an individually admitted message expands its
  stream window. This avoids restoring the original shared 64KiB connection
  bottleneck or granting every partial upload a large unread receive window.
* A connection owns a conservative 512KiB parser/workspace promise, explained in
  `store.IngressConnectionBytes`, under a finite 64KiB header-list limit. This is
  a reservation, not measured RSS or unlimited idle-connection capacity. Native
  reader/writer termination detaches framer/loopy workspace before credit ends;
  failed constructors and transports rejected before serving also retire it.
* Stream cancellation may enqueue cleanup before a concurrent response DATA
  write. If the loopy writer has already removed that stream, it frees the
  discarded frame's queued buffer reference rather than orphaning it until GC
  or connection retirement. Caller references and live sibling streams retain
  their normal ownership; no connection-wide abort or early credit return is
  needed. Focused tests deterministically interleave native `write` and
  `closeStream`, check both caller-release orders and rejected enqueues, and
  verify the sibling's payload and final native release.
* Compressed requests fail before body/decompression allocation. Weir does not
  register a request decompressor; a future compression feature must provide an
  uncompressed allocation estimate rather than accounting compressed length.

No new HTTP/2 parser, framing implementation, worker process, configurable
memory pool or automatic mutation replay is introduced. Without the option,
upstream client/server receive and dynamic-window behavior are unchanged.

Focused offline race coverage lives in `receive_memory_test.go`,
`mem/receive_memory_test.go` and `internal/transport/receive_memory_test.go`.
