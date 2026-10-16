package server

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/protowire"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/protoadapt"
)

// responseCodec counts encoded bytes queued by a live transport. Native Free or
// final transport retirement ends that account; RSS still includes physical
// storage awaiting Go collection after an orphaned transport reference.
type responseCodec struct {
	admission *Admission
}

func (*responseCodec) Name() string { return "proto" }

func (codec *responseCodec) Marshal(value any) (mem.BufferSlice, error) {
	message, ok := value.(protoadapt.MessageV1)
	var wire proto.Message
	if ok {
		wire = protoadapt.MessageV2Of(message)
	} else {
		wire, ok = value.(proto.Message)
	}
	if !ok || wire == nil {
		return nil, fmt.Errorf("response is not a protobuf message: %T", value)
	}
	size := proto.Size(wire)

	capacity := max(size, 1025)
	codec.admission.wireBytes.Add(int64(capacity))
	pool := &responseBufferOwner{admission: codec.admission, bytes: int64(capacity), message: value, done: make(chan struct{})}
	codec.admission.responses.Store(value, pool)
	data := make([]byte, 0, capacity)
	options := proto.MarshalOptions{}
	data, err := options.MarshalAppend(data, wire)
	if err != nil {
		pool.Put(&data)
		return nil, err
	}
	// A stream on a live connection can be orphaned without native Free. Keep
	// cleanup as the fallback when its encoded storage becomes unreachable.
	pool.cleanup = runtime.AddCleanup(&data, func(owner *responseBufferOwner) { owner.finish() }, pool)
	buffer := mem.NewBuffer(&data, pool)
	buffers := mem.BufferSlice{buffer}
	return buffers, nil
}

func (*responseCodec) Unmarshal(data mem.BufferSlice, value any) error {
	switch value.(type) {
	case *pb.ExecuteRequest:
		if err := protowire.ValidateExecuteFrame(data); err != nil {
			return err
		}

	case *peerpb.SyncDirectoryRequest:

		if err := protowire.ValidateRepeatedMessages(data, 1); err != nil {
			return err
		}
	}
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}

// responseConnection retains only account owners, never RPC contexts or encoded
// buffers. The owner lock precedes this lock; retirement drops this lock before
// ending individual accounts.
type responseConnection struct {
	mu     sync.Mutex
	closed bool
	owners map[*responseBufferOwner]struct{}
}

func (connection *responseConnection) retire() {
	connection.mu.Lock()
	connection.closed = true
	owners := connection.owners
	connection.owners = nil
	connection.mu.Unlock()
	for owner := range owners {
		owner.finish()
	}
}

// Native Put reports the last gRPC buffer reference. A connection retirement
// ends only queued-byte accounting, without freeing or reusing encoded data.
type responseBufferOwner struct {
	admission  *Admission
	bytes      int64
	released   bool
	mu         sync.Mutex
	message    any
	timer      *time.Timer
	cleanup    runtime.Cleanup
	done       chan struct{}
	connection *responseConnection
}

func (*responseBufferOwner) Get(length int) *[]byte {
	data := make([]byte, length)
	return &data
}

func (owner *responseBufferOwner) Put(*[]byte) {
	owner.finish()
}

func (owner *responseBufferOwner) finish() {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.released {
		return
	}
	owner.released = true
	owner.cleanup.Stop()
	if owner.timer != nil {
		owner.timer.Stop()
		owner.timer = nil
	}
	if owner.message != nil {
		owner.admission.responses.CompareAndDelete(owner.message, owner)
	}
	if connection := owner.connection; connection != nil {
		connection.mu.Lock()
		delete(connection.owners, owner)
		connection.mu.Unlock()
		owner.connection = nil
	}
	owner.admission.wireBytes.Add(-owner.bytes)
	owner.message = nil
	if owner.done != nil {
		close(owner.done)
	}
}

func (owner *responseBufferOwner) attachConnection(connection *responseConnection) {
	owner.mu.Lock()
	if owner.released || owner.connection != nil {
		owner.mu.Unlock()
		return
	}
	connection.mu.Lock()
	if connection.closed {
		connection.mu.Unlock()
		owner.mu.Unlock()
		// OutPayload can arrive after ConnEnd when a successful enqueue races
		// transport shutdown. The writer has already ended in this case.
		owner.finish()
		return
	}
	if connection.owners == nil {
		connection.owners = make(map[*responseBufferOwner]struct{})
	}
	connection.owners[owner] = struct{}{}
	owner.connection = connection
	connection.mu.Unlock()
	owner.mu.Unlock()
}

// The stats notification identifies the RPC after native write enqueue. The
// timer follows the buffer's final ownership, including flow-control waiting.
func (owner *responseBufferOwner) watch(ctx context.Context, server *Server) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.released || owner.message == nil || ctx.Err() != nil {
		return
	}
	owner.timer = time.AfterFunc(server.limits.Stall, func() {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if owner.released {
			return
		}
		owner.timer = nil
		server.metrics.watchdogs.WithLabelValues("output").Inc()
		server.abortPeer(ctx)
	})
}

// Cancellation detaches the original document and watchdog context. It does not
// end queued-byte accounting on a still-live connection.
func (owner *responseBufferOwner) detachMessage() {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	// The watchdog's RPC context can reach the orphaned transport buffer.
	// End that reference chain on cancellation; native Free, connection retirement
	// or unreachable-storage cleanup still owns the queued-byte account.
	if owner.timer != nil {
		owner.timer.Stop()
		owner.timer = nil
	}
	if owner.message != nil {
		owner.admission.responses.CompareAndDelete(owner.message, owner)
		owner.message = nil
	}
}
