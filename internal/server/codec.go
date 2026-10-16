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

// responseCodec keeps encoded output charged until the native transport drops
// its final reference. A handler completion is earlier than that ownership end.
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
	// A native stream can be orphaned on connection abort without calling Free.
	// Its actual encoded storage must become unreachable before returning credit.
	pool.cleanup = runtime.AddCleanup(&data, func(owner *responseBufferOwner) { owner.Put(nil) }, pool)
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

// gRPC's mem.BufferPool is the native buffer ownership seam. Marshal allocates
// once after reserving bytes; Put is called after every transport reference ends.
type responseBufferOwner struct {
	admission *Admission
	bytes     int64
	released  bool
	mu        sync.Mutex
	message   any
	timer     *time.Timer
	cleanup   runtime.Cleanup
	done      chan struct{}
}

func (*responseBufferOwner) Get(length int) *[]byte {
	data := make([]byte, length)
	return &data
}

func (owner *responseBufferOwner) Put(*[]byte) {
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
	owner.admission.wireBytes.Add(-owner.bytes)
	owner.message = nil
	if owner.done != nil {
		close(owner.done)
	}
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

// On cancellation the completed Send no longer needs the original document.
// Its independent encoded copy remains charged until the transport frees it.
func (owner *responseBufferOwner) detachMessage() {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	// The watchdog's RPC context can reach the orphaned transport buffer.
	// End that reference chain on cancellation; storage cleanup still owns credit.
	if owner.timer != nil {
		owner.timer.Stop()
		owner.timer = nil
	}
	if owner.message != nil {
		owner.admission.responses.CompareAndDelete(owner.message, owner)
		owner.message = nil
	}
}
