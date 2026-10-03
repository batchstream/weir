package server

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
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
	if size > protocol.MaxBatchResponseBytes {
		return nil, status.Error(codes.ResourceExhausted, "response exceeds protobuf byte bound")
	}
	capacity := max(size, 1025)
	if !codec.admission.reserveWire(capacity) {
		return nil, status.Error(codes.ResourceExhausted, "queued response byte budget exhausted")
	}
	pool := &responseBufferOwner{admission: codec.admission, bytes: int64(capacity), message: value}
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
	case *pb.ReadBatchRequest, *pb.MutateBatchRequest:
		if err := validateMessageDecodeBudget(data, 2, protocol.MaxBatchResponseBytes/protocol.ResultOverhead); err != nil {
			return err
		}

	case *peerpb.SyncDirectoryRequest:
		if data.Len() > directory.MaxSyncBytes {
			return status.Error(codes.ResourceExhausted, "directory sync exceeds input byte budget")
		}
		if err := validateMessageDecodeBudget(data, 1, directory.MaxAnnouncements); err != nil {
			return err
		}
	case *pb.ResolveStoreRequest:
		if data.Len() > directory.MaxSyncBytes {
			return status.Error(codes.ResourceExhausted, "discovery request exceeds input byte budget")
		}
	}
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}

// Apply existing result-envelope or directory-announcement capacity before
// protobuf construction, bounding amplification by tiny repeated messages.
func validateMessageDecodeBudget(data mem.BufferSlice, repeatedField uint64, maximumItems int) error {
	source := data.Reader()
	defer source.Close()
	reader := bufio.NewReader(source)
	items := 0
	for {
		tag, err := binary.ReadUvarint(reader)
		if err == io.EOF {
			return nil
		}
		if err != nil || tag>>3 < 1 || tag>>3 > repeatedField || tag&7 != 2 {
			return status.Error(codes.InvalidArgument, "invalid protobuf request framing")
		}
		length, err := binary.ReadUvarint(reader)
		if err != nil || length > uint64(data.Len()) {
			return status.Error(codes.InvalidArgument, "invalid protobuf field length")
		}
		if tag>>3 == repeatedField {
			items++
			if items > maximumItems {
				return status.Error(codes.ResourceExhausted, "request metadata exceeds decode budget")
			}
		}
		if _, err := reader.Discard(int(length)); err != nil {
			return status.Error(codes.InvalidArgument, "truncated protobuf field")
		}
	}
}

// gRPC's mem.BufferPool is the native buffer ownership seam. Marshal allocates
// once after reserving bytes; Put is called after every transport reference ends.
type responseBufferOwner struct {
	admission *Admission
	bytes     int64
	released  atomic.Bool
	mu        sync.Mutex
	message   any
	timer     *time.Timer
	cleanup   runtime.Cleanup
}

func (*responseBufferOwner) Get(length int) *[]byte {
	data := make([]byte, length)
	return &data
}

func (owner *responseBufferOwner) Put(*[]byte) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.released.CompareAndSwap(false, true) {
		owner.cleanup.Stop()
		if owner.timer != nil {
			owner.timer.Stop()
			owner.timer = nil
		}
		if owner.message != nil {
			owner.admission.responses.CompareAndDelete(owner.message, owner)
		}
		owner.admission.wireBytes.Add(-owner.bytes)
	}
}

// The stats notification identifies the RPC after native write enqueue. The
// timer follows the buffer's final ownership, including flow-control waiting.
func (owner *responseBufferOwner) watch(ctx context.Context, server *Server) {
	owner.mu.Lock()
	defer owner.mu.Unlock()
	if owner.released.Load() {
		return
	}
	owner.message = nil
	owner.timer = time.AfterFunc(server.limits.Stall, func() {
		owner.mu.Lock()
		defer owner.mu.Unlock()
		if owner.released.Load() {
			return
		}
		owner.timer = nil
		server.metrics.watchdogs.WithLabelValues("output").Inc()
		server.abortPeer(ctx)
	})
}

func (a *Admission) reserveWire(bytes int) bool {
	for {
		current := a.wireBytes.Load()
		if int64(bytes) > a.wireLimit-current {
			return false
		}
		if a.wireBytes.CompareAndSwap(current, current+int64(bytes)) {
			return true
		}
	}
}
