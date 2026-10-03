package directory

import (
	"github.com/batchstream/weir/internal/protowire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// peerCodec applies the same announcement capacity to synchronization replies
// before native protobuf decoding. The peer client serves only SyncDirectory.
type peerCodec struct{}

func (peerCodec) Name() string { return "proto" }

func (peerCodec) Marshal(value any) (mem.BufferSlice, error) {
	return encoding.GetCodecV2("proto").Marshal(value)
}

func (peerCodec) Unmarshal(data mem.BufferSlice, value any) error {
	if data.Len() > MaxSyncBytes {
		return status.Error(codes.ResourceExhausted, "directory sync exceeds response byte budget")
	}
	if err := protowire.ValidateRepeatedMessages(data, 1, MaxAnnouncements); err != nil {
		return err
	}
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}
