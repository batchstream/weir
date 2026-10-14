package directory

import (
	"github.com/batchstream/weir/internal/protowire"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
)

// peerCodec validates the synchronization envelope before protobuf decoding.
type peerCodec struct{}

func (peerCodec) Name() string { return "proto" }

func (peerCodec) Marshal(value any) (mem.BufferSlice, error) {
	return encoding.GetCodecV2("proto").Marshal(value)
}

func (peerCodec) Unmarshal(data mem.BufferSlice, value any) error {
	if err := protowire.ValidateRepeatedMessages(data, 1); err != nil {
		return err
	}
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}
