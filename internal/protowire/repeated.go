// Package protowire validates protobuf envelopes before native decoding.
package protowire

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateRepeatedMessages checks root field identities and length-delimited
// framing without copying payloads or imposing a message count quota.
func ValidateRepeatedMessages(data mem.BufferSlice, repeatedField uint64) error {
	reader := frameCursor{buffers: data, remaining: data.Len()}
	for reader.remaining > 0 {
		tag, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || tag>>3 < 1 || tag>>3 > repeatedField || tag&7 != 2 {
			return status.Error(codes.InvalidArgument, "invalid protobuf request framing")
		}
		length, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || length > uint64(data.Len()) {
			return status.Error(codes.InvalidArgument, "invalid protobuf field length")
		}
		if !reader.discard(int(length)) {
			return status.Error(codes.InvalidArgument, "truncated protobuf field")
		}
	}
	return nil
}
