// Package protowire checks repeated-message envelopes before native protobuf
// decoding can amplify a small wire payload into many allocated objects.
package protowire

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateRepeatedMessages accepts length-delimited root fields up to the
// repeated field and applies its existing capacity without copying payloads.
func ValidateRepeatedMessages(data mem.BufferSlice, repeatedField uint64, maximumItems int) error {
	reader := frameCursor{buffers: data, remaining: data.Len()}
	items := 0
	for reader.remaining > 0 {
		tag, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || tag>>3 < 1 || tag>>3 > repeatedField || tag&7 != 2 {
			return status.Error(codes.InvalidArgument, "invalid protobuf request framing")
		}
		length, _, err := frameVarint(&reader, uint64(reader.remaining))
		if err != nil || length > uint64(data.Len()) {
			return status.Error(codes.InvalidArgument, "invalid protobuf field length")
		}
		if tag>>3 == repeatedField {
			items++
			if items > maximumItems {
				return status.Error(codes.ResourceExhausted, "request metadata exceeds decode budget")
			}
		}
		if !reader.discard(int(length)) {
			return status.Error(codes.InvalidArgument, "truncated protobuf field")
		}
	}
	return nil
}
