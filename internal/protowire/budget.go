// Package protowire checks repeated-message envelopes before native protobuf
// decoding can amplify a small wire payload into many allocated objects.
package protowire

import (
	"bufio"
	"encoding/binary"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateRepeatedMessages accepts length-delimited root fields up to the
// repeated field and applies its existing capacity without copying payloads.
func ValidateRepeatedMessages(data mem.BufferSlice, repeatedField uint64, maximumItems int) error {
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
