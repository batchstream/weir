package protowire

import (
	"bufio"

	"github.com/batchstream/weir-protocol/api/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateExecuteFrame rejects oversized frames and repeated routing/command
// envelopes before protobuf allocates the single request object.
func ValidateExecuteFrame(data mem.BufferSlice) error {
	if data.Len() > protocol.MaxExecuteRequestBytes {
		return status.Error(codes.ResourceExhausted, "execution frame exceeds input byte budget")
	}
	source := data.Reader()
	defer source.Close()
	reader := bufio.NewReader(source)
	return validateMessageFrame(reader, wireExecute, uint64(data.Len()))
}

// Message shapes are fixed by the public protocol. Walking nested messages before
// decoding prevents protobuf's last-value-wins merge from hiding duplicate fields.
type wireKind uint8

const (
	wireExecute wireKind = iota + 1
	wireCommand
	wireRead
	wireMutate
	wireScan
	wireNative
	wireDocument
	wireEmpty
	wireTransform
	wireLua
	wireProjection
)

type wireField struct {
	number   uint64
	scalar   bool
	repeated bool
	nested   wireKind
	maximum  int
	oneof    bool
}

var requestShapes = [...][]wireField{
	wireExecute: {{number: 1, maximum: protocol.MaxExecuteRequestBytes},
		{number: 2, scalar: true}, {number: 3, nested: wireCommand, maximum: protocol.MaxCommandBytes}},
	wireCommand: {{number: 1, nested: wireRead, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 2, nested: wireMutate, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 3, nested: wireScan, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 4, nested: wireNative, maximum: protocol.MaxCommandBytes, oneof: true}},
	wireRead: {{number: 1, maximum: protocol.MaxResourceBytes}},
	wireMutate: {{number: 1, maximum: protocol.MaxResourceBytes},
		{number: 2, nested: wireDocument, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 3, nested: wireDocument, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 4, nested: wireDocument, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 5, nested: wireEmpty, oneof: true},
		{number: 6, nested: wireTransform, maximum: protocol.MaxCommandBytes, oneof: true}},
	wireScan: {{number: 1, maximum: protocol.MaxResourceBytes},
		{number: 2, nested: wireDocument, maximum: protocol.MaxScanFilterBytes + 256},
		{number: 3, nested: wireProjection, maximum: 8 << 10},
		{number: 4, scalar: true},
		{number: 5, maximum: protocol.MaxScanToken}},
	wireNative: {{number: 1, maximum: protocol.MaxResourceBytes},
		{number: 2, nested: wireDocument, maximum: protocol.MaxNativeRequestBytes + 256}},
	wireDocument: {{number: 1, maximum: 128},
		{number: 2, maximum: protocol.MaxCommandBytes}},
	wireEmpty: {},
	wireTransform: {{number: 1, nested: wireLua, maximum: protocol.MaxCommandBytes, oneof: true},
		{number: 2, nested: wireDocument, maximum: protocol.MaxCommandBytes, oneof: true}},
	wireLua: {{number: 1, maximum: protocol.MaxCommandBytes},
		{number: 2, nested: wireDocument, maximum: protocol.MaxCommandBytes}},
	wireProjection: {{number: 1, scalar: true},
		{number: 2, repeated: true, maximum: 512}},
}

// Every recursion borrows the same buffered reader and owns an exact byte span.
// Native payload bytes are opaque here; only their Document envelope is parsed.
func validateMessageFrame(reader *bufio.Reader, kind wireKind, remaining uint64) error {
	seen := uint64(0)
	oneof := false
	repeated := 0
	for remaining > 0 {
		tag, tagBytes, err := frameVarint(reader, remaining)
		if err != nil {
			return err
		}
		remaining -= tagBytes
		number := tag >> 3
		var shape *wireField
		for i := range requestShapes[kind] {
			candidate := &requestShapes[kind][i]
			if candidate.number == number {
				shape = candidate
				break
			}
		}
		if shape == nil || !shape.repeated && seen&(1<<number) != 0 || shape.oneof && oneof {
			return invalidExecuteFraming()
		}
		seen |= 1 << number
		oneof = oneof || shape.oneof
		if shape.repeated {
			repeated++
			if kind == wireProjection && repeated > 128 {
				return invalidExecuteFraming()
			}
		}
		if shape.scalar {
			if tag&7 != 0 {
				return invalidExecuteFraming()
			}
			_, consumed, err := frameVarint(reader, remaining)
			if err != nil {
				return err
			}
			remaining -= consumed
			continue
		}
		if tag&7 != 2 {
			return invalidExecuteFraming()
		}
		length, lengthBytes, err := frameVarint(reader, remaining)
		if err != nil {
			return err
		}
		remaining -= lengthBytes
		if length > uint64(shape.maximum) || length > remaining {
			return invalidExecuteFraming()
		}
		if shape.nested != 0 {
			if err := validateMessageFrame(reader, shape.nested, length); err != nil {
				return err
			}
		} else if _, err := reader.Discard(int(length)); err != nil {
			return invalidExecuteFraming()
		}
		remaining -= length
	}
	return nil
}

func frameVarint(reader *bufio.Reader, remaining uint64) (uint64, uint64, error) {
	var value uint64
	for offset := uint64(0); offset < 10 && offset < remaining; offset++ {
		next, err := reader.ReadByte()
		if err != nil || offset == 9 && next > 1 {
			return 0, 0, invalidExecuteFraming()
		}
		value |= uint64(next&0x7f) << (7 * offset)
		if next < 128 {
			return value, offset + 1, nil
		}
	}
	return 0, 0, invalidExecuteFraming()
}

func invalidExecuteFraming() error {
	return status.Error(codes.InvalidArgument, "invalid execution protobuf framing")
}
