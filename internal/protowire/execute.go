package protowire

import (
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

// ValidateExecuteFrame rejects malformed frames and repeated routing/command
// envelopes before protobuf allocates the single request object.
func ValidateExecuteFrame(data mem.BufferSlice) error {
	reader := frameCursor{buffers: data, remaining: data.Len()}
	return validateMessageFrame(&reader, wireExecute, uint64(reader.remaining))
}

// Requests fail closed on unknown fields: silently dropping a future condition or
// command option could change its effects. Walking nested messages before decoding
// prevents last-value-wins from hiding duplicate fields without copying payloads.
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
	wireMutationOptions
)

type wireField struct {
	number   uint64
	scalar   bool
	repeated bool
	nested   wireKind
	oneof    bool
}

var requestShapes = [...][]wireField{
	wireExecute: {{number: 1},
		{number: 2, scalar: true}, {number: 3, nested: wireCommand}},
	wireCommand: {{number: 1, nested: wireRead, oneof: true},
		{number: 2, nested: wireMutate, oneof: true},
		{number: 3, nested: wireScan, oneof: true},
		{number: 4, nested: wireNative, oneof: true}},
	wireRead: {{number: 1}},
	wireMutate: {{number: 1},
		{number: 2, nested: wireDocument, oneof: true},
		{number: 3, nested: wireDocument, oneof: true},
		{number: 4, nested: wireDocument, oneof: true},
		{number: 5, nested: wireEmpty, oneof: true},
		{number: 6, nested: wireTransform, oneof: true},
		{number: 7, nested: wireMutationOptions}},
	wireScan: {{number: 1},
		{number: 2, nested: wireDocument},
		{number: 3, nested: wireProjection},
		{number: 4, scalar: true},
		{number: 5}},
	wireNative: {{number: 1},
		{number: 2, nested: wireDocument}},
	wireDocument: {{number: 1},
		{number: 2}},
	wireEmpty: {},
	wireTransform: {{number: 1, nested: wireLua, oneof: true},
		{number: 2, nested: wireDocument, oneof: true}},
	wireLua: {{number: 1},
		{number: 2, nested: wireDocument}},
	wireProjection: {{number: 1, scalar: true},
		{number: 2, repeated: true}},
	wireMutationOptions: {{number: 1, scalar: true}},
}

// Every recursion borrows the same cursor and owns an exact byte span.
// Native payload bytes are opaque here; only their Document envelope is parsed.
func validateMessageFrame(reader *frameCursor, kind wireKind, remaining uint64) error {
	seen := uint64(0)
	oneof := false
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

		if shape.scalar {
			if tag&7 != 0 {
				return invalidExecuteFraming()
			}
			value, consumed, err := frameVarint(reader, remaining)
			if err != nil {
				return err
			}
			if kind == wireMutationOptions && number == 1 && value != uint64(pb.MutationVisibility_APPLICATION_ACKNOWLEDGED) && value != uint64(pb.MutationVisibility_QUERY_VISIBLE) {
				return invalidExecuteFraming()
			}
			if kind == wireProjection && number == 1 && value != uint64(pb.ProjectionMode_PROJECTION_MODE_UNSPECIFIED) && value != uint64(pb.ProjectionMode_INCLUDE) && value != uint64(pb.ProjectionMode_EXCLUDE) {
				return invalidExecuteFraming()
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
		if length > remaining {
			return invalidExecuteFraming()
		}
		if shape.nested != 0 {
			if err := validateMessageFrame(reader, shape.nested, length); err != nil {
				return err
			}
		} else if !reader.discard(int(length)) {
			return invalidExecuteFraming()
		}
		remaining -= length
	}
	return nil
}

func frameVarint(reader *frameCursor, remaining uint64) (uint64, uint64, error) {
	var value uint64
	for offset := uint64(0); offset < 10 && offset < remaining; offset++ {
		next, ok := reader.readByte()
		if !ok || offset == 9 && next > 1 {
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
