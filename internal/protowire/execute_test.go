package protowire

import (
	"encoding/binary"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func wireMessage(field byte, data []byte) []byte {
	raw := []byte{field<<3 | 2}
	raw = binary.AppendUvarint(raw, uint64(len(data)))
	return append(raw, data...)
}

func TestExecuteDecodeEnvelopeAcrossBuffers(t *testing.T) {
	read := &pb.ReadRequest{Resource: "records/s:key"}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
	valid, err := proto.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	commandRaw, err := proto.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	duplicateCommand := append(append([]byte(nil), valid...), wireMessage(3, commandRaw)...)
	duplicateRouting := append(append([]byte(nil), valid...), wireMessage(1, []byte("other"))...)
	duplicateKind := append(append([]byte(nil), commandRaw...), wireMessage(2, nil)...)
	cases := []struct {
		name string
		raw  []byte
		code codes.Code
	}{
		{name: "valid", raw: valid, code: codes.OK},
		{name: "duplicate command", raw: duplicateCommand, code: codes.InvalidArgument},
		{name: "duplicate routing", raw: duplicateRouting, code: codes.InvalidArgument},
		{name: "duplicate kind", raw: wireMessage(3, duplicateKind), code: codes.InvalidArgument},
		{name: "unknown root", raw: []byte{0x22, 0}, code: codes.InvalidArgument},
		{name: "index wire type", raw: []byte{0x12, 0}, code: codes.InvalidArgument},
		{name: "truncated command", raw: []byte{0x1a, 3, 0x0a, 2}, code: codes.InvalidArgument},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			var data mem.BufferSlice
			for i := range item.raw {
				data = append(data, mem.SliceBuffer(item.raw[i:i+1]))
			}
			defer data.Free()
			err := ValidateExecuteFrame(data)
			if status.Code(err) != item.code {
				t.Fatal("unexpected decode framing result", err, item.code)
			}
		})
	}
}

func TestExecuteDecodeRejectsOversizedInputBeforeUnmarshal(t *testing.T) {
	raw := make([]byte, protocol.MaxExecuteRequestBytes+1)
	data := mem.BufferSlice{mem.SliceBuffer(raw)}
	err := ValidateExecuteFrame(data)
	data.Free()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatal("oversized source reached protobuf decoding", err)
	}
}
