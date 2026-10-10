package protowire

import (
	"bytes"
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
	nestedVarint := append(wireMessage(3, []byte{0x80}), 0x10, 1)
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
		{name: "varint exceeds nested span", raw: nestedVarint, code: codes.InvalidArgument},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			data := mem.BufferSlice{mem.SliceBuffer(nil)}
			for i := range item.raw {
				data = append(data, mem.SliceBuffer(item.raw[i:i+1]))
				data = append(data, mem.SliceBuffer(nil))
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

func TestExecuteDecodeNestedRequestContracts(t *testing.T) {
	resource := wireMessage(1, []byte("records"))
	condition := append(wireMessage(1, []byte("application/json")), wireMessage(2, []byte(`{"match_all":{}}`))...)
	projection := append([]byte{8, 1}, wireMessage(2, []byte("name"))...)
	scan := append(append(append([]byte(nil), resource...), wireMessage(2, condition)...), wireMessage(3, projection)...)
	opaque := append(wireMessage(1, []byte("application/vnd.test.native")), wireMessage(2, []byte{0xff, 0x12, 0x80})...)
	native := append(append([]byte(nil), resource...), wireMessage(2, opaque)...)
	input := wireMessage(2, condition)
	lua := append(wireMessage(1, []byte("return nil")), input...)
	transform := wireMessage(1, lua)
	mutation := append(append([]byte(nil), resource...), wireMessage(6, transform)...)
	cases := []struct {
		name    string
		kind    byte
		payload []byte
		valid   bool
	}{
		{name: "typed scan", kind: 3, payload: scan, valid: true},
		{name: "opaque native document", kind: 4, payload: native, valid: true},
		{name: "typed Lua", kind: 2, payload: mutation, valid: true},
		{name: "duplicate scan filter", kind: 3, payload: append(append([]byte(nil), scan...), wireMessage(2, condition)...)},
		{name: "duplicate projection mode", kind: 3, payload: append(append([]byte(nil), resource...), wireMessage(3, append(projection, 8, 2))...)},
		{name: "duplicate native document", kind: 4, payload: append(append([]byte(nil), native...), wireMessage(2, opaque)...)},
		{name: "unknown native backend variant", kind: 4, payload: append(append([]byte(nil), native...), wireMessage(3, nil)...)},
		{name: "duplicate native content type", kind: 4, payload: append(append([]byte(nil), resource...), wireMessage(2, append(opaque, wireMessage(1, []byte("application/http"))...))...)},
		{name: "duplicate Lua input", kind: 2, payload: append(append([]byte(nil), resource...), wireMessage(6, wireMessage(1, append(lua, input...)))...)},
		{name: "duplicate transform form", kind: 2, payload: append(append([]byte(nil), resource...), wireMessage(6, append(transform, wireMessage(2, condition)...))...)},
		{name: "duplicate document content type", kind: 3, payload: append(append([]byte(nil), resource...), wireMessage(2, append(condition, wireMessage(1, []byte("application/bson"))...))...)},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			raw := wireMessage(3, wireMessage(item.kind, item.payload))
			data := mem.BufferSlice{mem.SliceBuffer(nil)}
			for i := range raw {
				data = append(data, mem.SliceBuffer(raw[i:i+1]))
				data = append(data, mem.SliceBuffer(nil))
			}
			defer data.Free()
			err := ValidateExecuteFrame(data)
			if (err == nil) != item.valid {
				t.Fatal("nested protobuf collapse accepted", err)
			}
		})
	}
}

func TestExecuteDecodeOpaqueNativeUsesBoundedAllocation(t *testing.T) {
	// Opaque bytes resembling malformed protobuf are never recursively decoded.
	payload := bytes.Repeat([]byte{0xff, 0x12, 0x80}, (protocol.MaxNativeRequestBytes-2)/3)
	document := append(wireMessage(1, []byte("application/vnd.future.store")), wireMessage(2, payload)...)
	native := append(wireMessage(1, []byte("records")), wireMessage(2, document)...)
	frame := wireMessage(3, wireMessage(4, native))
	data := mem.BufferSlice{mem.SliceBuffer(frame)}
	defer data.Free()
	allocations := testing.AllocsPerRun(3, func() {
		if err := ValidateExecuteFrame(data); err != nil {
			t.Fatal(err)
		}
	})
	if allocations > 32 {
		t.Fatal("opaque payload allocated per-message buffers", allocations)
	}
	oversizedDocument := append(wireMessage(1, []byte("application/vnd.future.store")), wireMessage(2, make([]byte, protocol.MaxNativeRequestBytes+256))...)
	native = append(wireMessage(1, []byte("records")), wireMessage(2, oversizedDocument)...)
	frame = wireMessage(3, wireMessage(4, native))
	oversized := mem.BufferSlice{mem.SliceBuffer(frame)}
	defer oversized.Free()
	if status.Code(ValidateExecuteFrame(oversized)) != codes.InvalidArgument {
		t.Fatal("Native document bound disappeared")
	}
}
