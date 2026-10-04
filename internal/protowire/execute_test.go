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

func TestExecuteDecodeNestedRequestContracts(t *testing.T) {
	resource := wireMessage(1, []byte("records"))
	condition := append(wireMessage(1, []byte("application/json")), wireMessage(2, []byte(`{"match_all":{}}`))...)
	projection := append([]byte{8, 1}, wireMessage(2, []byte("name"))...)
	scan := append(append(append([]byte(nil), resource...), wireMessage(2, condition)...), wireMessage(3, projection)...)
	http := append(wireMessage(1, []byte("GET")), wireMessage(2, []byte("/_doc/id"))...)
	native := append(append([]byte(nil), resource...), wireMessage(3, http)...)
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
		{name: "typed native HTTP", kind: 4, payload: native, valid: true},
		{name: "typed Lua", kind: 2, payload: mutation, valid: true},
		{name: "duplicate scan filter", kind: 3, payload: append(append([]byte(nil), scan...), wireMessage(2, condition)...)},
		{name: "duplicate projection mode", kind: 3, payload: append(append([]byte(nil), resource...), wireMessage(3, append(projection, 8, 2))...)},
		{name: "duplicate native variant", kind: 4, payload: append(append([]byte(nil), native...), wireMessage(2, []byte{5, 0, 0, 0, 0})...)},
		{name: "duplicate HTTP path", kind: 4, payload: append(append([]byte(nil), resource...), wireMessage(3, append(http, wireMessage(2, []byte("/_bulk"))...))...)},
		{name: "duplicate Lua input", kind: 2, payload: append(append([]byte(nil), resource...), wireMessage(6, wireMessage(1, append(lua, input...)))...)},
		{name: "duplicate transform form", kind: 2, payload: append(append([]byte(nil), resource...), wireMessage(6, append(transform, wireMessage(2, condition)...))...)},
		{name: "duplicate document content type", kind: 3, payload: append(append([]byte(nil), resource...), wireMessage(2, append(condition, wireMessage(1, []byte("application/bson"))...))...)},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			raw := wireMessage(3, wireMessage(item.kind, item.payload))
			var data mem.BufferSlice
			for i := range raw {
				data = append(data, mem.SliceBuffer(raw[i:i+1]))
			}
			defer data.Free()
			err := ValidateExecuteFrame(data)
			if (err == nil) != item.valid {
				t.Fatal("nested protobuf collapse accepted", err)
			}
		})
	}
}

func TestExecuteDecodeNestedMetadataUsesBoundedAllocation(t *testing.T) {
	header := append(wireMessage(1, []byte("x")), wireMessage(2, nil)...)
	base := append(wireMessage(1, []byte("GET")), wireMessage(2, []byte("/_doc/id"))...)
	raw := append([]byte(nil), base...)
	for range 8192 {
		raw = append(raw, wireMessage(4, header)...)
	}
	native := append(wireMessage(1, []byte("records")), wireMessage(3, raw)...)
	frame := wireMessage(3, wireMessage(4, native))
	data := mem.BufferSlice{mem.SliceBuffer(frame)}
	defer data.Free()
	allocations := testing.AllocsPerRun(3, func() {
		if err := ValidateExecuteFrame(data); err != nil {
			t.Fatal(err)
		}
	})
	if allocations > 32 {
		t.Fatal("nested metadata allocated per-message buffers", allocations)
	}
	for range 1200 {
		raw = append(raw, wireMessage(4, header)...)
	}
	native = append(wireMessage(1, []byte("records")), wireMessage(3, raw)...)
	frame = wireMessage(3, wireMessage(4, native))
	oversized := mem.BufferSlice{mem.SliceBuffer(frame)}
	defer oversized.Free()
	if status.Code(ValidateExecuteFrame(oversized)) != codes.InvalidArgument {
		t.Fatal("HTTP metadata bound disappeared")
	}
}
