package protowire

import (
	"bytes"
	"encoding/binary"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestExecutionFrameShapesMatchPublicSchema(t *testing.T) {
	cases := []struct {
		kind    wireKind
		message proto.Message
	}{
		{kind: wireExecute, message: &pb.ExecuteRequest{}},
		{kind: wireCommand, message: &pb.Command{}},
		{kind: wireRead, message: &pb.ReadRequest{}},
		{kind: wireMutate, message: &pb.MutateRequest{}},
		{kind: wireNative, message: &pb.NativeRequest{}},
		{kind: wireDocument, message: &pb.Document{}},
		{kind: wireEmpty, message: &pb.Empty{}},
		{kind: wireTransform, message: &pb.Transform{}},
		{kind: wireLua, message: &pb.LuaTransform{}},
		{kind: wireMutationOptions, message: &pb.MutationOptions{}},
	}
	for _, item := range cases {
		descriptor := item.message.ProtoReflect().Descriptor()
		fields := descriptor.Fields()
		shapes := requestShapes[item.kind]
		if fields.Len() != len(shapes) {
			t.Fatalf("%s framing field count differs from public schema", descriptor.FullName())
		}
		for _, shape := range shapes {
			field := fields.ByNumber(protoreflect.FieldNumber(shape.number))
			if field == nil {
				t.Fatalf("%s framing accepts undeclared field %d", descriptor.FullName(), shape.number)
			}
			scalar := field.Kind() == protoreflect.BoolKind || field.Kind() == protoreflect.EnumKind || field.Kind() == protoreflect.Uint32Kind || field.Kind() == protoreflect.Uint64Kind
			if shape.scalar != scalar || field.IsList() || (shape.nested != 0) != (field.Message() != nil) || shape.oneof != (field.ContainingOneof() != nil) {
				t.Fatalf("%s framing type differs from public schema", field.FullName())
			}
		}
	}
}

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

func TestExecuteDecodeLargeNativeBeforeUnmarshal(t *testing.T) {
	payload := bytes.Repeat([]byte{0xff}, (10<<20)+1)
	document := append(wireMessage(1, []byte("application/bson")), wireMessage(2, payload)...)
	native := wireMessage(2, document)
	raw := wireMessage(3, wireMessage(3, native))
	data := mem.BufferSlice{mem.SliceBuffer(raw)}
	defer data.Free()
	if err := ValidateExecuteFrame(data); err != nil {
		t.Fatal("large valid frame rejected", err)
	}
}

func TestExecuteDecodeNestedRequestContracts(t *testing.T) {
	resource := wireMessage(1, []byte("records"))
	condition := append(wireMessage(1, []byte("application/json")), wireMessage(2, []byte(`{"match_all":{}}`))...)
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
		{name: "opaque native document", kind: 3, payload: native, valid: true},
		{name: "unknown operation", kind: 4, payload: native},
		{name: "typed Lua", kind: 2, payload: mutation, valid: true},
		{name: "visible Lua", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, []byte{8, 1})...), valid: true},
		{name: "empty options", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, nil)...), valid: true},
		{name: "explicit application acknowledgement", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, []byte{8, 0})...), valid: true},
		{name: "duplicate options", kind: 2, payload: append(append(append([]byte(nil), mutation...), wireMessage(7, []byte{8, 1})...), wireMessage(7, nil)...)},
		{name: "duplicate visibility", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, []byte{8, 1, 8, 0})...)},
		{name: "options wire type", kind: 2, payload: append(append([]byte(nil), mutation...), 0x38, 1)},
		{name: "visibility wire type", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, wireMessage(1, nil))...)},
		{name: "unknown option", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, []byte{16, 1})...)},
		{name: "invalid visibility enum", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, []byte{8, 2})...)},
		{name: "negative visibility enum", kind: 2, payload: append(append([]byte(nil), mutation...), wireMessage(7, binary.AppendUvarint([]byte{8}, ^uint64(0)))...)},
		{name: "unknown mutation option", kind: 2, payload: append(append([]byte(nil), mutation...), 0x40, 1)},
		{name: "duplicate native document", kind: 3, payload: append(append([]byte(nil), native...), wireMessage(2, opaque)...)},
		{name: "unknown native backend variant", kind: 3, payload: append(append([]byte(nil), native...), wireMessage(3, nil)...)},
		{name: "duplicate native content type", kind: 3, payload: append(append([]byte(nil), resource...), wireMessage(2, append(opaque, wireMessage(1, []byte("application/http"))...))...)},
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
	payload := bytes.Repeat([]byte{0xff, 0x12, 0x80}, ((8<<20)-2)/3)
	document := append(wireMessage(1, []byte("application/vnd.future.store")), wireMessage(2, payload)...)
	native := append(wireMessage(1, []byte("records")), wireMessage(2, document)...)
	frame := wireMessage(3, wireMessage(3, native))
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
	oversizedDocument := append(wireMessage(1, []byte("application/vnd.future.store")), wireMessage(2, make([]byte, (8<<20)+256))...)
	native = append(wireMessage(1, []byte("records")), wireMessage(2, oversizedDocument)...)
	frame = wireMessage(3, wireMessage(3, native))
	oversized := mem.BufferSlice{mem.SliceBuffer(frame)}
	defer oversized.Free()
	if err := ValidateExecuteFrame(oversized); err != nil {
		t.Fatal("large Native document rejected", err)
	}
}
