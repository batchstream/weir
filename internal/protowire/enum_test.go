package protowire

import (
	"encoding/binary"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestExecuteDecodeProjectionRawEnumsBeforeNativeTruncation(t *testing.T) {
	cases := []struct {
		name        string
		value       uint64
		wireValid   bool
		publicValid bool
	}{
		{name: "unspecified", value: uint64(pb.ProjectionMode_PROJECTION_MODE_UNSPECIFIED), wireValid: true},
		{name: "include", value: uint64(pb.ProjectionMode_INCLUDE), wireValid: true, publicValid: true},
		{name: "exclude", value: uint64(pb.ProjectionMode_EXCLUDE), wireValid: true, publicValid: true},
		{name: "unknown", value: 3},
		{name: "uint32 maximum", value: 1<<32 - 1},
		{name: "high bits unspecified", value: 1 << 32},
		{name: "high bits include", value: 1<<32 | uint64(pb.ProjectionMode_INCLUDE)},
		{name: "high bits exclude", value: 1<<32 | uint64(pb.ProjectionMode_EXCLUDE)},
		{name: "negative", value: ^uint64(0)},
		{name: "negative alias include", value: 0xffffffff00000001},
		{name: "negative alias exclude", value: 0xffffffff00000002},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			projection := binary.AppendUvarint([]byte{8}, item.value)
			projection = append(projection, wireMessage(2, []byte("name"))...)
			scan := append(wireMessage(1, []byte("records")), wireMessage(3, projection)...)
			command := wireMessage(3, scan)
			frame := append(wireMessage(1, []byte("records")), 16, 1)
			frame = append(frame, wireMessage(3, command)...)
			for _, fragmented := range []bool{false, true} {
				data := mem.BufferSlice{mem.SliceBuffer(frame)}
				if fragmented {
					data = nil
					for i := range frame {
						data = append(data, mem.SliceBuffer(nil), mem.SliceBuffer(frame[i:i+1]))
					}
				}
				err := ValidateExecuteFrame(data)
				data.Free()
				if (err == nil) != item.wireValid {
					t.Fatalf("raw ProjectionMode %d, fragmented=%t: framing error %v", item.value, fragmented, err)
				}
				if err != nil {
					continue
				}
				decoded := &pb.ExecuteRequest{}
				if err := proto.Unmarshal(frame, decoded); err != nil {
					t.Fatal(err)
				}
				validationErr := protocol.ValidateExecuteRequest(decoded)
				if (validationErr == nil) != item.publicValid {
					t.Fatalf("raw ProjectionMode %d decoded as %d: public validation %v", item.value, decoded.Command.GetScan().Projection.Mode, validationErr)
				}
			}
		})
	}
}
