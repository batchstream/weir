package protowire

import (
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

func TestRepeatedMessageBudgetAcrossBuffers(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
		code codes.Code
	}{
		{name: "valid", raw: []byte{0x0a, 1, 's', 0x12, 0, 0x12, 0}, code: codes.OK},
		{name: "capacity", raw: []byte{0x12, 0, 0x12, 0, 0x12, 0}, code: codes.ResourceExhausted},
		{name: "tag", raw: []byte{0x80}, code: codes.InvalidArgument},
		{name: "wire-type", raw: []byte{0x10, 0}, code: codes.InvalidArgument},
		{name: "unknown-field", raw: []byte{0x1a, 0}, code: codes.InvalidArgument},
		{name: "length", raw: []byte{0x12, 0x80}, code: codes.InvalidArgument},
		{name: "length-overflow", raw: []byte{0x12, 0xff, 0xff, 0xff, 0xff, 0x7f}, code: codes.InvalidArgument},
		{name: "truncated", raw: []byte{0x12, 2, 'x'}, code: codes.InvalidArgument},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var input mem.BufferSlice
			for index := range test.raw {
				buffer := mem.SliceBuffer(test.raw[index : index+1])
				input = append(input, buffer)
			}
			defer input.Free()
			err := ValidateRepeatedMessages(input, 2, 2)
			if status.Code(err) != test.code {
				t.Fatal("unexpected framing result", err, test.code)
			}
		})
	}
}
