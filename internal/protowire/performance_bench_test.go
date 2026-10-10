package protowire

import (
	"bytes"
	"testing"

	"google.golang.org/grpc/mem"
)

func BenchmarkValidateFrames(b *testing.B) {
	read := wireMessage(3, wireMessage(1, wireMessage(1, []byte("records/s:record"))))
	document := append(wireMessage(1, []byte("application/vnd.test")), wireMessage(2, bytes.Repeat([]byte("x"), 64<<10))...)
	native := wireMessage(3, wireMessage(4, append(wireMessage(1, []byte("records")), wireMessage(2, document)...)))
	repeated := append(wireMessage(1, []byte("seed")), wireMessage(2, []byte("announcement"))...)
	cases := []struct {
		name     string
		raw      []byte
		fragment bool
		repeated bool
	}{
		{name: "Read", raw: read},
		{name: "ReadByteFragments", raw: read, fragment: true},
		{name: "Native64KiB", raw: native},
		{name: "RepeatedMessages", raw: repeated, repeated: true},
	}
	for _, item := range cases {
		b.Run(item.name, func(b *testing.B) {
			input := mem.BufferSlice{mem.SliceBuffer(item.raw)}
			if item.fragment {
				input = nil
				for i := range item.raw {
					input = append(input, mem.SliceBuffer(item.raw[i:i+1]))
				}
			}
			defer input.Free()
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var err error
				if item.repeated {
					err = ValidateRepeatedMessages(input, 2, 2)
				} else {
					err = ValidateExecuteFrame(input)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
