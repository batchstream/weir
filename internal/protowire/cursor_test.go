package protowire

import (
	"bytes"
	"testing"

	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

type ownershipPool struct {
	puts int
}

func (*ownershipPool) Get(length int) *[]byte {
	data := make([]byte, length)
	return &data
}

func (pool *ownershipPool) Put(*[]byte) {
	pool.puts++
}

func TestValidatorsLeaveTransportOwnershipAndDataIntact(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 4096)
	document := append(wireMessage(1, []byte("application/vnd.test")), wireMessage(2, payload)...)
	native := wireMessage(3, wireMessage(4, append(wireMessage(1, []byte("records")), wireMessage(2, document)...)))
	repeated := wireMessage(2, payload)
	cases := []struct {
		name     string
		raw      []byte
		repeated bool
	}{
		{name: "execute", raw: native},
		{name: "repeated", raw: repeated, repeated: true},
	}
	for _, item := range cases {
		t.Run(item.name, func(t *testing.T) {
			pool := &ownershipPool{}
			raw := append([]byte(nil), item.raw...)
			buffer := mem.NewBuffer(&raw, pool)
			input := mem.BufferSlice{buffer}
			var err error
			if item.repeated {
				err = ValidateRepeatedMessages(input, 2, 2)
			} else {
				err = ValidateExecuteFrame(input)
			}
			if err != nil {
				t.Fatal(err)
			}
			if pool.puts != 0 || !bytes.Equal(buffer.ReadOnlyData(), item.raw) {
				t.Fatal("validation changed transport-owned data or references", pool.puts)
			}
			input.Free()
			if pool.puts != 1 {
				t.Fatal("validation retained a transport buffer", pool.puts)
			}
		})
	}
}

func FuzzFrameValidatorsAcrossFragments(f *testing.F) {
	read := wireMessage(3, wireMessage(1, wireMessage(1, []byte("records/s:record"))))
	f.Add(read, uint8(1))
	f.Add([]byte{0x0a, 1, 's', 0x12, 0, 0x12, 0}, uint8(0))
	f.Add([]byte{0x80}, uint8(0))
	f.Add(append(wireMessage(3, []byte{0x80}), 0x10, 1), uint8(1))
	f.Add([]byte{0x12, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02}, uint8(0))
	f.Fuzz(func(t *testing.T, raw []byte, span uint8) {
		if len(raw) > 64<<10 {
			t.Skip()
		}
		flat := mem.BufferSlice{mem.SliceBuffer(raw)}
		fragments := mem.BufferSlice{mem.SliceBuffer(nil), mem.SliceBuffer(nil)}
		width := int(span) + 1
		for offset := 0; offset < len(raw); offset += width {
			end := min(offset+width, len(raw))
			fragments = append(fragments, mem.SliceBuffer(raw[offset:end]), mem.SliceBuffer(nil))
		}
		defer flat.Free()
		defer fragments.Free()
		flatExecute := status.Convert(ValidateExecuteFrame(flat))
		fragmentExecute := status.Convert(ValidateExecuteFrame(fragments))
		if flatExecute.Code() != fragmentExecute.Code() || flatExecute.Message() != fragmentExecute.Message() {
			t.Fatal("execution framing depends on transport fragments", flatExecute, fragmentExecute)
		}
		flatRepeated := status.Convert(ValidateRepeatedMessages(flat, 2, 2))
		fragmentRepeated := status.Convert(ValidateRepeatedMessages(fragments, 2, 2))
		if flatRepeated.Code() != fragmentRepeated.Code() || flatRepeated.Message() != fragmentRepeated.Message() {
			t.Fatal("repeated-message framing depends on transport fragments", flatRepeated, fragmentRepeated)
		}
	})
}
