package protowire

import (
	"testing"
	"unsafe"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc/mem"
	"google.golang.org/protobuf/proto"
)

func TestPeerDecodeBytesCountsEmptyObjectsAndRepeatedStringSlots(t *testing.T) {
	announcement := peerpb.NodeAnnouncement{}
	objectBytes := int(unsafe.Sizeof(announcement)) + 2*int(unsafe.Sizeof((*peerpb.NodeAnnouncement)(nil))) + 16
	for _, count := range []int{0, 1, 512, 1025} {
		entries := make([]*peerpb.NodeAnnouncement, count)
		for i := range entries {
			item := &peerpb.NodeAnnouncement{StoreNames: []string{"", "a"}, StoreEndpoints: []string{""}}
			entries[i] = item
		}
		request := &peerpb.SyncDirectoryRequest{Announcements: entries}
		data, err := proto.Marshal(request)
		if err != nil {
			t.Fatal(err)
		}
		input := mem.BufferSlice{mem.SliceBuffer(data)}
		estimated, err := PeerDecodeBytes(input)
		if err != nil {
			t.Fatal(err)
		}
		want := len(data) + 4096 + count*(objectBytes+6*int(unsafe.Sizeof("")))
		if estimated != want {
			t.Fatal("repeated allocations were hidden by the short wire length", count, estimated, want)
		}
		fragments := make(mem.BufferSlice, 0, len(data))
		for i := range data {
			fragments = append(fragments, mem.SliceBuffer(data[i:i+1]))
		}
		fragmented, err := PeerDecodeBytes(fragments)
		if err != nil || fragmented != estimated {
			t.Fatal("estimate depended on DATA fragmentation", fragmented, estimated, err)
		}
	}
}
func TestPeerDecodeBytesRejectsTruncatedOrUnknownNestedFields(t *testing.T) {
	for _, data := range [][]byte{{10, 2, 10}, {10, 2, 80, 1}, {10, 2, 10, 3}, {10, 1, 128}, {10, 2, 16, 128}} {
		input := mem.BufferSlice{mem.SliceBuffer(data)}
		if _, err := PeerDecodeBytes(input); err == nil {
			t.Fatal("malformed announcement reached native allocation", data)
		}
	}
}
