package directory

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestPeerResponseDecodeBudgetBeforeObjectConstruction(t *testing.T) {
	codec := peerCodec{}
	for _, count := range []int{MaxAnnouncements, MaxAnnouncements + 1} {
		raw := bytes.Repeat([]byte{0x0a, 0}, count)
		input := mem.BufferSlice{mem.SliceBuffer(raw)}
		decoded := &peerpb.SyncDirectoryResponse{}
		err := codec.Unmarshal(input, decoded)
		input.Free()
		if count == MaxAnnouncements && (err != nil || len(decoded.Announcements) != count) {
			t.Fatal("legitimate response capacity rejected", err, len(decoded.Announcements))
		}
		if count > MaxAnnouncements && (status.Code(err) != codes.ResourceExhausted || len(decoded.Announcements) != 0) {
			t.Fatal("over-budget response constructed announcements", err, len(decoded.Announcements))
		}
	}
	input := mem.BufferSlice{mem.SliceBuffer(bytes.Repeat([]byte{0x0a, 0}, MaxSyncBytes/2+1))}
	decoded := &peerpb.SyncDirectoryResponse{}
	err := codec.Unmarshal(input, decoded)
	input.Free()
	if status.Code(err) != codes.ResourceExhausted || len(decoded.Announcements) != 0 {
		t.Fatal("oversized peer response constructed objects", err, len(decoded.Announcements))
	}
}

// A real peer can emit arbitrary protobuf bytes without constructing the
// equivalent message tree. This exercises the actual client transport codec.
type rawPeerResponseCodec struct {
	raw []byte
}

func (rawPeerResponseCodec) Name() string { return "proto" }

func (c rawPeerResponseCodec) Marshal(any) (mem.BufferSlice, error) {
	buffer := mem.SliceBuffer(c.raw)
	encoded := mem.BufferSlice{buffer}
	return encoded, nil
}

func (rawPeerResponseCodec) Unmarshal(data mem.BufferSlice, value any) error {
	return encoding.GetCodecV2("proto").Unmarshal(data, value)
}

type rawDirectoryPeer struct {
	peerpb.UnimplementedPeerDiscoveryServiceServer
}

func (rawDirectoryPeer) SyncDirectory(context.Context, *peerpb.SyncDirectoryRequest) (*peerpb.SyncDirectoryResponse, error) {
	response := &peerpb.SyncDirectoryResponse{}
	return response, nil
}

func startRawDirectoryPeer(t *testing.T, raw []byte) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	codec := rawPeerResponseCodec{raw: raw}
	server := grpc.NewServer(grpc.ForceServerCodecV2(codec))
	peer := rawDirectoryPeer{}
	peerpb.RegisterPeerDiscoveryServiceServer(server, peer)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		<-done
	})
	return listener.Addr().String()
}

func TestSyncPeerAcceptsFullWatermarksAndRejectsRawAmplification(t *testing.T) {
	cfg := Config{Group: "observer"}
	directory := newTestDirectory(t, cfg)
	announcements := directory.snapshot(time.Now())
	for i := 1; i < MaxAnnouncements; i++ {
		node := announcement(i, "retired", "127.0.0.1:7447")
		node.Withdrawn = true
		node.StoreNames = nil
		node.StoreEndpoints = nil
		announcements = append(announcements, node)
	}
	response := &peerpb.SyncDirectoryResponse{Announcements: announcements}
	raw, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	address := startRawDirectoryPeer(t, raw)
	if err := directory.syncPeer(t.Context(), address); err != nil {
		t.Fatal("full withdrawal watermark response rejected", err)
	}
	if len(directory.records) != MaxAnnouncements {
		t.Fatal("legitimate watermarks lost", len(directory.records))
	}
	newNode := announcement(MaxAnnouncements, "unmerged", "127.0.0.1:7447")
	response.Announcements = append(response.Announcements, newNode)
	raw, err = proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	address = startRawDirectoryPeer(t, raw)
	err = directory.syncPeer(t.Context(), address)
	// Native gRPC wraps codec errors as Internal. The decode budget must reject
	// this reply before the application's directory merge sees its messages.
	if status.Code(err) != codes.Internal || !strings.Contains(status.Convert(err).Message(), "metadata exceeds decode budget") {
		t.Fatal("over-budget response escaped native client codec", err)
	}
	if len(directory.records) != MaxAnnouncements {
		t.Fatal("invalid response changed directory capacity", len(directory.records))
	}
	if _, exists := directory.records[newNode.IncarnationId]; exists {
		t.Fatal("invalid response partially merged new owner")
	}
}
