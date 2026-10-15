package directory

import (
	"bytes"
	"io"
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

func TestPeerResponseFramingHasNoAnnouncementQuota(t *testing.T) {
	codec := peerCodec{}
	for _, count := range []int{512, 1024, 8192} {
		raw := bytes.Repeat([]byte{0x0a, 0}, count)
		input := mem.BufferSlice{mem.SliceBuffer(raw)}
		decoded := &peerpb.SyncDirectoryResponse{}
		err := codec.Unmarshal(input, decoded)
		input.Free()
		if err != nil || len(decoded.Announcements) != count {
			t.Fatal("legitimate large response rejected", err, len(decoded.Announcements))
		}
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
}

func (rawDirectoryPeer) SyncDirectory(stream grpc.BidiStreamingServer[peerpb.SyncDirectoryRequest, peerpb.SyncDirectoryResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	response := &peerpb.SyncDirectoryResponse{}
	if err := stream.Send(response); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	_, err := stream.Recv()
	if err == io.EOF {
		return nil
	}
	return err
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

func TestSyncPeerAcceptsLargeWatermarksAndRejectsMalformedAnnouncements(t *testing.T) {
	cfg := Config{Group: "observer"}
	directory := newTestDirectory(t, cfg)
	announcements := directory.snapshot(time.Now())
	for i := 1; i < 640; i++ {
		node := announcement(i, "retired-"+strings.Repeat("g", 4096), "127.0.0.1:7447")
		node.Withdrawn = true
		node.StoreNames = nil
		node.StoreEndpoints = nil
		announcements = append(announcements, node)
	}
	response := &peerpb.SyncDirectoryResponse{Announcements: announcements}
	if proto.Size(response) <= 2<<20 {
		t.Fatal("peer codec fixture did not cross the former byte limit")
	}
	raw, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	address := startRawDirectoryPeer(t, raw)
	if err := directory.syncPeer(t.Context(), address); err != nil {
		t.Fatal("full withdrawal watermark response rejected", err)
	}
	if len(directory.records) != 640 {
		t.Fatal("legitimate watermarks lost", len(directory.records))
	}
	newNode := announcement(640, "unmerged", "0.0.0.0:7447")
	response.Announcements = append(response.Announcements, newNode)
	raw, err = proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	address = startRawDirectoryPeer(t, raw)
	err = directory.syncPeer(t.Context(), address)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal("malformed announcement escaped atomic validation", err)
	}
	if len(directory.records) != 640 {
		t.Fatal("invalid response changed directory capacity", len(directory.records))
	}
	if _, exists := directory.records[newNode.IncarnationId]; exists {
		t.Fatal("invalid response partially merged new owner")
	}
}
