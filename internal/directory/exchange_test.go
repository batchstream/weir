package directory

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func serveDirectoryTestPeer(t *testing.T, peer peerpb.PeerDiscoveryServiceServer) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
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

type terminalDirectoryPeer struct {
	mode string
}

func (peer terminalDirectoryPeer) SyncDirectory(stream grpc.BidiStreamingServer[peerpb.SyncDirectoryRequest, peerpb.SyncDirectoryResponse]) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	ad := announcement(1, "owner", "127.0.0.1:7447")
	if peer.mode == "expired before completion" {
		ad.LeaseRemainingMs = 10
	}
	response := &peerpb.SyncDirectoryResponse{Announcements: []*peerpb.NodeAnnouncement{ad}}
	if err := stream.Send(response); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != nil {
		return err
	}
	if _, err := stream.Recv(); err != io.EOF {
		return status.Error(codes.InvalidArgument, "missing half close")
	}
	switch peer.mode {
	case "extra response":
		return stream.Send(response)
	case "failed completion":
		return status.Error(codes.Unavailable, "injected final status")
	case "expired before completion":
		timer := time.NewTimer(25 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	return nil
}

func TestDirectoryInstallsOnlyOneCompleteTimelyResponse(t *testing.T) {
	for _, mode := range []string{"valid", "extra response", "failed completion", "expired before completion"} {
		t.Run(mode, func(t *testing.T) {
			peer := terminalDirectoryPeer{mode: mode}
			address := serveDirectoryTestPeer(t, peer)
			config := Config{Group: "observer"}
			directory := newTestDirectory(t, config)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			err := directory.syncPeer(ctx, address)
			want := codes.OK
			if mode == "extra response" {
				want = codes.InvalidArgument
			} else if mode == "failed completion" {
				want = codes.Unavailable
			}
			if status.Code(err) != want {
				t.Fatal("unexpected final exchange outcome", err)
			}
			_, installed := directory.records[announcement(1, "owner", "127.0.0.1:7447").IncarnationId]
			if installed != (mode == "valid") {
				t.Fatal("incomplete or expired exchange installed remote ownership", mode, installed)
			}
		})
	}
}

func TestDirectoryRejectsExtraRequestsBeforeInstallingSnapshot(t *testing.T) {
	config := Config{Group: "observer"}
	directory := newTestDirectory(t, config)
	address := serveDirectoryTestPeer(t, directory)
	connection, err := grpc.NewClient("passthrough:///"+address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	peer := peerpb.NewPeerDiscoveryServiceClient(connection)
	stream, err := peer.SyncDirectory(ctx)
	if err != nil {
		t.Fatal(err)
	}
	empty := &peerpb.SyncDirectoryRequest{}
	if err := stream.Send(empty); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	ad := announcement(1, "owner", "127.0.0.1:7447")
	request := &peerpb.SyncDirectoryRequest{Announcements: []*peerpb.NodeAnnouncement{ad}}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(empty); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("extra peer request accepted", err)
	}
	if len(directory.records) != 1 {
		t.Fatal("extra request partially installed ownership")
	}
}
