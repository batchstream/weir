package server

import (
	"context"
	"testing"
	"time"

	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestPeerExchangeDeadlineBoundsMissingSnapshotAndHalfClose(t *testing.T) {
	for _, mode := range []string{"missing snapshot", "missing half close"} {
		t.Run(mode, func(t *testing.T) {
			policy := directory.Policy{Lease: time.Second, SyncInterval: 10 * time.Millisecond, SyncTimeout: 40 * time.Millisecond}
			config := directory.Config{Group: "bounded-peer", Policy: policy}
			discovery, err := directory.New(config)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = discovery.Close(t.Context()) })
			options := peerServerOptions{directory: discovery, peer: true}
			server, address := startPeerServer(t, options)
			connection, _ := peerClient(t, address)
			client := peerpb.NewPeerDiscoveryServiceClient(connection)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if _, bounded := ctx.Deadline(); bounded {
				t.Fatal("fixture unexpectedly supplied a caller deadline")
			}
			stream, err := client.SyncDirectory(ctx)
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
			if mode == "missing half close" {
				if err := stream.Send(empty); err != nil {
					t.Fatal(err)
				}
			}
			finished := make(chan error, 1)
			go func() {
				_, err := stream.Recv()
				finished <- err
			}()
			select {
			case err := <-finished:
				if status.Code(err) != codes.DeadlineExceeded || ctx.Err() != nil {
					t.Fatal("peer policy did not bound the complete exchange", err, ctx.Err())
				}
			case <-time.After(time.Second):
				cancel()
				<-finished
				t.Fatal("decoded opening frame disabled the complete peer exchange deadline")
			}
			waitPeerIdle(t, server)
		})
	}
}
