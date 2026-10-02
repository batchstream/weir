package server

import (
	"context"
	"net"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDiscoveryIngressRolesAndIndependentAdmission(t *testing.T) {
	adapter, runtime := peerLocal(t, "records")
	application, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		application.Close()
		t.Fatal(err)
	}
	cfg := directory.Config{Group: "records-group", PeerAddress: peer.Addr().String(), Targets: []string{application.Addr().String()}, Stores: []string{"records"}}
	d, err := directory.New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = d.Close(ctx)
	})
	limits := DefaultLimits()
	limits.Connections = 1
	admission, err := NewAdmission(limits)
	if err != nil {
		t.Fatal(err)
	}
	appOpts := peerServerOptions{stores: map[string]*store.Runtime{"records": runtime}, directory: d, listener: application, admission: admission, limits: limits}
	app, address := startPeerServer(t, appOpts)
	appConn, client := peerClient(t, address)
	peerOpts := peerServerOptions{directory: d, listener: peer, peer: true, admission: admission, limits: limits}
	peerServer, peerAddress := startPeerServer(t, peerOpts)
	peerConn, peerWeir := peerClient(t, peerAddress)
	peerDirectory := peerpb.NewPeerDiscoveryServiceClient(peerConn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	request := &pb.ResolveStoreRequest{StoreName: "records"}
	response, err := client.ResolveStore(ctx, request)
	if err != nil || len(response.Endpoints) != 1 || response.Endpoints[0] != address {
		t.Fatal(response, err)
	}
	for range cap(admission.slots) {
		admission.slots <- struct{}{}
	}
	if _, err := client.ResolveStore(ctx, request); err != nil {
		t.Fatal("business saturation blocked control ResolveStore", err)
	}
	syncRequest := &peerpb.SyncDirectoryRequest{}
	if _, err := peerDirectory.SyncDirectory(ctx, syncRequest); err != nil {
		t.Fatal("business connection saturation blocked peer control", err)
	}
	for range cap(admission.slots) {
		<-admission.slots
	}
	if len(admission.connections) != 1 || len(peerServer.connectionSlots) != 1 {
		t.Fatal("application and peer physical connections share capacity", len(admission.connections), len(peerServer.connectionSlots))
	}
	appDirectory := peerpb.NewPeerDiscoveryServiceClient(appConn)
	if _, err := appDirectory.SyncDirectory(ctx, syncRequest); status.Code(err) != codes.Unimplemented {
		t.Fatal("application exposes peer SyncDirectory", err)
	}
	if _, err := peerWeir.ResolveStore(ctx, request); status.Code(err) != codes.Unimplemented {
		t.Fatal("peer exposes public ResolveStore", err)
	}
	if _, err := routeRead(peerWeir, ctx, testRequest()); status.Code(err) != codes.Unimplemented {
		t.Fatal("peer exposes public Execute", err)
	}
	for name, conn := range map[string]*grpc.ClientConn{"application": appConn, "peer": peerConn} {
		for _, path := range []string{"/weir.v1.Weir/Resolve", "/weir.v1.Weir/Route", "/weir.v1.Directory/Exchange"} {
			t.Run(name+path, func(t *testing.T) {
				request := &pb.Empty{}
				response := &pb.Empty{}
				err := conn.Invoke(ctx, path, request, response)
				if status.Code(err) != codes.Unimplemented {
					t.Fatal("removed RPC path accepted", path, err)
				}
			})
		}
	}
	unknown := &pb.ResolveStoreRequest{StoreName: "unknown"}
	if _, err := client.ResolveStore(ctx, unknown); status.Code(err) != codes.Unavailable {
		t.Fatal("unknown Store did not return retryable discovery error", err)
	}
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	business := routeAcceptanceRead(1, "data/s:key")
	business.StoreName = "unknown"
	if err := stream.Send(business); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	if _, err := stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatal("unhosted Store was accepted", err)
	}
	if adapter.commands.Load() != 0 {
		t.Fatal("unhosted Store executed business data")
	}
	waitPeerIdle(t, app)
}
