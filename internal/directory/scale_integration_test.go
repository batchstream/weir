//go:build integration

package directory_test

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/server"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func directoryScaleServer(t *testing.T, config server.Config) string {
	t.Helper()
	admission, err := server.NewAdmission(config.Limits)
	if err != nil {
		t.Fatal(err)
	}
	config.Admission = admission
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	instance, err := server.New(config)
	if err != nil {
		_ = listener.Close()
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- instance.Serve(listener) }()
	<-instance.Serving()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := instance.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		_ = listener.Close()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
	return listener.Addr().String()
}

func TestDirectoryLargeMetadataAcrossProductionGRPC(t *testing.T) {
	stores, targets := make([]string, 32), make([]string, 32)
	for i := range stores {
		stores[i] = fmt.Sprintf("store-%03d", i)
		targets[i] = fmt.Sprintf("127.0.0.1:%d", 7400+i)
	}
	group := strings.Repeat("replica-group-", 512)
	config := directory.Config{Group: group, Stores: stores, Targets: targets}
	owner, err := directory.New(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close(context.Background()) })
	peerConfig := server.Config{Directory: owner, Peer: true, Limits: server.DefaultLimits()}
	peerAddress := directoryScaleServer(t, peerConfig)
	businessConfig := server.Config{Directory: owner, Limits: server.DefaultLimits()}
	businessAddress := directoryScaleServer(t, businessConfig)
	callOptions := []grpc.CallOption{grpc.MaxCallSendMsgSize(protocol.MaxTransportMessageBytes), grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes)}
	dialOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry(), grpc.WithDefaultCallOptions(callOptions...)}
	peerConnection, err := grpc.NewClient("passthrough:///"+peerAddress, dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer peerConnection.Close()
	announcements := make([]*peerpb.NodeAnnouncement, 640)
	for i := range announcements {
		announcements[i] = &peerpb.NodeAnnouncement{IncarnationId: fmt.Sprintf("%032x", i+1), Revision: 1, ReplicaGroup: group, StoreNames: stores, StoreEndpoints: targets, LeaseRemainingMs: 30000}
	}
	request := &peerpb.SyncDirectoryRequest{Announcements: announcements}
	if proto.Size(request) <= 2<<20 {
		t.Fatal("metadata fixture did not cross the former peer byte limit", proto.Size(request))
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	peer := peerpb.NewPeerDiscoveryServiceClient(peerConnection)
	response, err := peer.SyncDirectory(ctx, request)
	if err != nil || len(response.GetAnnouncements()) != 641 || proto.Size(response) <= 2<<20 {
		t.Fatal("production gRPC directory truncated or rejected large metadata", err, len(response.GetAnnouncements()), proto.Size(response))
	}
	t.Logf("announcements=%d storesPerNode=%d endpointsPerNode=%d requestBytes=%d responseBytes=%d", len(announcements), len(stores), len(targets), proto.Size(request), proto.Size(response))
	businessConnection, err := grpc.NewClient("passthrough:///"+businessAddress, dialOptions...)
	if err != nil {
		t.Fatal(err)
	}
	defer businessConnection.Close()
	client := pb.NewStoreServiceClient(businessConnection)
	resolve := &pb.ResolveStoreRequest{StoreName: stores[len(stores)-1]}
	resolved, err := client.ResolveStore(ctx, resolve)
	if err != nil || !slices.Equal(resolved.GetEndpoints(), targets) {
		t.Fatal("production resolver lost endpoints beyond the old target count", err, resolved)
	}
	malformed := proto.Clone(announcements[0]).(*peerpb.NodeAnnouncement)
	malformed.Revision = 2
	malformed.PeerEndpoint = "0.0.0.0:7447"
	valid := proto.Clone(announcements[1]).(*peerpb.NodeAnnouncement)
	valid.Revision = 2
	invalidRequest := &peerpb.SyncDirectoryRequest{Announcements: []*peerpb.NodeAnnouncement{valid, malformed}}
	if _, err := peer.SyncDirectory(ctx, invalidRequest); status.Code(err) != codes.InvalidArgument {
		t.Fatal("large-scale directory lost semantic validation", err)
	}
	empty := &peerpb.SyncDirectoryRequest{}
	response, err = peer.SyncDirectory(ctx, empty)
	if err != nil || len(response.GetAnnouncements()) != 641 {
		t.Fatal("invalid snapshot partially changed membership", err)
	}
	for _, node := range response.Announcements {
		if node.IncarnationId == valid.IncarnationId && node.Revision != 1 {
			t.Fatal("invalid snapshot partially advanced another node revision", node.Revision)
		}
	}
}
