package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testpeer"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestExpressionRequiresMutatePermission(t *testing.T) {
	ca := testpeer.NewCA(t)
	identity, _, _ := ca.Identity(t, "node.weir.test")
	identity.ServerName = "node.weir.test"
	adapter, runtime := peerLocal(t, "records")
	service := Service{LocalStore: runtime}
	grants := map[string]map[string]Permission{"node.weir.test": {"records": ReadPermission}}
	opts := peerServerOptions{routes: map[string]Service{"records": service}, tls: identity, allow: grants}
	_, address := startPeerServer(t, opts)
	_, client := peerClient(t, address, identity)
	ctx, cancel := peerContext("4")
	defer cancel()
	ctx, stop := context.WithTimeout(ctx, time.Second)
	defer stop()
	d := &pb.Document{MediaType: "application/unknown", Data: []byte("opaque")}
	form := &pb.Transform_BackendExpression{BackendExpression: d}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	request := &pb.MutateRequest{Resource: "weir://records/db/c/s:a", Action: action}
	if _, err := client.Mutate(ctx, request); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	stream, err := client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	of := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: of}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	variant := &pb.BulkOperation_Mutate{Mutate: request}
	op := &pb.BulkOperation{Operation: variant}
	body := &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: body}
	_ = stream.Send(frame)
	_ = stream.CloseSend()
	if _, err := stream.Recv(); status.Code(err) != codes.PermissionDenied {
		t.Fatal(err)
	}
	if adapter.commands.Load() != 0 {
		t.Fatal("unauthorized expression executed")
	}
}
