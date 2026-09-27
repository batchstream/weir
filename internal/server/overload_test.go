package server

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOverloadDrainsAdmittedBulkResults(t *testing.T) {
	f := newChain(t, 0)
	gate := make(chan struct{})
	f.adapter.block = gate
	released := false
	defer func() {
		if !released {
			close(gate)
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://records"}
	opening := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: opening}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	read := &pb.BulkOperation_Read{Read: testRequest()}
	op := &pb.BulkOperation{Operation: read}
	variant := &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: variant}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	select {
	case <-f.adapter.seen:
	case <-ctx.Done():
		t.Fatal("not admitted")
	}
	for _, server := range f.servers {
		server.admission.SetOverloaded(true)
	}
	f.runtime.SetOverloaded(true)
	op = &pb.BulkOperation{Index: 1, Operation: read}
	variant = &pb.BulkRequestFrame_Operation{Operation: op}
	frame = &pb.BulkRequestFrame{Frame: variant}
	_ = stream.Send(frame)
	// Give rejection a chance to close input while the admitted result is held.
	response := make(chan error, 1)
	go func() {
		frame, err := stream.Recv()
		if err == nil && (frame.GetResult().GetIndex() != 0 || frame.GetResult().GetRead().GetMissing() == nil) {
			response <- status.Error(codes.Internal, "wrong result")
			return
		}
		response <- err
	}()
	select {
	case err := <-response:
		t.Fatal("admitted result interrupted", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(gate)
	released = true
	if err := <-response; err != nil {
		t.Fatal("admitted result", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("terminal overload", err)
	}
	waitPeerIdle(t, f.servers[0])
	if snapshot := f.runtime.Snapshot(); snapshot.Retained != 0 || snapshot.Active != 0 {
		t.Fatal("admitted ledger leaked", snapshot)
	}
}
