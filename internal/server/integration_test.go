//go:build integration

package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type fixture struct {
	server  *Server
	runtime *store.Runtime
	client  pb.WeirClient
	native  *mongo.Client
	db      string
}

func setup(t *testing.T, batch bool) fixture {
	t.Helper()
	native, db := testmongo.Open(t)
	cfg := mongostore.Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records"}
	l := store.DefaultLimits()
	if !batch {
		l.BatchOperations = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	r, err := store.Open(ctx, cfg, l)
	if err != nil {
		t.Fatal(err)
	}
	sl := DefaultLimits()
	sl.Stall = 300 * time.Millisecond
	sl.BulkLifetime = 5 * time.Second
	s, err := New(r, "mongo", sl)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = s.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	f := fixture{server: s, runtime: r, client: pb.NewWeirClient(conn), native: native, db: db}
	return f
}
func resource(f fixture, id string) string { return "weir://mongo/" + f.db + "/records/s:" + id }
func mutation(f fixture, id string, n int32) *pb.MutateRequest {
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: n}}
	raw, _ := bson.Marshal(doc)
	d := &pb.Document{MediaType: "application/bson", Data: raw}
	v := &pb.MutateRequest_Put{Put: d}
	m := &pb.MutateRequest{Resource: resource(f, id), Action: v}
	return m
}
func openFrame() *pb.BulkRequestFrame {
	o := &pb.BulkOpen{Store: "weir://mongo"}
	v := &pb.BulkRequestFrame_Open{Open: o}
	f := &pb.BulkRequestFrame{Frame: v}
	return f
}
func opFrame(op *pb.BulkOperation) *pb.BulkRequestFrame {
	v := &pb.BulkRequestFrame_Operation{Operation: op}
	f := &pb.BulkRequestFrame{Frame: v}
	return f
}
func TestGRPCUnaryAndUnsupported(t *testing.T) {
	for _, batch := range []bool{false, true} {
		t.Run(fmt.Sprint(batch), func(t *testing.T) {
			f := setup(t, batch)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			read := &pb.ReadRequest{Resource: resource(f, "a")}
			r, err := f.client.Read(ctx, read)
			if err != nil || r.GetMissing() == nil {
				t.Fatal(err, r)
			}
			m := mutation(f, "a", 3)
			mr, err := f.client.Mutate(ctx, m)
			if err != nil || mr.Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal(err, mr)
			}
			r, err = f.client.Read(ctx, read)
			if err != nil || bson.Raw(r.GetDocument().Data).Lookup("n").Int32() != 3 {
				t.Fatal(err, r)
			}
			transform := &pb.Transform{}
			m.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			mr, err = f.client.Mutate(ctx, m)
			if err != nil || mr.Outcome != pb.MutationOutcome_NOT_STARTED || mr.Failure.Code != pb.FailureCode_UNSUPPORTED {
				t.Fatal(err, mr)
			}
			empty := &pb.Empty{}
			m.Action = &pb.MutateRequest_Delete{Delete: empty}
			for i := 0; i < 2; i++ {
				mr, err = f.client.Mutate(ctx, m)
				if err != nil || mr.Outcome != pb.MutationOutcome_APPLIED {
					t.Fatal("missing delete is applied", err, mr)
				}
			}
			m = mutation(f, "absent", 1)
			m.Action = &pb.MutateRequest_Replace{Replace: m.GetPut()}
			mr, err = f.client.Mutate(ctx, m)
			if err != nil || mr.Outcome != pb.MutationOutcome_NOT_APPLIED || mr.Failure.Code != pb.FailureCode_PRECONDITION_FAILED {
				t.Fatal(err, mr)
			}
			m = mutation(f, "a", 1)
			m.Resource = "weir://another/db/c/s:a"
			mr, err = f.client.Mutate(ctx, m)
			if err != nil || mr.Outcome != pb.MutationOutcome_NOT_STARTED {
				t.Fatal("validation", err, mr)
			}
			f.runtime.SetOverloaded(true)
			if _, err = f.client.Read(ctx, read); status.Code(err) != codes.ResourceExhausted {
				t.Fatal("overload", err)
			}
			f.runtime.SetOverloaded(false)
		})
	}
}
func TestGRPCDuplexSameKeyOrderCountsAndWrongStore(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sent := make(chan error, 1)
	go func() {
		if e := stream.Send(openFrame()); e != nil {
			sent <- e
			return
		}
		for i := uint64(0); i < 81; i++ {
			op := &pb.BulkOperation{Index: i}
			if i == 80 {
				r := &pb.ReadRequest{Resource: "weir://other/db/c/s:a"}
				op.Operation = &pb.BulkOperation_Read{Read: r}
			} else if i%2 == 0 {
				m := mutation(f, "same", int32(i))
				op.Operation = &pb.BulkOperation_Mutate{Mutate: m}
			} else {
				r := &pb.ReadRequest{Resource: resource(f, "same")}
				op.Operation = &pb.BulkOperation_Read{Read: r}
			}
			if e := stream.Send(opFrame(op)); e != nil {
				sent <- e
				return
			}
		}
		sent <- stream.CloseSend()
	}()
	seen := make(map[uint64]bool)
	endCount := 0
	for {
		frame, e := stream.Recv()
		if e == io.EOF {
			break
		}
		if e != nil {
			t.Fatal(e)
		}
		if end := frame.GetEnd(); end != nil {
			endCount++
			if end.ReceivedCount != 81 || end.ResultCount != 81 {
				t.Fatal(end)
			}
			continue
		}
		r := frame.GetResult()
		if r == nil || seen[r.Index] {
			t.Fatal("invalid/duplicate result")
		}
		seen[r.Index] = true
		if r.Index == 80 {
			if r.GetRead().GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal(r)
			}
		} else if r.Index%2 == 0 {
			if r.GetMutation().Outcome != pb.MutationOutcome_APPLIED {
				t.Fatal(r)
			}
		} else {
			doc := r.GetRead().GetDocument()
			if doc == nil || bson.Raw(doc.Data).Lookup("n").Int32() != int32(r.Index-1) {
				t.Fatal("same stream key order", r)
			}
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if len(seen) != 81 || endCount != 1 {
		t.Fatal("incomplete Bulk", len(seen), endCount)
	}
}
func TestGRPCEmptyMalformedAndTruncated(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	_ = stream.CloseSend()
	frame, err := stream.Recv()
	if err != nil || frame.GetEnd() == nil || frame.GetEnd().ReceivedCount != 0 {
		t.Fatal(err, frame)
	}
	if _, err = stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	stream, err = f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	r := &pb.ReadRequest{Resource: resource(f, "a")}
	v := &pb.BulkOperation_Read{Read: r}
	op := &pb.BulkOperation{Index: 2, Operation: v}
	_ = stream.Send(opFrame(op))
	_ = stream.CloseSend()
	if _, err = stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("bad index must truncate, not End", err)
	}
	stream, err = f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	_ = stream.Send(openFrame())
	if _, err = stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("second Open", err)
	}
}
func TestGRPCOutOfOrderCompletion(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Warm through the real shared path so AIMD, rather than a test-only setting, opens C=2.
	var tickets []*store.Ticket
	for i := 0; i < 24; i++ {
		r := &pb.ReadRequest{Resource: resource(f, fmt.Sprint(i))}
		v := &pb.BulkOperation_Read{Read: r}
		op := &pb.BulkOperation{Operation: v}
		p, e := f.runtime.Prepare(op)
		if e != nil {
			t.Fatal(e)
		}
		ticket, e, _ := f.runtime.Submit(ctx, p, nil)
		if e != nil {
			t.Fatal(e)
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		_, _ = ticket.Wait(ctx)
		ticket.Ack()
	}
	if f.runtime.Snapshot().Window < 2 {
		t.Fatal("warmup")
	}
	data := bson.D{{Key: "failCommands", Value: bson.A{"find"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 150}}
	testmongo.FailCommand(t, f.native, data, 1)
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	for i := uint64(0); i < 2; i++ {
		r := &pb.ReadRequest{Resource: resource(f, fmt.Sprint(i))}
		v := &pb.BulkOperation_Read{Read: r}
		op := &pb.BulkOperation{Index: i, Operation: v}
		_ = stream.Send(opFrame(op))
		if i == 0 {
			time.Sleep(20 * time.Millisecond)
		}
	}
	_ = stream.CloseSend()
	first, err := stream.Recv()
	if err != nil || first.GetResult().Index != 1 {
		t.Fatal("results incorrectly reordered", err, first)
	}
	second, err := stream.Recv()
	if err != nil || second.GetResult().Index != 0 {
		t.Fatal(err, second)
	}
	last, err := stream.Recv()
	if err != nil || last.GetEnd() == nil {
		t.Fatal(err, last)
	}
}
func TestGRPCStoppedConsumerMemoryAndShutdown(t *testing.T) {
	f := setup(t, true)
	payload := make([]byte, 200<<10)
	doc := bson.D{{Key: "_id", Value: "large"}, {Key: "payload", Value: payload}}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, doc); err != nil {
		t.Fatal(err)
	}
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	var sent atomic.Uint64
	producer := make(chan struct{})
	go func() {
		defer close(producer)
		for i := uint64(0); i < 100000; i++ {
			r := &pb.ReadRequest{Resource: resource(f, "large")}
			v := &pb.BulkOperation_Read{Read: r}
			op := &pb.BulkOperation{Index: i, Operation: v}
			if err := stream.Send(opFrame(op)); err != nil {
				return
			}
			sent.Add(1)
		}
	}()
	time.Sleep(100 * time.Millisecond)
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	peak := 0
	for i := 0; i < 40; i++ {
		s := f.runtime.Snapshot()
		if s.Retained > peak {
			peak = s.Retained
		}
		if s.Retained > 8 || s.Pending > 8 || s.ResultBytes > 8*(protocol.MaxDocument+protocol.ResultOverhead) {
			t.Fatal("bounds exceeded", s)
		}
		time.Sleep(10 * time.Millisecond)
	}
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc+16<<20 {
		t.Fatal("heap did not plateau", before.HeapAlloc, after.HeapAlloc)
	}
	select {
	case <-producer:
	case <-time.After(time.Second):
		t.Fatal("stalled stream did not stop producer")
	}
	drain, stop := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer stop()
	start := time.Now()
	if err := f.server.Shutdown(drain); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second || f.runtime.Snapshot().Retained != 0 {
		t.Fatal("stalled send shutdown leaked", f.runtime.Snapshot())
	}
	t.Logf("stopped consumer: sent=%d peak_retained=%d heap_before=%d heap_after=%d shutdown=%s", sent.Load(), peak, before.HeapAlloc, after.HeapAlloc, time.Since(start))
}

func TestGRPCShutdownDuringResultSend(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	doc := bson.D{{Key: "_id", Value: "large"}, {Key: "data", Value: make([]byte, 200<<10)}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, doc); err != nil {
		t.Fatal(err)
	}
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	for i := uint64(0); i < 20; i++ {
		r := &pb.ReadRequest{Resource: resource(f, "large")}
		v := &pb.BulkOperation_Read{Read: r}
		op := &pb.BulkOperation{Index: i, Operation: v}
		if err := stream.Send(opFrame(op)); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(70 * time.Millisecond)
	if f.runtime.Snapshot().Retained == 0 {
		t.Fatal("send never stalled")
	}
	drain, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer stop()
	start := time.Now()
	if err := f.server.Shutdown(drain); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("shutdown blocked on send")
	}
	for i := 0; i < 100 && f.runtime.Snapshot().Retained > 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if f.runtime.Snapshot().Retained != 0 {
		t.Fatal("result reservation leak", f.runtime.Snapshot())
	}
}
func TestGRPCSessionLimit(t *testing.T) {
	f := setup(t, true)
	for i := 0; i < f.server.limits.Sessions; i++ {
		if err := f.server.enter(); err != nil {
			t.Fatal(err)
		}
	}
	defer func() {
		for i := 0; i < f.server.limits.Sessions; i++ {
			<-f.server.slots
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	r := &pb.ReadRequest{Resource: resource(f, "a")}
	if _, err := f.client.Read(ctx, r); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("application session limit", err)
	}
}

func TestGRPCDrainWithoutClientHalfClose(t *testing.T) {
	f := setup(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	data := bson.D{{Key: "failCommands", Value: bson.A{"update"}}, {Key: "appName", Value: "weir:" + f.db}, {Key: "blockConnection", Value: true}, {Key: "blockTimeMS", Value: 100}}
	testmongo.FailCommand(t, f.native, data, 1)
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = stream.Send(openFrame())
	m := mutation(f, "drain", 1)
	v := &pb.BulkOperation_Mutate{Mutate: m}
	op := &pb.BulkOperation{Operation: v}
	_ = stream.Send(opFrame(op))
	for i := 0; i < 200 && f.runtime.Snapshot().Active == 0; i++ {
		time.Sleep(time.Millisecond)
	}
	if f.runtime.Snapshot().Active == 0 {
		t.Fatal("never dispatched")
	}
	closed := make(chan error, 1)
	go func() {
		drain, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		closed <- f.server.Shutdown(drain)
	}()
	frame, err := stream.Recv()
	if err != nil || frame.GetResult().GetMutation().Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("drain abandoned accepted mutation", err, frame)
	}
	if _, err = stream.Recv(); status.Code(err) != codes.Unavailable {
		t.Fatal("open producer must end without false Bulk End", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
}
