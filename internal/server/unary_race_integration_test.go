//go:build integration

package server

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
)

type unaryObservation struct {
	bytes    int
	complete bool
	err      error
}

func receiveUnary(stream grpc.ClientStream) unaryObservation {
	var reply pb.ReadResult
	result := unaryObservation{}
	result.err = stream.RecvMsg(&reply)
	if result.err != nil {
		return result
	}
	result.bytes = len(reply.GetDocument().GetData())
	var extra pb.ReadResult
	result.err = stream.RecvMsg(&extra)
	result.complete = errors.Is(result.err, io.EOF)
	return result
}

func TestUnaryCancelSendDeadlineRacesReleaseResources(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = 100 * time.Millisecond
	limits.Stall = 100 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	setupCtx, stopSetup := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopSetup()
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "payload", Value: make([]byte, 200<<10)}}
	encoded, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(setupCtx, document); err != nil {
		t.Fatal(err)
	}
	conn := acceptanceConnection(t, f.address)
	successes := 0
	for i := 0; i < 12; i++ {
		call, cancel := context.WithCancel(context.Background())
		desc := &grpc.StreamDesc{ServerStreams: true}
		stream, err := conn.NewStream(call, desc, pb.Weir_Read_FullMethodName)
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		request := &pb.ReadRequest{Resource: resource(f, "large")}
		if err := stream.SendMsg(request); err != nil {
			cancel()
			t.Fatal(err)
		}
		if err := stream.CloseSend(); err != nil {
			cancel()
			t.Fatal(err)
		}
		if _, err := stream.Header(); err != nil {
			cancel()
			t.Fatal(err)
		}
		gate := make(chan struct{})
		done := make(chan unaryObservation, 1)
		cancelled := make(chan struct{})
		go func() { <-gate; done <- receiveUnary(stream) }()
		go func() {
			defer close(cancelled)
			<-gate
			if i%4 != 0 {
				cancel()
			}
		}()
		time.Sleep([]time.Duration{0, 40 * time.Millisecond, 95 * time.Millisecond, 110 * time.Millisecond}[i%4])
		close(gate)
		select {
		case result := <-done:
			if result.complete {
				successes++
				if result.bytes != len(encoded) {
					t.Fatal("successful unary had a truncated document", result)
				}
			} else if result.err == nil {
				t.Fatal("missing terminal status", result)
			}
		case <-time.After(time.Second):
			cancel()
			_ = conn.Close()
			t.Fatal("send/cancel/deadline race did not terminate")
		}
		<-cancelled
		cancel()
		assertUnaryResourcesReleased(t, f)
	}
	if successes == 0 {
		t.Fatal("normal completion branch was never exercised")
	}
	// Wait beyond retired deadlines, then prove the same healthy connection works.
	var physical any
	f.server.connections.Range(func(_, v any) bool { physical = v; return false })
	time.Sleep(200 * time.Millisecond)
	present := false
	f.server.connections.Range(func(_, v any) bool { present = present || v == physical; return true })
	if !present {
		t.Fatal("a retired timer closed the connection")
	}
	client := pb.NewWeirClient(conn)
	request := &pb.ReadRequest{Resource: resource(f, "missing")}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Read(ctx, request); err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	_ = f.conn.Close()
	if err := f.server.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	assertClosedUnaryServer(t, f)
	t.Logf("12 send/cancel/deadline races completed; normal successes=%d; no server delivery goroutines remain", successes)
}

func TestUnaryShutdownRacesWithCancelAndSend(t *testing.T) {
	for i := 0; i < 3; i++ {
		limits := DefaultLimits()
		limits.UnaryLifetime = 100 * time.Millisecond
		limits.Stall = 100 * time.Millisecond
		f := setupWithLimits(t, true, limits)
		ctx, cancel := context.WithCancel(context.Background())
		document := bson.D{{Key: "_id", Value: "large"}, {Key: "data", Value: make([]byte, 200<<10)}}
		setupCtx, stop := context.WithTimeout(context.Background(), time.Second)
		_, err := f.native.Database(f.db).Collection("records").InsertOne(setupCtx, document)
		stop()
		if err != nil {
			cancel()
			t.Fatal(err)
		}
		stream := startPausedUnaryRead(t, f, ctx)
		gate := make(chan struct{})
		received := make(chan unaryObservation, 1)
		closed := make(chan error, 1)
		cancelled := make(chan struct{})
		go func() { <-gate; received <- receiveUnary(stream) }()
		go func() {
			<-gate
			drain, stop := context.WithTimeout(context.Background(), 25*time.Millisecond)
			defer stop()
			closed <- f.server.Shutdown(drain)
		}()
		go func() { defer close(cancelled); <-gate; cancel() }()
		time.Sleep(time.Duration(i) * 45 * time.Millisecond)
		close(gate)
		select {
		case err := <-closed:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(time.Second):
			t.Fatal("shutdown race did not finish")
		}
		select {
		case result := <-received:
			if result.complete && result.bytes == 0 {
				t.Fatal("empty successful response")
			}
		case <-time.After(time.Second):
			t.Fatal("response waiter survived shutdown")
		}
		<-cancelled
		_ = f.conn.Close()
		assertClosedUnaryServer(t, f)
	}
}

func assertClosedUnaryServer(t *testing.T, f fixture) {
	t.Helper()
	assertUnaryResourcesReleased(t, f)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		connections := 0
		f.server.connections.Range(func(_, _ any) bool { connections++; return true })
		buffer := make([]byte, 2<<20)
		n := runtime.Stack(buffer, true)
		if n == len(buffer) {
			t.Fatal("goroutine audit truncated")
		}
		stacks := string(buffer[:n])
		active := strings.Contains(stacks, "internal/server.(*Server).serveHTTP") || strings.Contains(stacks, "internal/server.(*creditedBody).Read") || strings.Contains(stacks, "internal/transport.(*serverHandlerTransport).HandleStreams")
		if connections == 0 && !active {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("closed server still has connections or delivery goroutines")
}

func TestUnaryConnectionAbortDoesNotAffectOtherConnections(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = 2 * time.Second
	limits.BulkLifetime = 2 * time.Second
	limits.Stall = 200 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	document := bson.D{{Key: "_id", Value: "large"}, {Key: "data", Value: make([]byte, 200<<10)}}
	if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, document); err != nil {
		t.Fatal(err)
	}
	warm := &pb.ReadRequest{Resource: resource(f, "warm")}
	if _, err := f.client.Read(ctx, warm); err != nil {
		t.Fatal(err)
	}
	var affected *limitedConn
	f.server.connections.Range(func(_, v any) bool { affected = v.(*limitedConn); return false })
	other := acceptanceConnection(t, f.address)
	otherClient := pb.NewWeirClient(other)
	if _, err := otherClient.Read(ctx, warm); err != nil {
		t.Fatal(err)
	}
	idle, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := idle.Send(openFrame()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond)
	stream := startPausedUnaryRead(t, f, ctx)
	// Bulk's connection-wide input-stall watchdog runs before this unary's
	// own send budget. It is an existing documented connection-failure path.
	if _, err := idle.Recv(); err == nil {
		t.Fatal("idle Bulk should not complete normally")
	}
	var response pb.ReadResult
	if err := stream.RecvMsg(&response); err == nil {
		t.Fatal("same-connection unary survived a connection abort")
	}
	if affected == nil || !affected.closed.Load() {
		t.Fatal("connection-wide failure was not exercised")
	}
	result, err := otherClient.Read(ctx, warm)
	if err != nil || result.GetMissing() == nil {
		t.Fatal("other connection stopped serving", err, result)
	}
	assertUnaryResourcesReleased(t, f)
}
