//go:build integration

package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Adapted from independent acceptance without an overlay or external file dependency.
func TestAcceptanceUnaryTransportLifetime(t *testing.T) {
	for _, mode := range []string{"no_client_deadline", "longer_client_deadline", "shorter_client_deadline"} {
		t.Run(mode, func(t *testing.T) {
			limits := DefaultLimits()
			limits.UnaryLifetime = 100 * time.Millisecond
			limits.Stall = 100 * time.Millisecond
			f := setupWithLimits(t, true, limits)
			document := bson.D{{Key: "_id", Value: "large"}, {Key: "payload", Value: make([]byte, 200<<10)}}
			setupCtx, stopSetup := context.WithTimeout(context.Background(), 2*time.Second)
			defer stopSetup()
			if _, err := f.native.Database(f.db).Collection("records").InsertOne(setupCtx, document); err != nil {
				t.Fatal(err)
			}
			connection := acceptanceConnection(t, f.address)
			client := pb.NewWeirClient(connection)
			warm := &pb.ReadRequest{Resource: resource(f, "warm")}
			if _, err := client.Read(setupCtx, warm); err != nil {
				t.Fatal(err)
			}
			var call context.Context
			var cancel context.CancelFunc
			switch mode {
			case "no_client_deadline":
				call, cancel = context.WithCancel(context.Background())
			case "longer_client_deadline":
				call, cancel = context.WithTimeout(context.Background(), 2*time.Second)
			default:
				call, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
			}
			defer cancel()
			if mode == "no_client_deadline" {
				if _, ok := call.Deadline(); ok {
					t.Fatal("test must not set a client deadline")
				}
			}
			desc := &grpc.StreamDesc{ServerStreams: true}
			stream, err := connection.NewStream(call, desc, pb.Weir_Read_FullMethodName)
			if err != nil {
				t.Fatal(err)
			}
			request := &pb.ReadRequest{Resource: resource(f, "large")}
			start := time.Now()
			if err := stream.SendMsg(request); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if _, err := stream.Header(); err != nil {
				t.Fatal(err)
			}
			time.Sleep(600 * time.Millisecond)
			t.Logf("before resuming reads: elapsed=%s client=%v slots=%d runtime=%+v", time.Since(start), call.Err(), len(f.server.slots), f.runtime.Snapshot())
			done := make(chan error, 1)
			go func() {
				var reply pb.ReadResult
				if err := stream.RecvMsg(&reply); err != nil {
					done <- err
					return
				}
				var extra pb.ReadResult
				done <- stream.RecvMsg(&extra)
			}()
			select {
			case terminal := <-done:
				if terminal == nil || errors.Is(terminal, io.EOF) {
					t.Fatal("complete response and gRPC OK after server bound", terminal)
				}
				t.Log("unary correctly terminated:", terminal)
			case <-time.After(time.Second):
				cancel()
				<-done
				t.Fatal("blocked response was not terminated")
			}
			assertUnaryResourcesReleased(t, f)
		})
	}
}

func acceptanceConnection(t *testing.T, address string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(65535))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func assertUnaryResourcesReleased(t *testing.T, f fixture) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := f.runtime.Snapshot()
		if snapshot.Pending == 0 && snapshot.Active == 0 && snapshot.Retained == 0 && snapshot.ResultBytes == 0 && len(f.server.slots) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("unary resources not released", len(f.server.slots), f.runtime.Snapshot())
}

func TestAcceptanceNormalUnaryResponses(t *testing.T) {
	limits := DefaultLimits()
	limits.UnaryLifetime = 100 * time.Millisecond
	limits.Stall = 100 * time.Millisecond
	f := setupWithLimits(t, true, limits)
	connection := acceptanceConnection(t, f.address)
	client := pb.NewWeirClient(connection)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, size := range []int{32, 200 << 10} {
		id := fmt.Sprint(size)
		document := bson.D{{Key: "_id", Value: id}, {Key: "payload", Value: make([]byte, size)}}
		if _, err := f.native.Database(f.db).Collection("records").InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
		request := &pb.ReadRequest{Resource: resource(f, id)}
		result, err := client.Read(ctx, request)
		if err != nil || result.GetDocument() == nil {
			t.Fatal("normal unary failed", size, err, result)
		}
		_, payload, ok := bson.Raw(result.GetDocument().Data).Lookup("payload").BinaryOK()
		if !ok || len(payload) != size {
			t.Fatal("normal response was truncated", size, len(payload))
		}
	}
	assertUnaryResourcesReleased(t, f)
	var physical any
	f.server.connections.Range(func(_, v any) bool { physical = v; return false })
	time.Sleep(250 * time.Millisecond)
	same := false
	f.server.connections.Range(func(_, v any) bool { same = same || v == physical; return true })
	if !same {
		t.Fatal("a completed unary left a connection-closing timer")
	}
	request := &pb.ReadRequest{Resource: resource(f, "32")}
	if _, err := client.Read(ctx, request); err != nil {
		t.Fatal("healthy connection failed after old deadlines", err)
	}
}
