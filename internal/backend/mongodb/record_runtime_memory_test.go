package mongodb

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestMongoMaximumReadRunsThroughRuntimeAndActualCodecBudget(t *testing.T) {
	const documentBytes = (16 << 20) - 64
	const database = "capacity_fixture1"
	document := bson.D{{Key: "_id", Value: "sdk-write-256-16777152"}, {Key: "n", Value: int64(256)}, {Key: "pad", Value: ""}}
	document[2].Value = strings.Repeat("x", documentBytes-len(expressionBSON(t, document)))
	expected := expressionBSON(t, document)
	digest := sha256.Sum256(expected)
	for _, realCodec := range []bool{false, true} {
		t.Run(fmt.Sprintf("actual_codec=%v", realCodec), func(t *testing.T) {
			cursor := bson.D{{Key: "id", Value: int64(0)}, {Key: "ns", Value: database + ".records"}, {Key: "firstBatch", Value: bson.A{document}}}
			responses := []bson.D{collectionQualificationResponse(database, "records"), readCursorResponse(cursor)}
			adapter := batchMockAdapter(t, responses, nil)
			memory := store.NewExecutionMemory()
			// This is the mutex-held denial's real trusted allowance, rather
			// than an idealized fraction of the container limit.
			sample := overload.Snapshot{RecoveryAllowance: 170_316_595, RecoveryAllowanceKnown: true}
			memory.ObserveMemory(sample)
			if snapshot := memory.Snapshot(); snapshot.BudgetBytes != 113_544_396 {
				t.Fatal("fixture changed the observed capacity", snapshot)
			}
			limits := store.DefaultLimits()
			runtime, err := store.New(adapter, limits, memory)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			resource := database + "/records/s:sdk-write-256-16777152"
			if realCodec {
				transportLimits := server.DefaultLimits()
				admission, err := server.NewAdmission(transportLimits)
				if err != nil {
					t.Fatal(err)
				}
				admission.SetExecutionMemory(memory)
				stores := map[string]*store.Runtime{"mongo": runtime}
				config := server.Config{Stores: stores, Admission: admission, Limits: transportLimits}
				front, err := server.New(config)
				if err != nil {
					t.Fatal(err)
				}
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				done := make(chan error, 1)
				go func() { done <- front.Serve(listener) }()
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := front.Shutdown(ctx); err != nil {
						t.Error(err)
					}
					_ = listener.Close()
					if err := <-done; err != nil {
						t.Error(err)
					}
				})
				connection, err := grpc.NewClient("passthrough:///"+listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(protocol.MaxTransportMessageBytes)))
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				client := pb.NewStoreServiceClient(connection)
				read := &weirclient.ReadRequest{Resource: resource}
				options := weirclient.ReadOneOptions{StoreName: "mongo", Request: read}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				result, err := weirclient.ReadOne(ctx, client, options)
				if err != nil || result == nil || result.Failure != nil || result.Document == nil || len(result.Document.Data) != documentBytes || sha256.Sum256(result.Document.Data) != digest {
					var failure *pb.Failure
					bytes := 0
					if result != nil {
						failure = result.Failure
						bytes = len(result.Document.GetData())
					}
					t.Fatal("real codec lost a legal complete read at the observed capacity", err, failure, bytes)
				}
				// Closing the real connection also retires transport ownership
				// when gRPC has finished delivery before native Free is observed.
				_ = connection.Close()
			} else {
				read := &pb.ReadRequest{Resource: resource}
				operation := &pb.Command_Read{Read: read}
				command := &pb.Command{Operation: operation}
				record, err := execution.NewRecord("mongo", 1, command)
				if err != nil {
					t.Fatal(err)
				}
				if record.Bytes() != 1247 {
					t.Fatal("fixture changed the observed read input size", record.Bytes())
				}
				work, failure := runtime.PrepareRecord(t.Context(), record)
				if failure != nil {
					t.Fatal(failure)
				}
				ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				defer ticket.Ack()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				event, err := ticket.Wait(ctx)
				raw := event.GetReadResult().GetDocument().GetData()
				if err != nil || len(raw) != documentBytes || sha256.Sum256(raw) != digest {
					t.Fatal("Runtime completion changed the read or its capacity decision", err, event.GetReadResult().GetFailure(), len(raw))
				}
				response := &pb.ExecuteResponse{Index: 1, Event: event}
				wire, err := ticket.ReserveWire(max(proto.Size(response), execution.MinimumWireBytes))
				if err != nil {
					t.Fatal("real event/envelope size exceeded its future wire reservation", err, memory.Snapshot())
				}
				defer wire.Release()
				ticket.Ack()
				if snapshot := memory.Snapshot(); snapshot.Count != 1 || snapshot.WireCount != 1 || snapshot.WireBytes != uint64(proto.Size(response)) || snapshot.UsedBytes < snapshot.WireBytes {
					t.Fatal("Ack erased the actual encoded buffer's lifetime", snapshot)
				}
				wire.Release()
			}
			deadline := time.NewTimer(time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				snapshot := memory.Snapshot()
				if snapshot.Count == 0 && snapshot.UsedBytes == 0 && snapshot.ResidentBytes == 0 && snapshot.InputUsedBytes == 0 && snapshot.InputReservedBytes == 0 && snapshot.WireBytes == 0 && snapshot.WireCount == 0 {
					break
				}
				select {
				case <-deadline.C:
					t.Fatal("completed maximum read retained input/result/wire ownership", snapshot)
				case <-tick.C:
				}
			}
		})
	}
}
