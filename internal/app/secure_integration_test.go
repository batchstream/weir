//go:build integration

package app

import (
	"bytes"
	"context"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func secureNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "node.yaml")
	routingFilename := writeConfigFiles(t, filename, cfg, 0600)
	cfg, err := Load(filename, routingFilename)
	if err != nil {
		t.Fatal("owned config load failed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	node, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := node.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	node.Start(context.Background())
	return node
}

func TestMongoTLSApplicationAssemblyAllOperations(t *testing.T) {
	if os.Getenv("WEIR_M10_INTEGRATION") != "1" {
		t.Skip("secure profile opt-in")
	}
	fixture := testmongo.OpenSecure(t)
	mongo := mongoFixtureConfig(t, fixture.URI)
	valueDirectory := t.TempDir()
	mongo.UsernameFile = filepath.Join(valueDirectory, "username")
	mongo.PasswordFile = filepath.Join(valueDirectory, "password")
	if err := os.WriteFile(mongo.UsernameFile, []byte(mongo.Username+"\n"), 0600); err != nil {
		t.Fatal("cannot write owned username value")
	}
	if err := os.WriteFile(mongo.PasswordFile, []byte(mongo.Password+"\r\n"), 0600); err != nil {
		t.Fatal("cannot write owned password value")
	}
	mongo.Username, mongo.Password = "", ""

	local := &Local{MongoDB: mongo}
	service := StoreConfig{Name: "mongo", Local: local}

	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{service}

	node := secureNode(t, cfg)
	for name, address := range map[string]string{"direct": node.Addresses()[0]} {
		t.Run(name, func(t *testing.T) {
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := pb.NewStoreServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			root := "weir://mongo/" + fixture.DB + "/records"
			document := bson.D{{Key: "_id", Value: name}, {Key: "n", Value: int64(9007199254740993)}}
			raw, err := bson.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			doc := &pb.Document{MediaType: "application/bson", Data: raw}
			put := &pb.MutateRequest_Put{Put: doc}
			request := &pb.MutateRequest{Resource: root + "/s:" + name, Action: put}
			routedResult97, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result := routedResult97.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			read := &pb.ReadRequest{Resource: request.Resource}
			routedResult102, err := testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(read))
			found := routedResult102.GetRead()
			if err != nil || !bytes.Equal(found.GetDocument().GetData(), raw) {
				t.Fatal("opaque BSON changed", err)
			}
			create := &pb.MutateRequest_Create{Create: doc}
			request.Action = create
			var routedResult108 *pb.Result
			routedResult108, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result = routedResult108.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
				t.Fatal("duplicate not definite", result, err)
			}
			empty := &pb.Empty{}
			remove := &pb.MutateRequest_Delete{Delete: empty}
			request.Action = remove
			var routedResult115 *pb.Result
			routedResult115, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result = routedResult115.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			var routedResult119 *pb.Result
			routedResult119, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(read))
			found = routedResult119.GetRead()
			if err != nil || found.GetMissing() == nil {
				t.Fatal("missing contract", err)
			}
			request.Action = create
			var routedResult124 *pb.Result
			routedResult124, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result = routedResult124.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			replace := &pb.MutateRequest_Replace{Replace: doc}
			request.Action = replace
			var routedResult130 *pb.Result
			routedResult130, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result = routedResult130.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
			expression, _ := bson.Marshal(increment)
			expressionDoc := &pb.Document{MediaType: mongodb.ExpressionMedia, Data: expression}
			form := &pb.Transform_BackendExpression{BackendExpression: expressionDoc}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			request.Action = action
			var routedResult141 *pb.Result
			routedResult141, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(request))
			result = routedResult141.GetMutation()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			bulk := testutil.OpenEvents(ctx, client, "mongo")
			var frame *pb.Command
			mutation := &pb.Operation_Mutate{Mutate: request}
			operation := &pb.Operation{Operation: mutation}
			_, item := testutil.OperationCommand(operation)
			frame = item
			if err := bulk.Send(frame); err != nil {
				t.Fatal(err)
			}
			if err := bulk.CloseSend(); err != nil {
				t.Fatal(err)
			}
			reply, err := bulk.Recv()
			if err != nil || reply.GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(reply, err)
			}
			if _, err := bulk.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			var routedResult176 *pb.Result
			routedResult176, err = testutil.ExecuteRecord(ctx, client, testutil.RecordCommand(read))
			found = routedResult176.GetRead()
			if err != nil || bson.Raw(found.GetDocument().GetData()).Lookup("n").AsInt64() != 9007199254740995 {
				t.Fatal("expression replay/type change", err)
			}
			scanRequest := &pb.ScanRequest{Resource: root}
			scanVariant := &pb.Command_Scan{Scan: scanRequest}
			scanCall := &pb.Command{Version: 1, Operation: scanVariant}
			scan, err := testutil.OneEvents(ctx, client, scanCall)
			if err != nil {
				t.Fatal(err)
			}
			count := uint64(0)
			for {
				frame, err := scan.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetScanEnd(); end != nil {
					if end.Failure != nil || end.DocumentCount != count || count < 1 {
						t.Fatal(end)
					}
					break
				}
				count++
			}
			if _, err := scan.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
			nativeOpen := &pb.NativeOpen{Resource: root, Descriptor_: descriptor, BodyMediaType: "application/bson"}
			command := bson.D{{Key: "count", Value: "records"}}
			body, _ := bson.Marshal(command)
			nativeCall := &pb.NativeRequest{Open: nativeOpen, Body: body}
			nativeVariant := &pb.Command_Native{Native: nativeCall}
			call := &pb.Command{Version: 1, Operation: nativeVariant}
			stream, err := testutil.OneEvents(ctx, client, call)
			if err != nil {
				t.Fatal(err)
			}
			var response []byte
			for {
				frame, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				response = append(response, frame.GetChunk()...)
				if end := frame.GetNativeEnd(); end != nil {
					if end.Failure != nil || end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
						t.Fatal(end)
					}
					break
				}
			}
			if _, err := stream.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			if bson.Raw(response).Lookup("ok").AsInt64() != 1 {
				t.Fatal("Native lost response")
			}
			t.Log("production basic/routing YAML Load/Open: CRUD, conflict/missing, opaque int64, expression, Bulk End/EOF, Scan End/EOF, Native End/EOF passed")
		})
	}
}
