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
	if os.Getenv("WEIR_MONGO_SECURE_INTEGRATION") != "1" {
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

	local := &Local{Backend: BackendConfig{MongoDB: mongo}}
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
			root := fixture.DB + "/records"
			document := bson.D{{Key: "_id", Value: name}, {Key: "n", Value: int64(9007199254740993)}}
			raw, err := bson.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			doc := &pb.Document{ContentType: "application/bson", Data: raw}
			put := &pb.MutateRequest_Put{Put: doc}
			request := &pb.MutateRequest{Resource: root + "/s:" + name, Action: put}
			recordResult, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result := recordResult.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			read := &pb.ReadRequest{Resource: request.Resource}
			recordResult2, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", read))
			found := recordResult2.GetReadResult()
			if err != nil || !bytes.Equal(found.GetDocument().GetData(), raw) {
				t.Fatal("opaque BSON changed", err)
			}
			create := &pb.MutateRequest_Create{Create: doc}
			request.Action = create
			recordResult3, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result = recordResult3.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
				t.Fatal("duplicate not definite", result, err)
			}
			empty := &pb.Empty{}
			remove := &pb.MutateRequest_Delete{Delete: empty}
			request.Action = remove
			recordResult4, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result = recordResult4.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			recordResult5, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", read))
			found = recordResult5.GetReadResult()
			if err != nil || found.GetMissing() == nil {
				t.Fatal("missing contract", err)
			}
			request.Action = create
			recordResult6, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result = recordResult6.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			replace := &pb.MutateRequest_Replace{Replace: doc}
			request.Action = replace
			recordResult7, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result = recordResult7.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: int64(1)}}}}
			expression, _ := bson.Marshal(increment)
			expressionDoc := &pb.Document{ContentType: mongodb.ExpressionContentType, Data: expression}
			form := &pb.Transform_BackendExpression{BackendExpression: expressionDoc}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			request.Action = action
			recordResult8, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", request))
			result = recordResult8.GetMutationResult()
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			batch := []*pb.MutateRequest{request}
			reply, err := testutil.MutateRecords(ctx, client, "mongo", batch)
			if err != nil || len(reply) != 1 || reply[0].GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("batch mutation", reply, err)
			}
			recordResult9, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("mongo", read))
			found = recordResult9.GetReadResult()
			if err != nil || bson.Raw(found.GetDocument().GetData()).Lookup("n").AsInt64() != 9007199254740995 {
				t.Fatal("expression replay/type change", err)
			}
			scanRequest := &pb.ScanRequest{Resource: root}
			scanVariant := &pb.Command_Scan{Scan: scanRequest}
			scanCall := &pb.Command{Operation: scanVariant}
			scan, err := testutil.ExecuteEvents(ctx, client, "mongo", scanCall)
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
			command := bson.D{{Key: "count", Value: "records"}}
			body, _ := bson.Marshal(command)
			nativeRequest := &pb.Document{ContentType: "application/bson", Data: body}
			nativeCall := &pb.NativeRequest{Resource: root, Request: nativeRequest}
			nativeVariant := &pb.Command_Native{Native: nativeCall}
			call := &pb.Command{Operation: nativeVariant}
			stream, err := testutil.ExecuteEvents(ctx, client, "mongo", call)
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
			t.Log("production basic/routing YAML Load/Open: CRUD, conflict/missing, opaque int64, expression, batch mutation, Scan End/EOF, Native End/EOF passed")
		})
	}
}
