//go:build integration

package app

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func secureNode(t *testing.T, cfg Config) *Node {
	t.Helper()
	filename := filepath.Join(t.TempDir(), "node.json")
	writeConfigFiles(t, filename, cfg, 0600)
	cfg, err := Load(filename)
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
	mongo := &Mongo{URI: fixture.URI, Database: fixture.DB, Collection: "records"}
	local := &Local{MongoDB: mongo}
	service := Service{Name: "database", Local: local}
	route := Route{Store: "mongo", Service: "database"}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Basic.Listeners.Peer = "127.0.0.1:0"
	cfg.Routing.Services = []Service{service}
	cfg.Routing.Routes = []Route{route}
	node := secureNode(t, cfg)
	remote := &Remote{Endpoints: []string{node.Addresses()[1]}, MaxConcurrency: 1}
	service = Service{Name: "database", Remote: remote}
	cfg.Routing.Services = []Service{service}
	cfg.Basic.Listeners.Peer = ""
	peer := secureNode(t, cfg)
	for name, address := range map[string]string{"direct": node.Addresses()[0], "peer": peer.Addresses()[0]} {
		t.Run(name, func(t *testing.T) {
			conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			client := pb.NewWeirClient(conn)
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
			result, err := client.Mutate(ctx, request)
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			read := &pb.ReadRequest{Resource: request.Resource}
			found, err := client.Read(ctx, read)
			if err != nil || !bytes.Equal(found.GetDocument().GetData(), raw) {
				t.Fatal("opaque BSON changed", err)
			}
			create := &pb.MutateRequest_Create{Create: doc}
			request.Action = create
			result, err = client.Mutate(ctx, request)
			if err != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
				t.Fatal("duplicate not definite", result, err)
			}
			empty := &pb.Empty{}
			remove := &pb.MutateRequest_Delete{Delete: empty}
			request.Action = remove
			result, err = client.Mutate(ctx, request)
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			found, err = client.Read(ctx, read)
			if err != nil || found.GetMissing() == nil {
				t.Fatal("missing contract", err)
			}
			request.Action = create
			result, err = client.Mutate(ctx, request)
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			replace := &pb.MutateRequest_Replace{Replace: doc}
			request.Action = replace
			result, err = client.Mutate(ctx, request)
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
			result, err = client.Mutate(ctx, request)
			if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result, err)
			}
			bulk, err := client.Bulk(ctx)
			if err != nil {
				t.Fatal(err)
			}
			open := &pb.BulkOpen{Store: "weir://mongo"}
			opening := &pb.BulkRequestFrame_Open{Open: open}
			frame := &pb.BulkRequestFrame{Frame: opening}
			if err := bulk.Send(frame); err != nil {
				t.Fatal(err)
			}
			mutation := &pb.BulkOperation_Mutate{Mutate: request}
			operation := &pb.BulkOperation{Operation: mutation}
			item := &pb.BulkRequestFrame_Operation{Operation: operation}
			frame = &pb.BulkRequestFrame{Frame: item}
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
			reply, err = bulk.Recv()
			if err != nil || reply.GetEnd().GetReceivedCount() != 1 || reply.GetEnd().GetResultCount() != 1 {
				t.Fatal(reply, err)
			}
			if _, err := bulk.Recv(); err != io.EOF {
				t.Fatal(err)
			}
			found, err = client.Read(ctx, read)
			if err != nil || bson.Raw(found.GetDocument().GetData()).Lookup("n").AsInt64() != 9007199254740995 {
				t.Fatal("expression replay/type change", err)
			}
			scanRequest := &pb.ScanRequest{Resource: root, FetchItemsHint: 1}
			scan, err := client.Scan(ctx, scanRequest)
			if err != nil {
				t.Fatal(err)
			}
			count := uint64(0)
			for {
				frame, err := scan.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if end := frame.GetEnd(); end != nil {
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
			stream, err := client.Native(ctx)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
			nativeOpen := &pb.NativeOpen{Resource: root, Descriptor_: descriptor, BodyMediaType: "application/bson"}
			nativeVariant := &pb.NativeRequestFrame_Open{Open: nativeOpen}
			nativeFrame := &pb.NativeRequestFrame{Frame: nativeVariant}
			if err := stream.Send(nativeFrame); err != nil {
				t.Fatal(err)
			}
			command := bson.D{{Key: "count", Value: "records"}}
			body, _ := bson.Marshal(command)
			chunk := &pb.NativeRequestFrame_Chunk{Chunk: body}
			nativeFrame = &pb.NativeRequestFrame{Frame: chunk}
			if err := stream.Send(nativeFrame); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			var response []byte
			for {
				frame, err := stream.Recv()
				if err != nil {
					t.Fatal(err)
				}
				response = append(response, frame.GetChunk()...)
				if end := frame.GetEnd(); end != nil {
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
			t.Log("production basic/routing JSON Load/Open: CRUD, conflict/missing, opaque int64, expression, Bulk End/EOF, Scan End/EOF, Native End/EOF passed")
		})
	}
}
