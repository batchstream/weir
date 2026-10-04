//go:build integration

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"io"
	"strings"
	"testing"
	"time"

	spb "github.com/batchstream/weir-protocol/api/weir/search/v1"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
)

func TestSearchTLSApplicationAssemblyAllOperations(t *testing.T) {
	fixture := testsearch.OpenSecure(t)
	b := fixture.Backend
	connection := &SearchConnection{Username: b.Username, Password: b.Password, CAFile: b.CAFile}
	backend := &Search{URL: b.URL, Connection: connection}
	t.Run("partial-startup-cleanup", func(t *testing.T) {
		base := secureHTTPOpenCount(t, fixture.Admin)
		first := &Local{Search: backend}
		otherBackend := *backend
		otherConnection := *connection
		otherConnection.Password = "wrong-owned-pair"
		otherBackend.Connection = &otherConnection
		other := &Local{Search: &otherBackend}
		firstService := StoreConfig{Name: "first", Local: first}
		otherService := StoreConfig{Name: "second", Local: other}

		failed := DefaultConfig()
		failed.Basic.Listeners.Application = "127.0.0.1:0"
		failed.Routing.Stores = []StoreConfig{firstService, otherService}

		for i := 0; i < 3; i++ {
			node, err := Open(context.Background(), failed)
			if node != nil || err == nil {
				t.Fatal("partial startup should reject failed server authentication")
			}
		}
		for deadline := time.Now().Add(time.Second); ; {
			current := secureHTTPOpenCount(t, fixture.Admin)
			if current <= base {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("partial startup retained backend sockets", base, current)
			}
			time.Sleep(20 * time.Millisecond)
		}
		t.Log("three partial startup failures: native DB HTTP socket count returned to baseline")
	})
	local := &Local{Search: backend}
	service := StoreConfig{Name: "search", Local: local}

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
			searchClientOperations(t, client, fixture, name)
		})
	}
}

func searchClientOperations(t *testing.T, client pb.StoreServiceClient, fixture *testsearch.SecureFixture, name string) {
	t.Helper()
	b := fixture.Backend
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	root := b.Index
	doc := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":9007199254740993,"keep":"opaque"}`)}
	put := &pb.MutateRequest_Put{Put: doc}
	mutation := &pb.MutateRequest{Resource: root + "/s:" + name, Action: put}
	recordResult, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	var result *pb.MutationResult
	if recordResult != nil {
		result = recordResult.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("Put", result, err)
	}
	read := &pb.ReadRequest{Resource: mutation.Resource}
	recordResult2, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", read))
	var found *pb.ReadResult
	if recordResult2 != nil {
		found = recordResult2.Read
	}
	if err != nil || !bytes.Equal(found.GetDocument().GetData(), doc.Data) {
		failure := found.GetFailure()
		t.Fatalf(
			"opaque JSON changed: transport=%v failure_code=%s failure_message=%q missing=%t got_bytes=%d want_bytes=%d",
			err,
			failure.GetCode(),
			failure.GetMessage(),
			found.GetMissing() != nil,
			len(found.GetDocument().GetData()),
			len(doc.Data),
		)
	}
	create := &pb.MutateRequest_Create{Create: doc}
	mutation.Action = create
	var recordResult3 *execution.Result
	recordResult3, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	result = nil
	if recordResult3 != nil {
		result = recordResult3.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED {
		t.Fatal("duplicate create", result, err)
	}
	empty := &pb.Empty{}
	remove := &pb.MutateRequest_Delete{Delete: empty}
	mutation.Action = remove
	var recordResult4 *execution.Result
	recordResult4, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	result = nil
	if recordResult4 != nil {
		result = recordResult4.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("delete", result, err)
	}
	var recordResult5 *execution.Result
	recordResult5, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", read))
	found = nil
	if recordResult5 != nil {
		found = recordResult5.Read
	}
	if err != nil || found.GetMissing() == nil {
		t.Fatal("missing", found, err)
	}
	mutation.Action = create
	var recordResult6 *execution.Result
	recordResult6, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	result = nil
	if recordResult6 != nil {
		result = recordResult6.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("create", result, err)
	}
	replace := &pb.MutateRequest_Replace{Replace: doc}
	mutation.Action = replace
	var recordResult7 *execution.Result
	recordResult7, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	result = nil
	if recordResult7 != nil {
		result = recordResult7.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("replace", result, err)
	}
	expression := &pb.Document{MediaType: search.ExpressionMedia, Data: []byte(`{"doc":{"n":9007199254740995}}`)}
	form := &pb.Transform_BackendExpression{BackendExpression: expression}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation.Action = action
	var recordResult8 *execution.Result
	recordResult8, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", mutation))
	result = nil
	if recordResult8 != nil {
		result = recordResult8.Mutation
	}
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("expression", result, err)
	}
	recordFixture := testutil.RecordRequest("search", mutation)
	batch := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{recordFixture.Operation.Mutate}}
	reply, err := client.Mutate(ctx, batch)
	if err != nil || len(reply.GetResults()) != 1 || reply.Results[0].GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("batch mutation", reply, err)
	}
	var recordResult9 *execution.Result
	recordResult9, err = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("search", read))
	found = nil
	if recordResult9 != nil {
		found = recordResult9.Read
	}
	if err != nil || !strings.Contains(string(found.GetDocument().GetData()), "9007199254740995") {
		t.Fatal("expression int64", err)
	}
	fixture.Admin.Do(t, "POST", "/"+b.Index+"/_refresh", "")
	scanRequest := &pb.ScanRequest{Resource: root}
	scanVariant := &pb.Command_Scan{Scan: scanRequest}
	scanCall := &pb.Command{Operation: scanVariant}
	scan, err := testutil.ExecuteEvents(ctx, client, "search", scanCall)
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
				t.Fatal("Scan End", end)
			}
			break
		}
		count++
	}
	if _, err := scan.Recv(); err != io.EOF {
		t.Fatal("Scan EOF", err)
	}
	descriptor := &spb.Request{Method: "POST", Path: "/_bulk"}
	encoded, err := proto.Marshal(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: search.NativeDescriptor, Data: encoded}
	nativeOpen := &pb.NativeOpen{Resource: root, Descriptor_: document, BodyMediaType: "application/x-ndjson"}
	body := []byte("{\"index\":{\"_id\":\"native-" + name + "\"}}\n{\"n\":9007199254740993}\n")
	nativeCall := &pb.NativeRequest{Open: nativeOpen, Body: body}
	nativeVariant := &pb.Command_Native{Native: nativeCall}
	call := &pb.Command{Operation: nativeVariant}
	native, err := testutil.ExecuteEvents(ctx, client, "search", call)
	if err != nil {
		t.Fatal(err)
	}
	var response []byte
	for {
		frame, err := native.Recv()
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
	if _, err := native.Recv(); err != io.EOF {
		t.Fatal("Native EOF", err)
	}
	if !strings.Contains(string(response), `"errors":false`) {
		t.Fatal("Native operation failed")
	}
	t.Log("Read CRUD conflict/missing batch mutation Scan End/EOF Native bulk End/EOF BackendExpression; exact int64 preserved")
}

func secureHTTPOpenCount(t *testing.T, b *testsearch.Backend) int {
	t.Helper()
	status, raw := b.Do(t, "GET", "/_nodes/stats/http", "")
	var stats struct {
		Nodes map[string]struct {
			HTTP struct {
				CurrentOpen int `json:"current_open"`
			}
		}
	}
	if status != 200 || json.Unmarshal(raw, &stats) != nil || len(stats.Nodes) != 1 {
		t.Fatal("independent native HTTP socket observation unavailable", status)
	}
	for _, node := range stats.Nodes {
		return node.HTTP.CurrentOpen
	}
	return 0
}
