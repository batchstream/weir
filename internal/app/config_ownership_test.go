package app

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestProcessLuaAndStreamPolicyReachEveryStore(t *testing.T) {
	var reads atomic.Int32
	var blocking atomic.Bool
	gate := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = w.Write([]byte(`{"version":{"build_flavor":"default"}}`))
		case "/_cluster/settings":
			_, _ = w.Write([]byte(`{"persistent":{"action.auto_create_index":"false"}}`))
		case "/records":
			_, _ = w.Write([]byte(`{"records":{"settings":{"index.uuid":"owned","index.number_of_shards":"1"},"mappings":{}}}`))
		case "/records/_mget":
			reads.Add(1)
			if blocking.Load() {
				select {
				case <-gate:
				case <-request.Context().Done():
					return
				}
			}
			var data struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(request.Body).Decode(&data); err != nil {
				t.Error(err)
				return
			}
			docs := make([]map[string]any, len(data.IDs))
			for i, id := range data.IDs {
				docs[i] = map[string]any{"_index": "records", "_id": id, "found": false}
			}
			reply := map[string]any{"docs": docs}
			if err := json.NewEncoder(w).Encode(reply); err != nil {
				t.Error(err)
			}
		default:
			t.Errorf("unexpected backend request %s", request.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	})
	endpoint := httptest.NewServer(handler)
	defer endpoint.Close()
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Basic.Transport.MaxPendingRecords = 3
	cfg.Basic.Transport.Keepalive.Interval = Duration(2 * time.Minute)
	cfg.Basic.Transport.Timeouts.Stall = Duration(time.Second)
	cfg.Basic.Lua.Values.MaxNodes = 10000
	cfg.Basic.Lua.VM.MaxInstructions = 50000
	for _, name := range []string{"first", "second"} {
		adapter := &Search{URL: endpoint.URL}
		backend := BackendConfig{Search: adapter}
		batching := BatchingConfig{MaxOperations: 1}
		local := &Local{Backend: backend, Batching: batching}
		definition := StoreConfig{Name: name, Local: local}
		cfg.Routing.Stores = append(cfg.Routing.Stores, definition)
	}
	node, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = node.Close(context.Background()) }()
	if err := node.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(node.Addresses()[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewStoreServiceClient(conn)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	incoming := map[string]any{"items": make([]int, 5000)}
	inputBytes, err := json.Marshal(incoming)
	if err != nil {
		t.Fatal(err)
	}
	input := &pb.Document{ContentType: "application/json", Data: inputBytes}
	for _, name := range []string{"first", "second"} {
		for _, source := range []string{`return function(current, incoming) return weir.keep() end`, `return function(current, incoming) local n=0; for i=1,1000000 do n=n+i end; return weir.keep() end`} {
			lua := &pb.LuaTransform{Source: []byte(source), Input: input}
			form := &pb.Transform_Lua{Lua: lua}
			transform := &pb.Transform{Form: form}
			action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
			mutation := &pb.MutateRequest{Resource: "records/s:item", Action: action}
			request := testutil.RecordRequest(name, mutation)
			event, err := testutil.ExecuteRecord(ctx, client, request)
			if err != nil {
				t.Fatal(name, err)
			}
			result := event.GetMutationResult()
			if strings.Contains(source, "for i") {
				if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
					t.Fatal("global VM policy missing", name, result)
				}
			} else if result.GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal("global value policy missing", name, result)
			}
		}
	}
	baseline := reads.Load()
	blocking.Store(true)
	streams := make([]grpc.BidiStreamingClient[pb.ExecuteRequest, pb.ExecuteResponse], 0, 2)
	for _, name := range []string{"first", "second"} {
		stream, err := client.Execute(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for index := uint64(1); index <= 10; index++ {
			read := &pb.ReadRequest{Resource: fmt.Sprint("records/s:item", index)}
			fixture := testutil.RecordRequest(name, read)
			request := &pb.ExecuteRequest{StoreName: name, Index: index, Command: fixture.Command}
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
		}
		if err := stream.CloseSend(); err != nil {
			t.Fatal(err)
		}
		streams = append(streams, stream)
	}
	for reads.Load()-baseline < 6 {
		select {
		case <-ctx.Done():
			t.Fatal("global stream windows did not fill", reads.Load()-baseline)
		case <-time.After(time.Millisecond):
		}
	}
	time.Sleep(20 * time.Millisecond)
	for _, name := range []string{"first", "second"} {
		snapshot := node.stores[name].Snapshot()
		if snapshot.Retained != 3 || snapshot.Active != 3 {
			t.Fatal("Store did not inherit process flow control", name, snapshot)
		}
	}
	if reads.Load()-baseline != 6 {
		t.Fatal("streams exceeded configured windows", reads.Load()-baseline)
	}
	close(gate)
	for _, stream := range streams {
		count := 0
		for {
			response, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil || response.GetEvent().GetReadResult().GetFailure() != nil {
				t.Fatal(response, err)
			}
			count++
		}
		if count != 10 {
			t.Fatal("lost responses", count)
		}
	}
}
