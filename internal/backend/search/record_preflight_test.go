package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
)

func TestSearchMutationWindowsSeparateResultsFromPreReadWorkingMemory(t *testing.T) {
	type workingCase struct {
		name  string
		bytes int
	}
	workingCases := []workingCase{{name: "sufficient", bytes: 3 * execution.BackendBatchBytes}, {name: "insufficient", bytes: 24 << 20}}
	for _, action := range []string{"replace", "program"} {
		for _, working := range workingCases {
			t.Run(action+"/"+working.name, func(t *testing.T) {
				var commands atomic.Int32
				handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
					commands.Add(1)
					<-request.Context().Done()
				})
				server := httptest.NewServer(handler)
				t.Cleanup(server.Close)
				config := Config{Store: "search", URL: server.URL, MaxReadSize: protocol.MaxDocument}
				lifetime, cancel := context.WithCancel(t.Context())
				client := server.Client()
				transport := client.Transport.(*http.Transport)
				adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: client, transport: transport, ctx: lifetime, cancel: cancel}
				limits := store.DefaultLimits()
				limits.WorkingBytes = working.bytes
				runtime, err := store.New(adapter, limits)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() {
					ctx, stop := context.WithTimeout(context.Background(), time.Second)
					defer stop()
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				})
				requests := make([]*pb.MutateRequest, 32)
				for index := range requests {
					work := batchTestPlan(t, adapter, action, fmt.Sprintf("records/s:%d", index))
					if work.ResultBytes != execution.ResultOverheadBytes || work.WorkingBytes < 3*execution.BackendBatchBytes {
						t.Fatal("mutation declarations misplaced pre-read scratch", work.ResultBytes, work.WorkingBytes)
					}
					requests[index] = work.Operation.Mutate
				}
				batch := &pb.MutationBatch{Requests: requests}
				operation := &pb.Command_Mutate{Mutate: batch}
				command := &pb.Command{Operation: operation}
				prepared, count, failure := runtime.PrepareWindow("search", command, 0)
				if commands.Load() != 0 {
					t.Fatal("preparing a mutation window performed backend I/O")
				}
				if working.name == "insufficient" {
					if prepared != nil || count != 0 || failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
						t.Fatal("pre-read scratch escaped Store working bound", prepared, count, failure)
					}
					return
				}
				if failure != nil || prepared == nil || count != len(requests) {
					t.Fatal("mutation pre-read consumed terminal result window credits", count, failure)
				}
				ticket, failure, _ := runtime.SubmitBatch(t.Context(), prepared)
				if failure != nil {
					t.Fatal(failure)
				}
				if snapshot := runtime.Snapshot(); snapshot.ResultBytes != len(requests)*execution.ResultOverheadBytes {
					t.Fatal("mutation window retained document-sized result credits", snapshot)
				}
				ticket.Abandon()
			})
		}
	}
}

func TestSearchCompleteBatchPreflightBeforeAnyCommand(t *testing.T) {
	for _, path := range []string{"records/i:1", "_records/s:bad", "Records/s:bad", "records"} {
		t.Run(path, func(t *testing.T) {
			var commands atomic.Int32
			handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { commands.Add(1) })
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{Store: "search", URL: server.URL}
			lifetime, cancel := context.WithCancel(context.Background())
			client := server.Client()
			transport := client.Transport.(*http.Transport)
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: client, transport: transport, ctx: lifetime, cancel: cancel}
			limits := store.DefaultLimits()
			runtime, err := store.New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			empty := &pb.Empty{}
			action := &pb.MutateRequest_Delete{Delete: empty}
			first := &pb.MutateRequest{Resource: "records/s:good", Action: action}
			last := &pb.MutateRequest{Resource: path, Action: action}
			request := &pb.MutationBatch{Requests: []*pb.MutateRequest{first, last}}
			records, failure := execution.NewMutationRecords("search", request.Requests, runtime.PendingByteLimit())
			if failure != nil {
				t.Fatal("backend-specific fixture failed common validation", failure)
			}
			prepared, failure := runtime.PrepareBatch(records)
			if failure == nil || prepared != nil || commands.Load() != 0 {
				t.Fatal("late invalid backend input produced a prepared batch or effects", prepared, failure, commands.Load())
			}
			if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.WorkingBytes != 0 {
				t.Fatal("failed preflight reserved runtime resources", snapshot)
			}
		})
	}
}

func TestSearchIndexBytePolicy(t *testing.T) {
	for _, name := range []string{"a", "z_a09-", strings.Repeat("a", 63)} {
		if !validIndex(name) {
			t.Fatal("valid index rejected", name)
		}
	}
	for _, name := range []string{"", "_a", "1a", "A", "a.b", "a/", "a\n", "aé", strings.Repeat("a", 64)} {
		if validIndex(name) {
			t.Fatal("invalid index accepted", name)
		}
	}
}
