package search

import (
	"context"
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

func TestSearchMutationPlansSeparateResultsFromPreReadWorkingMemory(t *testing.T) {
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
				work := batchTestPlan(t, adapter, action, "records/s:record")
				if work.ResultBytes != execution.ResultOverheadBytes || work.WorkingBytes < 3*execution.BackendBatchBytes {
					t.Fatal("mutation declarations misplaced pre-read scratch", work.ResultBytes, work.WorkingBytes)
				}
				record, err := execution.NewRecord("search", 1, work.Command)
				if err != nil {
					t.Fatal(err)
				}
				prepared, failure := runtime.PrepareRecord(record)
				if commands.Load() != 0 {
					t.Fatal("preparing a mutation performed backend I/O")
				}
				if failure != nil || prepared == nil {
					t.Fatal(failure)
				}
				ticket, failure, _ := runtime.Submit(t.Context(), prepared, nil)
				if working.name == "insufficient" {
					if ticket != nil || failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT || commands.Load() != 0 {
						t.Fatal("pre-read scratch escaped Store working bound", ticket, failure, commands.Load())
					}
					if snapshot := runtime.Snapshot(); snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
						t.Fatal("impossible working envelope retained admission credits", snapshot)
					}
					return
				}
				if failure != nil {
					t.Fatal(failure)
				}
				if snapshot := runtime.Snapshot(); snapshot.ResultBytes != execution.ResultOverheadBytes {
					t.Fatal("mutation retained document-sized result credits", snapshot)
				}
				ticket.Abandon()
			})
		}
	}
}

func TestSearchRecordPreflightBeforeAnyCommand(t *testing.T) {
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
			mutation := &pb.MutateRequest{Resource: path, Action: action}
			operation := &pb.Command_Mutate{Mutate: mutation}
			command := &pb.Command{Operation: operation}
			record, err := execution.NewRecord("search", 1, command)
			if err != nil {
				t.Fatal(err)
			}
			prepared, failure := runtime.PrepareRecord(record)
			if failure == nil || prepared != nil || commands.Load() != 0 {
				t.Fatal("invalid backend input produced a plan or effects", prepared, failure, commands.Load())
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
