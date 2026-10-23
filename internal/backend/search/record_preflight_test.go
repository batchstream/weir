package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
)

func TestSearchMutationPreparationPerformsNoBackendIO(t *testing.T) {
	for _, action := range []string{"replace", "program"} {
		t.Run(action, func(t *testing.T) {
			var commands atomic.Int32
			handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
				commands.Add(1)
				<-request.Context().Done()
			})
			server := httptest.NewServer(handler)
			t.Cleanup(server.Close)
			config := Config{Store: "search", URL: server.URL}
			lifetime, cancel := context.WithCancel(t.Context())
			t.Cleanup(cancel)
			client := server.Client()
			transport := client.Transport.(*http.Transport)
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: client, transport: transport, ctx: lifetime, cancel: cancel}
			work := batchTestPlan(t, adapter, action, "records/s:record")
			record, err := execution.NewRecord("search", 1, work.Command)
			if err != nil {
				t.Fatal(err)
			}
			prepared, failure := adapter.PrepareRecord(context.Background(), record)
			if failure != nil || prepared == nil {
				t.Fatal(failure)
			}
			if commands.Load() != 0 {
				t.Fatal("preparing a mutation performed backend I/O")
			}
		})
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
			prepared, failure := runtime.PrepareRecord(context.Background(), record)
			if failure == nil || prepared != nil || commands.Load() != 0 {
				t.Fatal("invalid backend input produced a plan or effects", prepared, failure, commands.Load())
			}
		})
	}
}

func TestSearchIndexBytePolicy(t *testing.T) {
	for _, name := range []string{"a", "z_a09-", "1a", "logs.2026", "中文", "aé", "percent%2f", strings.Repeat("a", 255)} {
		if !validIndex(name) {
			t.Fatal("valid index rejected", name)
		}
	}
	for _, name := range []string{"", "_a", "-a", "a+b", "A", ".", "..", "a/", "a,b", "a*", "a\n", strings.Repeat("a", 256)} {
		if validIndex(name) {
			t.Fatal("invalid index accepted", name)
		}
	}
}
