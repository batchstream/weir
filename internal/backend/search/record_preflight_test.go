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
			request := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{first, last}}
			records, failure := execution.NewMutationRecords(request, runtime.PendingByteLimit())
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
