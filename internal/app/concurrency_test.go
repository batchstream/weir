package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConcurrentRPCExecutionPolicy(t *testing.T) {
	tests := []struct {
		name        string
		maxInFlight int
		wantActive  int
	}{
		{name: "default", wantActive: 16},
		{name: "explicit", maxInFlight: 64, wantActive: 64},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const count = 64
			gate := make(chan struct{})
			defer func() {
				select {
				case <-gate:
				default:
					close(gate)
				}
			}()
			started := make(chan struct{}, count)
			var mu sync.Mutex
			active, peak := 0, 0
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				switch request.URL.Path {
				case "/":
					_, _ = w.Write([]byte(`{"version":{"build_flavor":"default"}}`))
				case "/records":
					_, _ = w.Write([]byte(`{"records":{"settings":{"index.uuid":"owned","index.number_of_shards":"1"},"mappings":{}}}`))
				case "/records/_mget":
					mu.Lock()
					active++
					peak = max(peak, active)
					mu.Unlock()
					defer func() {
						mu.Lock()
						active--
						mu.Unlock()
					}()
					started <- struct{}{}
					select {
					case <-gate:
					case <-request.Context().Done():
						return
					}
					var input struct {
						IDs []string `json:"ids"`
					}
					if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
						t.Error(err)
						return
					}
					docs := make([]map[string]any, len(input.IDs))
					for i, id := range input.IDs {
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
			t.Cleanup(endpoint.Close)
			definition := searchFixtureStore("records", endpoint.URL)
			definition.Batching = Batching{MaxOperations: 1, MaxInFlight: test.maxInFlight}
			cfg := emptyConfig(t)
			cfg.Stores = []StoreConfig{definition}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			node, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := node.Close(context.Background()); err != nil {
					t.Error(err)
				}
			})
			if err := node.Start(ctx); err != nil {
				t.Fatal(err)
			}
			connection, err := grpc.NewClient("passthrough:///"+node.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = connection.Close() })
			client := pb.NewStoreServiceClient(connection)
			type reply struct {
				event *pb.Event
				err   error
			}
			replies := make(chan reply, count)
			for range count {
				go func() {
					read := &pb.ReadRequest{Resource: "records/s:missing"}
					fixture := testutil.RecordRequest("records", read)
					event, err := testutil.ExecuteRecord(ctx, client, fixture)
					result := reply{event: event, err: err}
					replies <- result
				}()
			}
			for range test.wantActive {
				select {
				case <-started:
				case result := <-replies:
					t.Fatal("request finished before the backend gate opened", result.event, result.err)
				case <-ctx.Done():
					t.Fatal("configured execution concurrency was not reached")
				}
			}
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for {
				stats := node.stores["records"].Stats()
				if stats.Active > test.wantActive {
					t.Fatal("execution concurrency exceeded configured policy", stats)
				}
				if stats.Active == test.wantActive && stats.Queued == count-test.wantActive {
					break
				}
				select {
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal("requests did not fill the configured execution and queue boundary", stats)
				}
			}
			mu.Lock()
			observedActive, observedPeak := active, peak
			mu.Unlock()
			if observedActive != test.wantActive || observedPeak != test.wantActive {
				t.Fatal("backend concurrency differs from Store policy", observedActive, observedPeak)
			}
			close(gate)
			for range count {
				select {
				case result := <-replies:
					read := result.event.GetReadResult()
					if result.err != nil || read == nil || read.GetFailure() != nil || read.GetMissing() == nil {
						t.Fatal("concurrent read failed", result.event, result.err)
					}
				case <-ctx.Done():
					t.Fatal("queued reads failed to drain after backend release")
				}
			}
			// The backend handler decrements active after its reply is written.
			for {
				mu.Lock()
				idle := active == 0
				mu.Unlock()
				stats := node.stores["records"].Stats()
				if idle && stats.Active == 0 && stats.Queued == 0 {
					break
				}
				select {
				case <-tick.C:
				case <-ctx.Done():
					t.Fatal("completed reads retained backend or Store execution", stats)
				}
			}
			mu.Lock()
			observedActive, observedPeak = active, peak
			mu.Unlock()
			if observedActive != 0 || observedPeak != test.wantActive {
				t.Fatal("backend requests leaked or exceeded execution policy during drain", observedActive, observedPeak)
			}
			t.Logf("64 independent RPCs: %d backend requests active, %d queued; all completed after release", test.wantActive, count-test.wantActive)
		})
	}
}
