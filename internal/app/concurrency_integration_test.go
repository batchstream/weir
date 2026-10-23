//go:build integration

package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestMongoConcurrentRPCsUseAvailableBackendConnections(t *testing.T) {
	fixture := testmongo.Open(t)
	proxy := testmongo.StartProxy(t, fixture)
	observation := &budgetObservation{}
	proxy.Monitor = &event.CommandMonitor{Started: observation.start, Succeeded: observation.finish}
	cfg := recordsStoreConfig(t, proxy.URI())
	// A large execution budget isolates native connection growth.
	cfg.Stores[0].Batching.MaxInFlight = 1 << 16
	node := secureNode(t, cfg)
	concurrentBackendReads(t, node, fixture.DB+"/records/s:missing", observation)
}

func TestSearchConcurrentRPCsUseAvailableBackendConnections(t *testing.T) {
	fixture := testsearch.Open(t)
	endpoint, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(endpoint)
	observation := &budgetObservation{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if strings.HasSuffix(request.URL.Path, "/_mget") {
			observation.begin(request.Context())
			defer observation.end()
		}
		proxy.ServeHTTP(w, request)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	backend := &Search{URL: server.URL}
	// A large execution budget isolates native connection growth.
	batching := Batching{MaxOperations: 1, MaxInFlight: 1 << 16}
	definition := StoreConfig{Name: "records", Search: backend, Batching: batching}
	cfg := emptyConfig(t)
	cfg.Stores = []StoreConfig{definition}
	node := secureNode(t, cfg)
	concurrentBackendReads(t, node, fixture.Index+"/s:missing", observation)
}

func concurrentBackendReads(t *testing.T, node *Node, resource string, observation *budgetObservation) {
	t.Helper()
	const count = 64
	client := processClient(t, node.Address())
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	read := &pb.ReadRequest{Resource: resource}
	request := testutil.RecordRequest("records", read)
	// Qualify the target before holding commands on their actual connections.
	result, err := testutil.ExecuteRecord(ctx, client, request)
	if err != nil || result.GetReadResult().GetFailure() != nil {
		t.Fatal("warm read", result, err)
	}
	gate := make(chan struct{})
	defer func() {
		select {
		case <-gate:
		default:
			close(gate)
		}
	}()
	observation.hold(gate, 0)
	type reply struct {
		event *pb.Event
		err   error
	}
	replies := make(chan reply, count)
	for range count {
		go func() {
			event, err := testutil.ExecuteRecord(ctx, client, request)
			response := reply{event: event, err: err}
			replies <- response
		}()
	}
	for {
		active, _, _, _ := observation.snapshot()
		if active == count {
			break
		}
		select {
		case response := <-replies:
			t.Fatal("request finished before concurrent backend barrier", active, response)
		case <-ctx.Done():
			t.Fatal("backend connections did not grow with demand", active, node.stores["records"].Stats())
		case <-time.After(5 * time.Millisecond):
		}
	}
	stats := node.stores["records"].Stats()
	if stats.Active != count || stats.Queued != 0 || stats.QueuedBytes != 0 {
		t.Fatal("running requests retained queue capacity", stats)
	}
	close(gate)
	for range count {
		response := <-replies
		if response.err != nil || response.event.GetReadResult() == nil || response.event.GetReadResult().GetFailure() != nil {
			t.Fatal("concurrent read", response.event, response.err)
		}
	}
	budgetWait(t, "concurrent calls released", func() bool {
		stats := node.stores["records"].Stats()
		return stats.Active == 0 && stats.Queued == 0
	})
	t.Log("64 RPCs held on 64 real backend connections; all completed without admission rejection")
}
