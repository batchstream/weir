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
	cfg := packagedConfig(t, proxy.URI())
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
	local := &Local{Search: backend, MaxBatchOperations: 1}
	definition := StoreConfig{Name: "records", Local: local}
	cfg := DefaultConfig()
	cfg.Basic.Listeners.Application = "127.0.0.1:0"
	cfg.Routing.Stores = []StoreConfig{definition}
	node := secureNode(t, cfg)
	concurrentBackendReads(t, node, fixture.Index+"/s:missing", observation)
}

func concurrentBackendReads(t *testing.T, node *Node, resource string, observation *budgetObservation) {
	t.Helper()
	const count = 64
	client := endpointProcessClient(t, node.Addresses()[0])
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
			t.Fatal("backend connections did not grow with demand", active, node.stores["records"].Snapshot())
		case <-time.After(5 * time.Millisecond):
		}
	}
	snapshot := node.stores["records"].Snapshot()
	if snapshot.Active != count || snapshot.Pending != 0 || snapshot.PendingBytes != 0 {
		t.Fatal("running requests retained queue capacity", snapshot)
	}
	close(gate)
	for range count {
		response := <-replies
		if response.err != nil || response.event.GetReadResult() == nil || response.event.GetReadResult().GetFailure() != nil {
			t.Fatal("concurrent read", response.event, response.err)
		}
	}
	budgetWait(t, "concurrent calls released", func() bool {
		snapshot := node.stores["records"].Snapshot()
		return snapshot.Active == 0 && snapshot.Retained == 0 && snapshot.ResultBytes == 0 && snapshot.WorkingBytes == 0
	})
	t.Log("64 RPCs held on 64 real backend connections; all completed without admission rejection")
}
