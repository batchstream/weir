//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

type scenarioBulk struct {
	bytes, records int
}

type scenarioObserver struct {
	mu    sync.Mutex
	bulks []scenarioBulk
}

func startScenarioNode(t *testing.T, fixture *testsearch.Backend) (*routeAcceptanceNode, *scenarioObserver) {
	t.Helper()
	origin, err := url.Parse(fixture.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(origin)
	proxy.Transport = fixture.Client.Transport
	observer := &scenarioObserver{}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" {
			raw, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
			_ = r.Body.Close()
			if err != nil || len(raw) == 2<<20 || len(raw) == 0 || raw[len(raw)-1] != '\n' {
				t.Error("invalid owned bulk request", len(raw), err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			lines := bytes.Split(bytes.TrimSuffix(raw, []byte{'\n'}), []byte{'\n'})
			if len(lines)%2 != 0 {
				t.Error("expected two NDJSON lines per index mutation", len(lines))
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			observed := scenarioBulk{bytes: len(raw), records: len(lines) / 2}
			observer.mu.Lock()
			observer.bulks = append(observer.bulks, observed)
			observer.mu.Unlock()
			r.Body = io.NopCloser(bytes.NewReader(raw))
		}
		proxy.ServeHTTP(w, r)
	})
	front := httptest.NewServer(handler)
	t.Cleanup(front.Close)
	options := backend.DefaultOptions()
	options.BatchBytes = 64 << 10
	config := search.Config{Store: "records", URL: front.URL, Options: &options}
	adapter, err := search.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.BatchOperations, limits.BatchBytes = 32, options.BatchBytes
	limits.BatchWait, limits.MaxInFlight = 20*time.Millisecond, 4
	nodeOptions := routeAcceptanceNodeOptions{adapter: adapter, store: limits}
	node := startRouteAcceptanceNode(t, nodeOptions)
	return node, observer
}

type scenarioRecord struct {
	index, id string
	padding   int
}

func seedScenarioRecord(t *testing.T, fixture *testsearch.Backend, record scenarioRecord) {
	t.Helper()
	body := fmt.Sprintf(`{"n":0,"marker":%q,"pad":%q}`, record.id, strings.Repeat("x", record.padding))
	code, raw := fixture.Do(t, http.MethodPut, "/"+record.index+"/_doc/"+record.id, body)
	if code != http.StatusCreated {
		t.Fatal("seed owned scenario", code, string(raw))
	}
}

type scenarioReply struct {
	mutation *pb.MutationResult
	err      error
}

func incrementScenarioRecords(t *testing.T, node *routeAcceptanceNode, records []scenarioRecord) []scenarioReply {
	t.Helper()
	clients := make([]pb.StoreServiceClient, 4)
	for i := range clients {
		clients[i] = routeAcceptanceClient(t, node.address)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	results := make([]scenarioReply, len(records))
	start := make(chan struct{})
	var workers sync.WaitGroup
	for i, record := range records {
		workers.Go(func() {
			<-start
			mutation := luaRPCMutation(record.index+"/s:"+record.id, `return function(current, incoming) current.n = current.n + 1; return current end`)
			requests := []*pb.MutateRequest{mutation}
			replies, err := testutil.MutateRecords(ctx, clients[i%len(clients)], "records", requests)
			results[i].err = err
			if len(replies) == 1 {
				results[i].mutation = replies[0]
			}
		})
	}
	close(start)
	workers.Wait()
	return results
}

func verifyScenarioRecord(t *testing.T, fixture *testsearch.Backend, record scenarioRecord, increments int) {
	t.Helper()
	code, raw := fixture.Do(t, http.MethodGet, "/"+record.index+"/_doc/"+record.id, "")
	var reply struct {
		Version int                        `json:"_version"`
		Source  map[string]json.RawMessage `json:"_source"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &reply) != nil || len(reply.Source) != 3 {
		t.Fatal("persisted scenario shape", code, string(raw))
	}
	wantPadding, _ := json.Marshal(strings.Repeat("x", record.padding))
	wantMarker, _ := json.Marshal(record.id)
	if reply.Version != increments+1 || string(reply.Source["n"]) != fmt.Sprint(increments) || !bytes.Equal(reply.Source["pad"], wantPadding) || !bytes.Equal(reply.Source["marker"], wantMarker) {
		t.Fatal("persisted version, counter or untouched fields differ", record.id, reply.Version, increments)
	}
}

func TestRouteSearchCollectionAcrossSkewedIndexesAndDocumentSizes(t *testing.T) {
	fixture := testsearch.Open(t)
	indexes := []string{fixture.Index}
	for i := 1; i < 8; i++ {
		index := fmt.Sprintf("%s_%d", fixture.Index, i)
		fixture.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"pad":{"type":"keyword","index":false,"doc_values":false}}}}`)
		indexes = append(indexes, index)
	}
	records := make([]scenarioRecord, 100)
	for i := range records {
		index := 0
		if i >= 80 {
			index = 1 + (i-80)%7
		}
		padding := 1024
		if i%10 == 0 {
			padding = 256 << 10
		}
		record := scenarioRecord{index: indexes[index], id: fmt.Sprintf("record-%03d", i), padding: padding}
		records[i] = record
		seedScenarioRecord(t, fixture, record)
	}
	node, observer := startScenarioNode(t, fixture)
	results := incrementScenarioRecords(t, node, records)
	for i, result := range results {
		if result.err != nil || result.mutation.GetOutcome() != pb.MutationOutcome_APPLIED || result.mutation.GetFailure() != nil {
			t.Fatal("independent single-record mutation", i, result.err, result.mutation)
		}
		verifyScenarioRecord(t, fixture, records[i], 1)
	}
	observer.mu.Lock()
	defer observer.mu.Unlock()
	total, oversized, grouped := 0, 0, 0
	for _, bulk := range observer.bulks {
		total += bulk.records
		if bulk.bytes > 64<<10 {
			oversized++
			if bulk.records != 1 {
				t.Fatal("oversized generated document shared a bulk", bulk)
			}
		}
		if bulk.records > 1 {
			grouped++
		}
	}
	if total != len(records) || oversized != 10 || grouped == 0 {
		t.Fatal("wire aggregation or single-large-record evidence", total, oversized, grouped)
	}
	t.Logf("8 indexes, 80%% hot-index traffic, 1/256 KiB source documents: %d acknowledged increments; %d multi-record bulks, 10 isolated large bodies", total, grouped)
}

func TestRouteSearchBoundedCollectionPreservesHotKeyIncrements(t *testing.T) {
	fixture := testsearch.Open(t)
	hot := scenarioRecord{index: fixture.Index, id: "counter", padding: 1024}
	seedScenarioRecord(t, fixture, hot)
	records := make([]scenarioRecord, 80)
	for i := range records {
		if i%10 == 0 {
			records[i] = hot
			continue
		}
		record := scenarioRecord{index: fixture.Index, id: fmt.Sprintf("private-%03d", i), padding: 1024}
		records[i] = record
		seedScenarioRecord(t, fixture, record)
	}
	node, _ := startScenarioNode(t, fixture)
	results := incrementScenarioRecords(t, node, records)
	applied := 0
	for i, result := range results {
		if result.err != nil || result.mutation == nil {
			t.Fatal("hot-key mutation lost outcome", i, result.err)
		}
		if result.mutation.Outcome == pb.MutationOutcome_APPLIED && result.mutation.Failure == nil {
			if i%10 == 0 {
				applied++
			} else {
				verifyScenarioRecord(t, fixture, records[i], 1)
			}
			continue
		}
		if i%10 != 0 || result.mutation.Outcome != pb.MutationOutcome_NOT_APPLIED || result.mutation.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
			t.Fatal("unexpected mutation failure", i, result.mutation)
		}
	}
	if applied == 0 {
		t.Fatal("no shared-key write applied")
	}
	verifyScenarioRecord(t, fixture, hot, applied)
	t.Logf("10%% shared-key increments, 4 execution slots and 20ms collection: %d/8 shared mutations applied; persisted counter and version match exactly", applied)
}
