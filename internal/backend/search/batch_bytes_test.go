package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/prometheus/client_golang/prometheus"
)

type wireBatchFixture struct {
	t      *testing.T
	mu     sync.Mutex
	bodies [][]byte
	counts []int
	events []string
	source string
}

func (f *wireBatchFixture) serveHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, r.URL.Path)
	switch r.URL.Path {
	case "/records":
		_, _ = io.WriteString(w, testIndexReply)
	case "/records/_mget":
		source := f.source
		if source == "" {
			source = `{"n":1}`
		}
		var request struct{ IDs []string }
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			f.t.Error(err)
			return
		}
		_, _ = io.WriteString(w, `{"docs":[`)
		for i, id := range request.IDs {
			if i != 0 {
				_, _ = io.WriteString(w, ",")
			}
			_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":%s}`, id, source)
		}
		_, _ = io.WriteString(w, "]}")
	case "/_bulk":
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			f.t.Error(err)
			return
		}
		if len(raw) == 0 || raw[len(raw)-1] != '\n' {
			f.t.Error("NDJSON is empty or missing its final newline")
			return
		}
		lines := bytes.Split(raw[:len(raw)-1], []byte{'\n'})
		var items []any
		for i := 0; i < len(lines); i++ {
			var header map[string]map[string]any
			if err := json.Unmarshal(lines[i], &header); err != nil || len(header) != 1 {
				f.t.Error("invalid NDJSON header", string(lines[i]), err)
				return
			}
			for action, metadata := range header {
				if action != "delete" {
					i++
					if i >= len(lines) || !json.Valid(lines[i]) {
						f.t.Error("invalid NDJSON source")
						return
					}
				}
				status, result := 200, "updated"
				if action == "create" {
					status, result = 201, "created"
				} else if action == "delete" {
					result = "deleted"
				}
				shards := map[string]any{"total": 1, "successful": 1, "failed": 0}
				reply := map[string]any{"_index": metadata["_index"], "_id": metadata["_id"], "status": status, "result": result, "_version": 2, "_seq_no": len(items) + 2, "_primary_term": 1, "_shards": shards}
				items = append(items, map[string]any{action: reply})
			}
		}
		f.bodies = append(f.bodies, raw)
		f.counts = append(f.counts, len(items))
		response := map[string]any{"errors": false, "took": 1, "items": items}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			f.t.Error(err)
		}
	default:
		f.t.Error("unexpected request", r.URL)
		http.NotFound(w, r)
	}
}

func TestBulkFramingUsesSmallerConfiguredByteWindow(t *testing.T) {
	cases := []struct {
		name            string
		batch, exchange int
		payload         int
		counts          []int
	}{
		{name: "batch bytes", batch: 1024, exchange: 4096, payload: 400, counts: []int{2, 2, 1}},
		{name: "exchange bytes", batch: 4096, exchange: 1024, payload: 400, counts: []int{2, 2, 1}},
		{name: "single larger entry", batch: 1024, exchange: 1024, payload: 1600, counts: []int{1, 1, 1, 1, 1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &wireBatchFixture{t: t}
			handler := http.HandlerFunc(fixture.serveHTTP)
			adapter := testHTTPAdapter(t, handler)
			settings := backend.DefaultOptions()
			settings.BatchBytes, settings.ExchangeBytes = tc.batch, tc.exchange
			adapter.config.Options = &settings
			registry := prometheus.NewPedanticRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			source := []byte(`{ "payload": "` + strings.Repeat("x", tc.payload) + `" }`)
			works := make([]*execution.Plan, 0, 5)
			for i := range 5 {
				document := &pb.Document{ContentType: "application/json", Data: source}
				action := &pb.MutateRequest_Create{Create: document}
				mutation := &pb.MutateRequest{Resource: fmt.Sprintf("records/s:id-%d", i), Action: action}
				operation := &pb.Command_Mutate{Mutate: mutation}
				command := &pb.Command{Operation: operation}
				request := &pb.ExecuteRequest{Index: uint64(i + 1), Command: command}
				work, failure := prepareTestRecord(adapter, request)
				if failure != nil {
					t.Fatal(failure)
				}
				works = append(works, work)
			}
			for _, result := range adapter.executeRecords(t.Context(), works) {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
					t.Fatal("legal input failed", result)
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			if !reflect.DeepEqual(fixture.counts, tc.counts) {
				t.Fatal("unexpected framed batch counts", fixture.counts)
			}
			var totalBytes int
			for i, body := range fixture.bodies {
				totalBytes += len(body)
				if len(body) > min(tc.batch, tc.exchange) && fixture.counts[i] != 1 {
					t.Fatal("multiple records exceeded the soft byte window", len(body), fixture.counts[i])
				}
			}
			families := searchMetricFamilies(t, registry)
			labels := map[string]string{"operation": "bulk", "visibility": "none"}
			bodyBytes := searchMetric(t, families["weir_search_http_request_body_bytes"], labels).GetHistogram()
			if bodyBytes.GetSampleCount() != uint64(len(fixture.bodies)) || bodyBytes.GetSampleSum() != float64(totalBytes) {
				t.Fatal("byte observations differ from actual framed HTTP bodies", bodyBytes, totalBytes)
			}
		})
	}
}

func TestLuaGeneratedBytesFlushBeforeNextRead(t *testing.T) {
	cases := []struct {
		name                 string
		oversize, splitReads bool
	}{
		{name: "exact threshold", splitReads: true},
		{name: "single larger entry", oversize: true, splitReads: true},
		{name: "independent read window"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fixture := &wireBatchFixture{t: t, source: `{"payload":"` + strings.Repeat("x", 350) + `"}`}
			handler := http.HandlerFunc(fixture.serveHTTP)
			adapter := testHTTPAdapter(t, handler)
			settings := backend.DefaultOptions()
			if tc.splitReads {
				settings.ExchangeBytes = 2048
				settings.BatchBytes = 2048
			}
			source := `{"payload":"` + strings.Repeat("x", 700) + `"}`
			header := `{"index":{"_id":"id-0","_index":"records","if_primary_term":1,"if_seq_no":1,"pipeline":"_none"}}`
			settings.BatchBytes = len(header) + len(source) + 2
			if tc.oversize {
				settings.BatchBytes--
			}
			adapter.config.Options = &settings
			works := make([]*execution.Plan, 0, 3)
			for i := range 3 {
				work := batchTestPlan(t, adapter, "program", fmt.Sprintf("records/s:id-%d", i))
				work.Backend.(*plan).program.Source = `return function(current) current.payload = current.payload .. current.payload; return current end`
				works = append(works, work)
			}
			for _, result := range adapter.executeRecords(t.Context(), works) {
				if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
					t.Fatal("Lua expansion failed", result)
				}
			}
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			wantEvents := []string{"/records", "/records/_mget", "/_bulk", "/records/_mget", "/_bulk", "/records/_mget", "/_bulk"}
			if !tc.splitReads {
				wantEvents = []string{"/records", "/records/_mget", "/_bulk", "/_bulk", "/_bulk"}
			}
			if !reflect.DeepEqual(fixture.events, wantEvents) || !reflect.DeepEqual(fixture.counts, []int{1, 1, 1}) {
				t.Fatal("full generated batch waited for subsequent pre-read or Lua evaluation", fixture.events, fixture.counts)
			}
			wantBody := header + "\n" + source + "\n"
			if string(fixture.bodies[0]) != wantBody {
				t.Fatal("byte threshold omitted actual metadata or NDJSON framing", string(fixture.bodies[0]))
			}
		})
	}
}
