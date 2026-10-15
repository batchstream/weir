package search

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/batchstream/weir/internal/backend"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func scanTestReply() map[string]json.RawMessage {
	raw := `{"pit_id":"latest","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[{"_index":"records","_id":"a","_score":null,"_source":{"n":9223372036854775807},"sort":[0]}]}}`
	var fields map[string]json.RawMessage
	_ = json.Unmarshal([]byte(raw), &fields)
	return fields
}
func TestScanEnvelopeAndLatestPIT(t *testing.T) {
	for _, mode := range []string{"valid", "skipped", "timeout", "early", "shard_failure", "missing_shards", "missing_timed_out", "missing_took", "missing_hits", "null_hits", "missing_pit", "error_tail", "failed_tail", "duplicate_sort", "bad_score", "bad_max_score", "wrong_index", "too_many"} {
		t.Run(mode, func(t *testing.T) {
			a := &Adapter{dialect: ElasticsearchProduct, config: Config{Store: "search"}}
			n := &scanPlan{index: "records", items: 1, pit: "previous"}
			fields := scanTestReply()
			switch mode {
			case "skipped":
				fields["_shards"] = json.RawMessage(`{"total":1,"successful":1,"skipped":1,"failed":0}`)
			case "timeout":
				fields["timed_out"] = json.RawMessage(`true`)
			case "early":
				fields["terminated_early"] = json.RawMessage(`true`)
			case "shard_failure":
				fields["_shards"] = json.RawMessage(`{"total":1,"successful":0,"skipped":0,"failed":1}`)
			case "missing_shards":
				delete(fields, "_shards")
			case "missing_timed_out":
				delete(fields, "timed_out")
			case "missing_took":
				delete(fields, "took")
			case "missing_pit":
				delete(fields, "pit_id")
			case "missing_hits":
				fields["hits"] = json.RawMessage(`{"max_score":null}`)
			case "null_hits":
				fields["hits"] = json.RawMessage(`{"hits":null,"max_score":null}`)
			case "error_tail":
				fields["error"] = json.RawMessage(`{"type":"error"}`)
			case "failed_tail":
				fields["_shards"] = json.RawMessage(`{"total":1,"successful":1,"skipped":0,"failed":0,"failures":[{}]}`)
			case "duplicate_sort":
				n.after = 0
				n.hasAfter = true
			case "bad_max_score":
				fields["hits"] = json.RawMessage(strings.ReplaceAll(string(fields["hits"]), `"max_score":null`, `"max_score":"bad"`))
			case "bad_score":
				fields["hits"] = json.RawMessage(strings.ReplaceAll(string(fields["hits"]), `"_score":null`, `"_score":"bad"`))
			case "wrong_index":
				fields["hits"] = json.RawMessage(strings.ReplaceAll(string(fields["hits"]), "records", "other"))
			case "too_many":
				n.items = 0
			}
			raw, _ := json.Marshal(fields)
			page := a.scanReply(context.Background(), raw, n)
			valid := mode == "valid" || mode == "skipped"
			if valid {
				if page.Failure != nil || len(page.Documents) != 1 || !strings.Contains(string(page.Documents[0].Data), "9223372036854775807") {
					t.Fatal(page)
				}
			} else if page.Failure == nil || len(page.Documents) != 0 {
				t.Fatal("failed page leaked hits", page)
			}
			if mode != "missing_pit" && n.pit != "latest" {
				t.Fatal("latest PIT lost on failed envelope")
			}
		})
	}
}
func TestScanFilterControlsAndBounds(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct, config: Config{Store: "search"}}
	filter := &pb.Document{ContentType: "application/json", Data: []byte(`{"term":{"sort":1}}`)}
	projection := &pb.Projection{Mode: pb.ProjectionMode_INCLUDE, Fields: []string{"nested.name"}}
	request := &pb.ScanRequest{Resource: "records", Filter: filter, Projection: projection}
	work, failure := a.prepareScan(context.Background(), request)
	if failure != nil || string(work.Backend.(*scanPlan).query) != string(filter.Data) || work.Backend.(*scanPlan).projection != projection {
		t.Fatal("native query or projection changed", failure)
	}
	for _, raw := range []string{`{"query":{},"query":{}}`, `{"query":` + strings.Repeat(`{"x":`, 10001) + `{}` + strings.Repeat(`}`, 10001) + `}`} {
		selector := &pb.Document{ContentType: "application/json", Data: []byte(raw)}
		req := &pb.ScanRequest{Resource: "records", Filter: selector}
		if _, f := a.prepareScan(context.Background(), req); f == nil {
			t.Fatal("accepted excessive selector")
		}
	}
}
func TestScanLargeFiltersRetainTheirCompleteQuery(t *testing.T) {
	adapter := &Adapter{dialect: ElasticsearchProduct}
	for _, raw := range []string{`{"terms":{"x":[` + strings.Repeat(`0,`, 20000) + `0]}}`, strings.Repeat(" ", 20000) + "{}"} {
		filter := &pb.Document{ContentType: "application/json", Data: []byte(raw)}
		request := &pb.ScanRequest{Resource: "records", Filter: filter}
		work, failure := adapter.prepareScan(context.Background(), request)
		if failure != nil || string(work.Backend.(*scanPlan).query) != raw {
			t.Fatal("large native filter rejected or shortened", failure)
		}
	}
}

func TestScanResponseFramingBoundBeforeDecode(t *testing.T) {
	for _, mode := range []string{"truncated", "duplicate", "tail", "oversized_chunked", "oversized_length"} {
		t.Run(mode, func(t *testing.T) {
			fields := scanTestReply()
			raw, _ := json.Marshal(fields)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "truncated":
					raw = raw[:len(raw)-1]
				case "duplicate":
					raw = append([]byte(`{"timed_out":true,`), raw[1:]...)
				case "tail":
					raw = append(raw, []byte(`{}`)...)
				case "oversized_chunked":
					w.(http.Flusher).Flush()
					raw = []byte(strings.Repeat(" ", (8<<20)+1))
				case "oversized_length":
					w.Header().Set("Content-Length", fmt.Sprint((8<<20)+100))
					raw = nil
				}
				_, _ = w.Write(raw)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			transport := newTransport(connectionTimeout, backend.DefaultOptions().IdleTimeout)
			defer transport.CloseIdleConnections()
			a := &Adapter{dialect: ElasticsearchProduct, config: Config{URL: server.URL}, ctx: context.Background(), client: &http.Client{Transport: transport, CheckRedirect: noRedirect}}
			call := exchange{path: "/_search", body: []byte("{}")}
			_, _, err := a.request(context.Background(), call)
			if err == nil {
				t.Fatal("accepted incomplete/oversized response")
			}
		})
	}
}

func TestScanLargeHitsCrossHistoricalAndBatchByteThresholds(t *testing.T) {
	for _, size := range []int{2 << 20, (2 << 20) + 1, 6 << 20} {
		adapter := &Adapter{dialect: ElasticsearchProduct}
		state := &scanPlan{index: "records", items: 1, pit: "previous"}
		rows := []json.RawMessage{scanBatchHit(0, size)}
		page := adapter.scanReply(context.Background(), scanBatchReply(rows), state)
		if page.Failure != nil || len(page.Documents) != 1 || len(page.Documents[0].Data) != size || !state.hasAfter {
			t.Fatal("large single hit rejected or failed to advance", page, size)
		}
	}
}
