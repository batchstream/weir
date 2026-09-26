package searchstore

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
)

func scanTestReply() map[string]json.RawMessage {
	raw := `{"pit_id":"latest","took":1,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"max_score":null,"hits":[{"_index":"records","_id":"a","_score":null,"_source":{"n":9223372036854775807},"sort":[0]}]}}`
	var fields map[string]json.RawMessage
	_ = json.Unmarshal([]byte(raw), &fields)
	return fields
}
func TestScanEnvelopeAndLatestPIT(t *testing.T) {
	for _, mode := range []string{"valid", "skipped", "timeout", "early", "shard_failure", "missing_shards", "missing_timed_out", "missing_took", "missing_hits", "null_hits", "missing_pit", "error_tail", "failed_tail", "duplicate_sort", "bad_score", "bad_max_score", "wrong_index", "big_hit", "too_many"} {
		t.Run(mode, func(t *testing.T) {
			a := &Adapter{config: Config{Store: "search", Index: "records", Profile: "elasticsearch-8.17.0"}}
			n := &scanPlan{items: 1, pit: "previous"}
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
			case "big_hit":
				fields["hits"] = json.RawMessage(strings.ReplaceAll(string(fields["hits"]), "9223372036854775807", `"`+strings.Repeat("x", protocol.MaxDocument)+`"`))
			case "too_many":
				n.items = 0
			}
			raw, _ := json.Marshal(fields)
			page := a.scanReply(raw, n)
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
func TestScanSelectorControlsAndBounds(t *testing.T) {
	a := &Adapter{config: Config{Store: "search", Index: "records"}}
	for _, key := range []string{"pit", "search_after", "from", "size", "sort", "track_total_hits", "timeout", "terminate_after", "allow_partial_search_results", "aggregations", "aggs", "_source", "routing", "slice", "stored_fields", "rescore", "script_fields", "profile"} {
		selector := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"%s":{}}`, key))}
		req := &pb.ScanRequest{Resource: "weir://search/records", Selector: selector}
		if _, f := a.PrepareScan(req); f == nil {
			t.Fatal("allowed control", key)
		}
	}
	for _, raw := range []string{`{"query":{},"query":{}}`, `{"query":` + strings.Repeat(`{"x":`, 34) + `{}` + strings.Repeat(`}`, 34) + `}`, `{"query":{"x":[` + strings.Repeat(`0,`, 4096) + `0]}}`, strings.Repeat(" ", protocol.MaxSelector+1) + "{}"} {
		selector := &pb.Document{MediaType: "application/json", Data: []byte(raw)}
		req := &pb.ScanRequest{Resource: "weir://search/records", Selector: selector}
		if _, f := a.PrepareScan(req); f == nil {
			t.Fatal("accepted excessive selector")
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
					raw = []byte(strings.Repeat(" ", responseLimit+1))
				case "oversized_length":
					w.Header().Set("Content-Length", fmt.Sprint(responseLimit+100))
					raw = nil
				}
				_, _ = w.Write(raw)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			transport := newTransport(1)
			defer transport.CloseIdleConnections()
			a := &Adapter{config: Config{URL: server.URL}, ctx: context.Background(), client: &http.Client{Transport: transport, CheckRedirect: noRedirect}}
			call := exchange{path: "/_search", body: []byte("{}"), limit: responseLimit}
			_, _, err := a.request(context.Background(), call)
			if err == nil {
				t.Fatal("accepted incomplete/oversized response")
			}
		})
	}
}

func TestScanHitByteBoundary(t *testing.T) {
	for _, extra := range []int{0, 1} {
		a := &Adapter{config: Config{Index: "records", Profile: "elasticsearch-8.17.0"}}
		n := &scanPlan{items: 1, pit: "previous"}
		hit := `{"_index":"records","_id":"a","_score":null,"_source":{"pad":""},"sort":[0]}`
		hit = strings.Replace(hit, `"pad":""`, `"pad":"`+strings.Repeat("x", protocol.MaxDocument-len(hit)+extra)+`"`, 1)
		fields := scanTestReply()
		fields["hits"] = json.RawMessage(`{"max_score":null,"hits":[` + hit + `]}`)
		raw, _ := json.Marshal(fields)
		page := a.scanReply(raw, n)
		if extra == 0 {
			if page.Failure != nil || len(page.Documents) != 1 || len(page.Documents[0].Data) != protocol.MaxDocument {
				t.Fatal(page.Failure)
			}
		} else if page.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || len(page.Documents) != 0 {
			t.Fatal("hit boundary", page)
		}
	}
}
