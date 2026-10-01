package search

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchMixedRequestTargets(t *testing.T) {
	var inspections, reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		index := parts[0]
		if index == "left" || index == "right" {
			if len(parts) == 1 {
				inspections.Add(1)
				_, _ = io.WriteString(w, strings.ReplaceAll(testIndexReply, "records", index))
				return
			}
			if parts[1] == "_mget" {
				reads.Add(1)
				var body struct{ IDs []string }
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Error(err)
					return
				}
				value, sequence := 10, 1
				if index == "right" {
					value, sequence = 20, 2
				}
				fmt.Fprint(w, `{"docs":[`)
				for i, id := range body.IDs {
					if i != 0 {
						fmt.Fprint(w, ",")
					}
					fmt.Fprintf(w, `{"_index":%q,"_id":%q,"found":true,"_seq_no":%d,"_primary_term":1,"_source":{"n":%d}}`, index, id, sequence, value)
				}
				fmt.Fprint(w, "]}")
				return
			}
		}
		if r.URL.Path != "/_bulk" {
			t.Error("unexpected endpoint", r.URL.Path)
			return
		}
		writes.Add(1)
		scanner := bufio.NewScanner(r.Body)
		fmt.Fprint(w, `{"errors":false,"took":1,"items":[`)
		count := 0
		for scanner.Scan() {
			var header map[string]struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
				Seq   *int   `json:"if_seq_no"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &header); err != nil || len(header) != 1 {
				t.Error("invalid bulk metadata", err)
				return
			}
			for action, metadata := range header {
				if metadata.Index != "left" && metadata.Index != "right" {
					t.Error("bulk lost request target", metadata.Index)
				}
				if metadata.Seq != nil {
					want := 1
					if metadata.Index == "right" {
						want = 2
					}
					if *metadata.Seq != want {
						t.Error("conditional write used another index's observation")
					}
				}
				if action != "delete" && !scanner.Scan() {
					t.Error("source missing")
					return
				}
				result, status := "updated", 200
				if action == "create" {
					result, status = "created", 201
				} else if action == "delete" {
					result = "deleted"
				}
				if count != 0 {
					fmt.Fprint(w, ",")
				}
				fmt.Fprintf(w, `{%q:{"_index":%q,"_id":%q,"status":%d,"result":%q,"_version":1,"_seq_no":3,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, action, metadata.Index, metadata.ID, status, result)
				count++
			}
		}
		if err := scanner.Err(); err != nil {
			t.Error(err)
		}
		fmt.Fprint(w, "]}")
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{Store: "search", URL: server.URL}
	a := &Adapter{config: cfg, dialect: ElasticsearchProfile, client: server.Client(), ctx: context.Background()}
	cases := []struct{ index, action, id string }{
		{index: "left", action: "read", id: "same"},
		{index: "right", action: "read", id: "same"},
		{index: "left", action: "replace", id: "same"},
		{index: "right", action: "program", id: "same"},
		{index: "left", action: "expression", id: "expression"},
		{index: "right", action: "put", id: "put"},
		{index: "left", action: "create", id: "create"},
		{index: "right", action: "delete", id: "delete"},
	}
	works := make([]*execution.Plan, len(cases))
	for i, target := range cases {
		resource := "weir://search/" + target.index + "/s:" + target.id
		works[i] = batchTestPlan(t, a, target.action, resource)
		works[i].Operation.Index = uint64(i)
	}
	results, feedback := a.Execute(context.Background(), works)
	if len(results) != len(works) || feedback != execution.Healthy || inspections.Load() != 2 || reads.Load() != 2 || writes.Load() != 1 {
		t.Fatal("mixed index request framing", len(results), feedback, inspections.Load(), reads.Load(), writes.Load())
	}
	for i, result := range results {
		if result.Index != uint64(i) {
			t.Fatal("caller result position lost", result)
		}
		if i < 2 {
			want := fmt.Sprintf(`{"n":%d}`, 10+i*10)
			if string(result.GetRead().GetDocument().GetData()) != want {
				t.Fatal("same ID read from wrong index", result)
			}
		} else if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutation().GetFailure() != nil {
			t.Fatal("mixed target mutation failed", result)
		}
	}

	var concurrent sync.WaitGroup
	for range 16 {
		left := batchTestPlan(t, a, "read", "weir://search/left/s:same")
		right := batchTestPlan(t, a, "read", "weir://search/right/s:same")
		concurrent.Go(func() {
			results, _ := a.Execute(context.Background(), []*execution.Plan{left, right})
			if string(results[0].GetRead().GetDocument().GetData()) != `{"n":10}` || string(results[1].GetRead().GetDocument().GetData()) != `{"n":20}` {
				t.Error("concurrent request changed adapter target", results)
			}
		})
	}
	concurrent.Wait()
}

func TestSearchInvalidRequestTargetsArePure(t *testing.T) {
	cfg := Config{Store: "search"}
	a := &Adapter{config: cfg}
	for _, resource := range []string{
		"weir://other/records", "weir://search", "weir://search/Records",
		"weir://search/_hidden", "weir://search/a,b", "weir://search/a*",
		"weir://search/.hidden", "weir://search/" + strings.Repeat("a", 64),
		"weir://search/records/extra", "weir://search/records?query=x",
	} {
		read := &pb.ReadRequest{Resource: resource + "/s:same"}
		variant := &pb.BulkOperation_Read{Read: read}
		op := &pb.BulkOperation{Operation: variant}
		if _, failure := a.Prepare(op); failure == nil {
			t.Error("record target accepted", resource)
		}
		scan := &pb.ScanRequest{Resource: resource}
		if _, failure := a.PrepareScan(scan); failure == nil {
			t.Error("Scan target accepted", resource)
		}
		native := nativeOpen(t, "records", "GET", "/_doc/same")
		native.Resource = resource
		if _, failure := a.PrepareNative(native); failure == nil {
			t.Error("Native target accepted", resource)
		}
	}
}

func TestSearchPlansRetainRequestTargets(t *testing.T) {
	cfg := Config{Store: "search"}
	a := &Adapter{config: cfg}
	record := batchTestPlan(t, a, "read", "weir://search/left/s:same")
	record.Operation.GetRead().Resource = "weir://search/right/s:same"
	if record.Backend.(*plan).index != "left" {
		t.Fatal("record target follows mutable request")
	}
	req := &pb.ScanRequest{Resource: "weir://search/left"}
	scan, failure := a.PrepareScan(req)
	if failure != nil {
		t.Fatal(failure)
	}
	req.Resource = "weir://search/right"
	if scan.Backend.(*scanPlan).index != "left" {
		t.Fatal("Scan target follows mutable request")
	}
	open := nativeOpen(t, "left", "GET", "/_doc/same")
	native, failure := a.PrepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	open.Resource = "weir://search/right"
	if native.Backend.(*nativePlan).index != "left" {
		t.Fatal("Native target follows mutable request")
	}
}

func TestSearchCrossIndexRepliesAreNotTrusted(t *testing.T) {
	for _, action := range []string{"read", "put"} {
		t.Run(action, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
				index := parts[0]
				if index == "left" || index == "right" {
					if len(parts) == 1 {
						_, _ = io.WriteString(w, strings.ReplaceAll(testIndexReply, "records", index))
						return
					}
					calls.Add(1)
					other := "left"
					if index == "left" {
						other = "right"
					}
					fmt.Fprintf(w, `{"docs":[{"_index":%q,"_id":"same","found":false}]}`, other)
					return
				}
				calls.Add(1)
				fmt.Fprint(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"right","_id":"same","status":200}},{"index":{"_index":"left","_id":"same","status":200}}]}`)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			cfg := Config{Store: "search", URL: server.URL}
			a := &Adapter{config: cfg, dialect: ElasticsearchProfile, client: server.Client(), ctx: context.Background()}
			left := batchTestPlan(t, a, action, "weir://search/left/s:same")
			right := batchTestPlan(t, a, action, "weir://search/right/s:same")
			works := []*execution.Plan{left, right}
			results, _ := a.Execute(context.Background(), works)
			for _, result := range results {
				if action == "read" {
					if result.GetRead().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
						t.Fatal("cross-index mget became a trusted missing record", result)
					}
				} else if result.GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
					t.Fatal("cross-index acknowledgement became a trusted write", result)
				}
			}
			wantCalls := int32(1)
			if action == "read" {
				wantCalls = 2
			}
			if calls.Load() != wantCalls {
				t.Fatal("ambiguous response triggered replay", calls.Load())
			}
		})
	}
}
