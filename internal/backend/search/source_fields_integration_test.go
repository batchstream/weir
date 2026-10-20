//go:build integration

package search

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSearchSourceFieldsUseBackendMappingAcrossMutations(t *testing.T) {
	adapter, backend := setupSearch(t)
	var writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/_bulk" {
			writes.Add(1)
		}
		request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		request.Header = r.Header.Clone()
		request.GetBody = nil
		response, err := backend.Client.Do(request)
		if err != nil {
			t.Error(err)
			w.WriteHeader(502)
			return
		}
		defer response.Body.Close()
		w.Header().Set("Content-Type", response.Header.Get("Content-Type"))
		w.WriteHeader(response.StatusCode)
		_, _ = io.Copy(w, response.Body)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	cfg := adapter.config
	cfg.URL = proxy.URL
	through, err := Open(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer through.Close(context.Background())
	resource := searchResource(backend.Index, "counter")
	prepare := func(action, source string) *pb.ExecuteRequest {
		work := batchTestPlan(t, through, action, resource)
		mutation := work.Command.GetMutate()
		switch action {
		case "put":
			mutation.GetPut().Data = []byte(source)
		case "program":
			mutation.GetAtomicTransform().GetLua().Source = []byte(`return function(current, incoming) return incoming end`)
			input := &pb.Document{ContentType: "application/json", Data: []byte(source)}
			mutation.GetAtomicTransform().GetLua().Input = input
		case "expression":
			mutation.GetAtomicTransform().GetBackendExpression().Data = []byte(`{"doc":` + source + `}`)
		}
		request := &pb.ExecuteRequest{Index: work.ID, Command: work.Command}
		return request
	}
	for position, action := range []string{"put", "program", "expression"} {
		t.Run(action, func(t *testing.T) {
			request := prepare(action, `{"variables":{"_id":"business","usd.price":`+strconv.Itoa(position+1)+`}}`)
			work, failure := prepareTestRecord(through, request)
			if failure != nil {
				t.Fatal("source fields rejected before backend mapping", failure)
			}
			assertOutcome(t, runSearch(t, through, work), pb.MutationOutcome_APPLIED, 0)
			read := searchPlan(t, through, "read", resource)
			result := runSearch(t, through, read).GetReadResult()
			var source struct{ Variables map[string]json.RawMessage }
			if result.GetFailure() != nil || json.Unmarshal(result.GetDocument().GetData(), &source) != nil || string(source.Variables["_id"]) != `"business"` || string(source.Variables["usd.price"]) != strconv.Itoa(position+1) {
				t.Fatal("nested metadata name or dotted business key changed", result)
			}
		})
	}
	beforeSource := runSearch(t, through, searchPlan(t, through, "read", resource)).GetReadResult().GetDocument().GetData()
	for _, action := range []string{"put", "program", "expression"} {
		t.Run(action+"-native-rejection", func(t *testing.T) {
			request := prepare(action, `{"_id":"cannot-change-record-identity"}`)
			work, failure := prepareTestRecord(through, request)
			if failure != nil {
				t.Fatal("native source rejection must come from backend", failure)
			}
			before := writes.Load()
			assertOutcome(t, runSearch(t, through, work), pb.MutationOutcome_NOT_APPLIED, pb.FailureCode_PRECONDITION_FAILED)
			if writes.Load() != before+1 {
				t.Fatal("native rejection was replayed", writes.Load()-before)
			}
			after := runSearch(t, through, searchPlan(t, through, "read", resource)).GetReadResult().GetDocument().GetData()
			if string(after) != string(beforeSource) {
				t.Fatal("rejected source altered the record", string(after))
			}
		})
	}
}
