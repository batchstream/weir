package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
)

func readMgetIDs(reader io.Reader) ([]string, error) {
	var request struct {
		Docs []struct {
			Index string `json:"_index"`
			ID    string `json:"_id"`
		} `json:"docs"`
	}
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return nil, err
	}
	ids := make([]string, len(request.Docs))
	for i, document := range request.Docs {
		if document.Index == "" || document.ID == "" {
			return nil, fmt.Errorf("mget document requires an index and ID")
		}
		ids[i] = document.ID
	}
	return ids, nil
}

func TestMixedIndexMgetValidatesEveryPositionBeforePublishing(t *testing.T) {
	cases := []struct {
		name, right string
		leftValid   bool
		code        pb.FailureCode
	}{
		{name: "reordered", code: pb.FailureCode_UNAVAILABLE},
		{name: "wrong index", right: `{"_index":"left","_id":"same","found":false}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "wrong ID", right: `{"_index":"right","_id":"other","found":false}`, code: pb.FailureCode_UNAVAILABLE},
		{name: "missing", code: pb.FailureCode_UNAVAILABLE},
		{name: "permission", right: `{"_index":"right","_id":"same","status":403,"error":{"type":"security_exception"}}`, leftValid: true, code: pb.FailureCode_PERMISSION_DENIED},
		{name: "capacity", right: `{"_index":"right","_id":"same","status":429,"error":{"type":"es_rejected_execution_exception"}}`, leftValid: true, code: pb.FailureCode_UNAVAILABLE},
		{name: "missing index", right: `{"_index":"right","_id":"same","status":404,"error":{"type":"index_not_found_exception"}}`, leftValid: true, code: pb.FailureCode_TARGET_NOT_FOUND},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var reads atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				reads.Add(1)
				if r.URL.Path != "/_mget" || r.Method != http.MethodPost || r.URL.Query().Get("realtime") != "true" {
					t.Error("mixed read lost its realtime native request", r.URL)
				}
				var request struct {
					Docs []struct {
						Index string `json:"_index"`
						ID    string `json:"_id"`
					} `json:"docs"`
				}
				if json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Docs) != 2 || request.Docs[0].Index != "left" || request.Docs[1].Index != "right" || request.Docs[0].ID != "same" || request.Docs[1].ID != "same" {
					t.Error("mixed index request lost original positions", request)
				}
				left := `{"_index":"left","_id":"same","found":true,"_seq_no":7,"_primary_term":2,"_source":{"n":10}}`
				right := test.right
				if test.name == "reordered" {
					left, right = `{"_index":"right","_id":"same","found":false}`, left
				}
				if test.name == "missing" {
					fmt.Fprintf(w, `{"docs":[%s]}`, left)
				} else {
					fmt.Fprintf(w, `{"docs":[%s,%s]}`, left, right)
				}
			})
			adapter := scanBatchAdapter(t, handler)
			left := batchTestPlan(t, adapter, "read", "left/s:same")
			right := batchTestPlan(t, adapter, "read", "right/s:same")
			works := []*execution.Plan{left, right}
			observed := adapter.mget(t.Context(), works)
			if len(observed) != 2 || reads.Load() != 1 || observed[1].failure.GetCode() != test.code {
				t.Fatal("mixed response was accepted or replayed", observed, reads.Load())
			}
			if (observed[0].reply != nil) != test.leftValid {
				t.Fatal("malformed correspondence or item failure affected another position", observed[0])
			}
			if test.leftValid && (string(observed[0].reply.Source) != `{"n":10}` || *observed[0].reply.Seq != 7 || *observed[0].reply.Term != 2) {
				t.Fatal("valid peer lost its source or OCC evidence", observed[0])
			}
		})
	}
}

func TestMixedIndexMgetRetainsReadWindowAndCanceledPositions(t *testing.T) {
	var reads atomic.Int32
	settings := backend.DefaultOptions()
	settings.Scan.Bytes = 1024
	settings.ExchangeBytes = 3*getFramingLimit + 2*settings.Scan.Bytes + 32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reads.Add(1)
		var request struct {
			Docs []struct {
				Index string `json:"_index"`
				ID    string `json:"_id"`
			} `json:"docs"`
		}
		if r.URL.Path != "/_mget" || json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Docs) > 2 {
			t.Error("mixed mget exceeded its read window", r.URL, request)
			return
		}
		items := make([]string, len(request.Docs))
		for i, document := range request.Docs {
			if document.ID == "canceled" {
				t.Error("canceled caller reached the backend")
			}
			items[i] = fmt.Sprintf(`{"_index":%q,"_id":%q,"found":false}`, document.Index, document.ID)
		}
		fmt.Fprintf(w, `{"docs":[%s]}`, strings.Join(items, ","))
	})
	adapter := scanBatchAdapter(t, handler)
	adapter.config.Options = &settings
	works := make([]*execution.Plan, 0, 6)
	for i := range 6 {
		index := "left"
		if i%2 == 1 {
			index = "right"
		}
		id := fmt.Sprint(i)
		if i == 2 {
			id = "canceled"
		}
		work := batchTestPlan(t, adapter, "read", index+"/s:"+id)
		if i == 2 {
			caller, cancel := context.WithCancel(t.Context())
			cancel()
			work.Context = caller
		}
		works = append(works, work)
	}
	observed := adapter.mget(t.Context(), works)
	if len(observed) != 6 || reads.Load() != 3 {
		t.Fatal("mixed read window or canceled slot changed", len(observed), reads.Load())
	}
	for i, record := range observed {
		if i == 2 {
			if record.failure.GetCode() != pb.FailureCode_CANCELLED || record.reply != nil {
				t.Fatal("canceled observation lost its original position", record)
			}
		} else if record.failure != nil || record.reply == nil || *record.reply.Found {
			t.Fatal("cancellation affected a peer", i, record)
		}
	}
}
