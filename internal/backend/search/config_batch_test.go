package search

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
)

func TestLargeBatchSplitsConfiguredExchangesWithoutRejectingInputs(t *testing.T) {
	calls, received := 0, 0
	settings := backend.DefaultOptions()
	settings.ExchangeBytes = 4 << 20
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = w.Write([]byte(testIndexReply))
			return
		}
		if r.URL.Path != "/_bulk" || r.URL.Query().Has("timeout") {
			t.Error("unexpected bulk request", r.URL)
			return
		}
		calls++
		reader := bufio.NewScanner(r.Body)
		reader.Buffer(make([]byte, 4096), 2<<20)
		size := 0
		var items []any
		for reader.Scan() {
			raw := reader.Bytes()
			size += len(raw) + 1
			var header map[string]map[string]any
			if err := json.Unmarshal(raw, &header); err != nil {
				t.Error(err)
				return
			}
			if !reader.Scan() {
				t.Error("missing source")
				return
			}
			size += len(reader.Bytes()) + 1
			metadata := header["create"]
			shards := map[string]any{"total": 1, "successful": 1, "failed": 0}
			reply := map[string]any{"_index": metadata["_index"], "_id": metadata["_id"], "status": 201, "result": "created", "_version": 1, "_seq_no": received, "_primary_term": 1, "_shards": shards}
			items = append(items, map[string]any{"create": reply})
			received++
		}
		if err := reader.Err(); err != nil {
			t.Error(err)
		}
		if size > settings.ExchangeBytes {
			t.Error("native exchange exceeded configuration", size)
		}
		response := map[string]any{"errors": false, "took": 1, "items": items}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			t.Error(err)
		}
	})
	adapter := scanBatchAdapter(t, handler)
	adapter.config.Options = &settings
	source := []byte(`{"payload":"` + strings.Repeat("x", 1<<20) + `"}`)
	var plans []*execution.Plan
	for i := range 40 {
		document := &pb.Document{ContentType: "application/json", Data: source}
		action := &pb.MutateRequest_Create{Create: document}
		mutation := &pb.MutateRequest{Resource: "records/s:" + strings.Repeat("x", i+1), Action: action}
		operation := &pb.Command_Mutate{Mutate: mutation}
		command := &pb.Command{Operation: operation}
		request := &pb.ExecuteRequest{Index: uint64(i + 1), Command: command}
		work, failure := prepareTestRecord(adapter, request)
		if failure != nil {
			t.Fatal(failure)
		}
		plans = append(plans, work)
	}
	results := adapter.executeRecords(t.Context(), plans)
	if calls < 2 || received != 40 {
		t.Fatal("batch did not split or lost inputs", calls, received)
	}
	for _, result := range results {
		if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("large batch failed", result)
		}
	}
}
