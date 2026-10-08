package search

import (
	"bytes"
	"encoding/json"
	"fmt"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"net/http"
	"strings"
	"testing"
)

func TestOrdinaryReadAcceptsLargeJSONArrays(t *testing.T) {
	source := `{"items":[` + strings.Repeat("0,", 19999) + `0]}`
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(w, `{"docs":[{"_index":"records","_id":"item","found":true,"_seq_no":1,"_primary_term":1,"_source":%s}]}`, source)
	})
	adapter := scanBatchAdapter(t, handler)
	settings := backend.DefaultOptions()
	settings.Lua.Values.MaxNodes = 32768
	adapter.config.Options = &settings
	document := &pb.Document{ContentType: "application/json", Data: []byte(source)}
	mutationAction := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "records/s:item", Action: mutationAction}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{Index: 1, Command: command}
	if _, failure := prepareTestRecord(adapter, request); failure != nil {
		t.Fatal("put rejected", failure)
	}
	work := batchTestPlan(t, adapter, "read", "records/s:item")
	works := []*execution.Plan{work}
	result := adapter.mget(t.Context(), works)[0]
	if result.failure != nil {
		t.Fatalf("accepted %d-byte 20000-item document unreadable with max_nodes=32768: %v", len(source), result.failure)
	}
}

func TestBulkAcknowledgementUsesConfiguredExchangeCapacity(t *testing.T) {
	settings := backend.DefaultOptions()
	config := Config{Store: "search", Options: &settings}
	adapter := &Adapter{dialect: ElasticsearchProduct, config: config}
	var output bytes.Buffer
	output.WriteString(`{"errors":false,"took":1,"items":[`)
	works := make([]*execution.Plan, 50000)
	for i := range works {
		id := fmt.Sprintf("item-%d", i)
		empty := &pb.Empty{}
		action := &pb.MutateRequest_Delete{Delete: empty}
		mutation := &pb.MutateRequest{Resource: "records/s:" + id, Action: action}
		operation := &pb.Command_Mutate{Mutate: mutation}
		command := &pb.Command{Operation: operation}
		native := &plan{index: "records", id: id, action: "delete"}
		work := &execution.Plan{ID: uint64(i + 1), Command: command, Backend: native}
		works[i] = work
		if i != 0 {
			output.WriteByte(',')
		}
		encodedID, _ := json.Marshal(id)
		fmt.Fprintf(&output, `{"delete":{"_index":"records","_id":%s,"status":200,"result":"deleted","_version":1,"_seq_no":%d,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, encodedID, i)
	}
	output.WriteString(`]}`)
	if output.Len() <= responseLimit || output.Len() > settings.ExchangeBytes {
		t.Fatal("bad fixture size", output.Len())
	}
	results := adapter.bulkResults(works, 200, output.Bytes(), nil)
	if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatalf("valid %d-byte acknowledgement below configured %d cap returned %v", output.Len(), settings.ExchangeBytes, results[0].GetMutationResult())
	}
}
