package search

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
)

func testBulkMutationReply(a *Adapter, native *plan, raw string) *pb.MutationResult {
	work := &execution.Plan{Backend: native}
	body := []byte(fmt.Sprintf(`{"errors":%t,"took":1,"items":[{%q:%s}]}`, strings.Contains(raw, `"error":`), native.bulkAction(), raw))
	results := a.bulkResults([]*execution.Plan{work}, 200, body, nil)
	return results[0].GetMutationResult()
}

func TestProgramWriteReplyRecognizesRejectionEnvelopes(t *testing.T) {
	cases := []struct {
		product string
		status  int
		kind    string
		failure pb.FailureCode
	}{
		{product: ElasticsearchProduct, status: 400, kind: "mapper_parsing_exception", failure: pb.FailureCode_PRECONDITION_FAILED},
		{product: ElasticsearchProduct, status: 400, kind: "document_parsing_exception", failure: pb.FailureCode_PRECONDITION_FAILED},
		{product: ElasticsearchProduct, status: 404, kind: "index_not_found_exception", failure: pb.FailureCode_TARGET_NOT_FOUND},
		{product: ElasticsearchProduct, status: 409, kind: "version_conflict_engine_exception", failure: pb.FailureCode_CONFLICT},
		{product: ElasticsearchProduct, status: 429, kind: "es_rejected_execution_exception", failure: pb.FailureCode_UNAVAILABLE},
		{product: OpenSearchProduct, status: 429, kind: "rejected_execution_exception", failure: pb.FailureCode_UNAVAILABLE},
		{product: ElasticsearchProduct, status: 503, kind: "unavailable_shards_exception", failure: pb.FailureCode_UNAVAILABLE},
	}
	for _, tc := range cases {
		t.Run(tc.product+"/"+tc.kind, func(t *testing.T) {
			a := &Adapter{dialect: tc.product}
			program := &luaengine.Program{}
			native := &plan{index: "records", id: "item", action: "index", program: program}
			raw := fmt.Sprintf(`{"_index":"records","_id":"item","error":{"type":%q},"status":%d}`, tc.kind, tc.status)
			result := testBulkMutationReply(a, native, raw)
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != tc.failure {
				t.Fatalf("rejection was not recognized: result=%v", result)
			}
		})
	}
	// Lua create conflicts must retain CONFLICT so the caller can reevaluate.
	a := &Adapter{dialect: ElasticsearchProduct}
	program := &luaengine.Program{}
	native := &plan{index: "records", id: "item", action: "create", program: program}
	raw := `{"_index":"records","_id":"item","error":{"type":"version_conflict_engine_exception"},"status":409}`
	result := testBulkMutationReply(a, native, raw)
	if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != pb.FailureCode_CONFLICT {
		t.Fatalf("Lua create conflict did not allow reevaluation: %v", result)
	}
}

func TestProgramWriteReplyRequiresCompleteSuccessEvidence(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	success := `{"_index":"records","_id":"item","status":200,"_version":1,"_seq_no":0,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`
	cases := []struct {
		operation, result string
		status            int
	}{
		{operation: "create", result: "created", status: 201},
		{operation: "index", result: "updated", status: 200},
		{operation: "delete", result: "deleted", status: 200},
	}
	for _, tc := range cases {
		program := &luaengine.Program{}
		native := &plan{index: "records", id: "item", action: tc.operation, program: program}
		raw := strings.Replace(success, `"updated"`, fmt.Sprintf("%q", tc.result), 1)
		raw = strings.Replace(raw, `"status":200`, fmt.Sprintf(`"status":%d`, tc.status), 1)
		result := testBulkMutationReply(a, native, raw)
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
			t.Fatalf("success was not recognized: %v", result)
		}
		wrongStatus := strings.Replace(raw, fmt.Sprintf(`"status":%d`, tc.status), `"status":202`, 1)
		if result := testBulkMutationReply(a, native, wrongStatus); result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatalf("unexpected status was accepted: %s: %v", wrongStatus, result)
		}
	}
	invalid := []string{
		`{}`,
		success[:len(success)-1],
		strings.Replace(success, `"_index":"records"`, `"_index":"other"`, 1),
		strings.Replace(success, `"_id":"item"`, `"_id":"other"`, 1),
		strings.Replace(success, `"_seq_no":0,`, "", 1),
		strings.Replace(success, `"_version":1,`, "", 1),
		strings.Replace(success, `"_version":1`, `"_version":0`, 1),
		strings.Replace(success, `"updated"`, `"created"`, 1),
		strings.Replace(strings.Replace(success, `"updated"`, `"created"`, 1), `"status":200`, `"status":201`, 1),
		`{"_index":"records","_id":"item","error":{"type":"mapper_parsing_exception"},"status":200}`,
		`{"_index":"records","_id":"item","error":{"type":"internal_server_error"},"status":200}`,
		`{"_index":"records","_id":"item","error":{"type":"mapper_parsing_exception"},"status":200,"result":"updated"}`,
	}
	program := &luaengine.Program{}
	native := &plan{index: "records", id: "item", action: "index", program: program}
	for _, raw := range invalid {
		result := testBulkMutationReply(a, native, raw)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatalf("ambiguous response was accepted: %s: %v", raw, result)
		}
	}
	native.action = "delete"
	raw := strings.Replace(strings.Replace(success, `"updated"`, `"not_found"`, 1), `"status":200`, `"status":404`, 1)
	if result := testBulkMutationReply(a, native, raw); result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
		t.Fatalf("conditional delete accepted an absent record: %v", result)
	}
}

func TestProgramWriteReplyRejectsContradictoryErrorEvidence(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	program := &luaengine.Program{}
	native := &plan{index: "records", id: "item", action: "index", program: program}
	for _, field := range []string{`"result":"updated"`, `"_version":1`, `"_seq_no":1`, `"_primary_term":1`, `"_shards":{}`} {
		raw := fmt.Sprintf(`{"_index":"records","_id":"item","error":{"type":"mapper_parsing_exception"},"status":400,%s}`, field)
		result := testBulkMutationReply(a, native, raw)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatalf("contradictory response was accepted: %s: %v", raw, result)
		}
	}
}

func TestBulkTransformItemLimitsPreserveOtherAcknowledgements(t *testing.T) {
	success := `{"_index":"records","_id":"item","status":200,"_version":1,"_seq_no":0,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`
	for _, action := range []string{"index", "expression"} {
		for _, limit := range []string{"bytes", "nodes"} {
			t.Run(action+"/"+limit, func(t *testing.T) {
				a := &Adapter{dialect: ElasticsearchProduct}
				ordinary := &plan{index: "records", id: "item", action: "index"}
				native := &plan{index: "records", id: "item", action: action}
				message := "update acknowledgement unavailable or incomplete"
				if action == "index" {
					native.program = &luaengine.Program{}
					message = "conditional write acknowledgement unavailable or incomplete"
				}
				ordinaryWork := &execution.Plan{Backend: ordinary}
				transformWork := &execution.Plan{Backend: native}
				extra := `[` + strings.Repeat("0,", 4096) + `0]`
				if limit == "bytes" {
					extra = `"` + strings.Repeat("x", metadataLimit) + `"`
				}
				item := success[:len(success)-1] + `,"extra":` + extra + `}`
				body := []byte(fmt.Sprintf(`{"errors":false,"took":1,"items":[{"index":%s},{%q:%s}]}`, success, native.bulkAction(), item))
				if len(body) > responseLimit || validateJSON(body, 16384) != nil {
					t.Fatal("fixture exceeds whole-batch limits")
				}
				results := a.bulkResults([]*execution.Plan{ordinaryWork, transformWork}, 200, body, nil)
				if results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Fatal("bounded ordinary acknowledgement lost", results[0])
				}
				result := results[1].GetMutationResult()
				if limit == "nodes" {
					if result.GetOutcome() != pb.MutationOutcome_APPLIED {
						t.Fatal("valid reply rejected by hidden node cap", result)
					}
					return
				}
				if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetMessage() != message {
					t.Fatal("excessive transform evidence trusted", result)
				}
			})
		}
	}
}

func TestBulkValidatesAllItemsBeforeAcknowledgingAny(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	ordinary := &plan{index: "records", id: "item", action: "index"}
	program := &luaengine.Program{}
	native := &plan{index: "records", id: "item", action: "index", program: program}
	ordinaryWork := &execution.Plan{Backend: ordinary}
	programWork := &execution.Plan{Backend: native}
	success := `{"_index":"records","_id":"item","status":200,"_version":1,"_seq_no":0,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`
	for _, tail := range []string{
		`{"delete":` + success + `}`,
		`{"index":` + strings.Replace(success, `"_id":"item"`, `"_id":"other"`, 1) + `}`,
		`{"index":` + strings.Replace(success, `"_index":"records"`, `"_index":"other"`, 1) + `}`,
		`{"index":` + success + `,"delete":` + success + `}`,
		`{"index":{"_index":"records","_id":"item","status":409,"error":{"type":"version_conflict_engine_exception"}}}`,
	} {
		body := []byte(`{"errors":false,"took":1,"items":[{"index":` + success + `},` + tail + `]}`)
		results := a.bulkResults([]*execution.Plan{ordinaryWork, programWork}, 200, body, nil)
		for _, result := range results {
			if result.GetMutationResult().GetOutcome() != pb.MutationOutcome_UNKNOWN {
				t.Fatalf("acknowledgement escaped batch validation: %s: %v", tail, results)
			}
		}
	}
}
