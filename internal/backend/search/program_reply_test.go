package search

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

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
			raw := []byte(fmt.Sprintf(`{"error":{"type":%q},"status":%d}`, tc.kind, tc.status))
			opts := programWriteReplyOptions{index: "records", id: "item", expectedResult: "updated", status: tc.status, raw: raw}
			result := a.programWriteReply(opts)
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != tc.failure {
				t.Fatalf("rejection was not recognized: result=%v", result)
			}
		})
	}
}

func TestProgramWriteReplyRequiresCompleteSuccessEvidence(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	success := `{"_index":"records","_id":"item","_version":1,"_seq_no":0,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`
	cases := []struct {
		action string
		status int
	}{
		{action: "created", status: 201},
		{action: "updated", status: 200},
		{action: "deleted", status: 200},
	}
	for _, tc := range cases {
		raw := []byte(strings.Replace(success, `"updated"`, fmt.Sprintf("%q", tc.action), 1))
		opts := programWriteReplyOptions{index: "records", id: "item", expectedResult: tc.action, status: tc.status, raw: raw}
		result := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
			t.Fatalf("success was not recognized: %v", result)
		}
	}
	invalid := []string{
		`{}`,
		success[:len(success)-1],
		strings.Replace(success, `"_index":"records"`, `"_index":"other"`, 1),
		strings.Replace(success, `"_id":"item"`, `"_id":"other"`, 1),
		strings.Replace(success, `"_seq_no":0,`, "", 1),
		strings.Replace(success, `"updated"`, `"created"`, 1),
		`{"error":{"type":"mapper_parsing_exception"},"status":400}`,
		`{"error":{"type":"internal_server_error"},"status":200}`,
		`{"error":{"type":"mapper_parsing_exception"},"status":200,"result":"updated"}`,
	}
	for _, raw := range invalid {
		opts := programWriteReplyOptions{index: "records", id: "item", expectedResult: "updated", status: 200, raw: []byte(raw)}
		result := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatalf("ambiguous response was accepted: %s: %v", raw, result)
		}
	}

}

func TestProgramWriteReplyRejectsContradictoryErrorEvidence(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	for _, field := range []string{`"result":"updated"`, `"_version":1`, `"_seq_no":1`, `"_primary_term":1`, `"_shards":{}`} {
		raw := []byte(fmt.Sprintf(`{"error":{"type":"mapper_parsing_exception"},"status":400,%s}`, field))
		opts := programWriteReplyOptions{index: "records", id: "item", expectedResult: "updated", status: 400, raw: raw}
		result := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN {
			t.Fatalf("contradictory response was accepted: %s: %v", raw, result)
		}
	}
}
