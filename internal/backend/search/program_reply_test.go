package search

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestProgramWriteReplyRecognizesRejectionEnvelopes(t *testing.T) {
	cases := []struct {
		profile  string
		status   int
		kind     string
		failure  pb.FailureCode
		feedback execution.Feedback
	}{
		{profile: ElasticsearchProfile, status: 400, kind: "mapper_parsing_exception", failure: pb.FailureCode_PRECONDITION_FAILED},
		{profile: ElasticsearchProfile, status: 400, kind: "document_parsing_exception", failure: pb.FailureCode_PRECONDITION_FAILED},
		{profile: ElasticsearchProfile, status: 404, kind: "index_not_found_exception", failure: pb.FailureCode_NOT_FOUND},
		{profile: ElasticsearchProfile, status: 409, kind: "version_conflict_engine_exception", failure: pb.FailureCode_CONFLICT},
		{profile: ElasticsearchProfile, status: 429, kind: "es_rejected_execution_exception", failure: pb.FailureCode_UNAVAILABLE, feedback: execution.Congested},
		{profile: OpenSearchProfile, status: 429, kind: "rejected_execution_exception", failure: pb.FailureCode_UNAVAILABLE, feedback: execution.Congested},
		{profile: ElasticsearchProfile, status: 503, kind: "unavailable_shards_exception", failure: pb.FailureCode_UNAVAILABLE, feedback: execution.Congested},
	}
	for _, tc := range cases {
		t.Run(tc.profile+"/"+tc.kind, func(t *testing.T) {
			config := Config{Index: "records", Profile: tc.profile}
			a := &Adapter{config: config}
			raw := []byte(fmt.Sprintf(`{"error":{"type":%q},"status":%d}`, tc.kind, tc.status))
			opts := programWriteReplyOptions{id: "item", expectedResult: "updated", status: tc.status, raw: raw}
			result, feedback := a.programWriteReply(opts)
			if result.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || result.GetFailure().GetCode() != tc.failure || feedback != tc.feedback {
				t.Fatalf("rejection was not recognized: result=%v feedback=%v", result, feedback)
			}
		})
	}
}

func TestProgramWriteReplyRequiresCompleteSuccessEvidence(t *testing.T) {
	config := Config{Index: "records", Profile: ElasticsearchProfile}
	a := &Adapter{config: config}
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
		opts := programWriteReplyOptions{id: "item", expectedResult: tc.action, status: tc.status, raw: raw}
		result, feedback := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil || feedback != execution.Healthy {
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
		opts := programWriteReplyOptions{id: "item", expectedResult: "updated", status: 200, raw: []byte(raw)}
		result, feedback := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
			t.Fatalf("ambiguous response was accepted: %s: %v", raw, result)
		}
	}
	opts := programWriteReplyOptions{id: "item", expectedResult: "updated", status: 200, raw: []byte(success), err: errTimeout}
	result, feedback := a.programWriteReply(opts)
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
		t.Fatalf("lost acknowledgement was accepted: %v", result)
	}
}

func TestProgramWriteReplyRejectsContradictoryErrorEvidence(t *testing.T) {
	config := Config{Index: "records", Profile: ElasticsearchProfile}
	a := &Adapter{config: config}
	for _, field := range []string{`"result":"updated"`, `"_version":1`, `"_seq_no":1`, `"_primary_term":1`, `"_shards":{}`} {
		raw := []byte(fmt.Sprintf(`{"error":{"type":"mapper_parsing_exception"},"status":400,%s}`, field))
		opts := programWriteReplyOptions{id: "item", expectedResult: "updated", status: 400, raw: raw}
		result, feedback := a.programWriteReply(opts)
		if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || feedback != execution.Neutral {
			t.Fatalf("contradictory response was accepted: %s: %v", raw, result)
		}
	}
}
