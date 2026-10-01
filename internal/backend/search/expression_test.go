package search

import (
	"fmt"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
)

func expressionOperation(resource, raw string) *pb.BulkOperation {
	doc := &pb.Document{MediaType: ExpressionMedia, Data: []byte(raw)}
	form := &pb.Transform_BackendExpression{BackendExpression: doc}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	req := &pb.MutateRequest{Resource: resource, Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: req}
	op := &pb.BulkOperation{Operation: variant}
	return op
}

func TestSearchExpressionValidation(t *testing.T) {
	cfg := Config{Store: "search"}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg}
	allowed := []string{`{"doc":{}}`, `{"doc":{"n":9223372036854775807,"null":null,"array":[1,{"x":true}],"data":{"$set":"literal"}}}`}
	for _, raw := range allowed {
		p, f := a.Prepare(expressionOperation("weir://search/records/s:a", raw))
		if f != nil || string(p.Backend.(*plan).source) != raw {
			t.Fatal(p, f)
		}
	}
	denied := []string{"", `{}`, `[]`, `{"doc":null}`, `{"doc":[]}`, `{"doc":{},"doc":{}}`, `{"doc":{"x":1,"x":2}}`, `{"doc":{"x":{"a":1,"a":2}}}`, `{"doc":{"_id":"other"}}`, `{"doc":{"a":{},"a.b":1}}`, `{"doc":{"bad":"\ud800"}}`, `{"doc":{"x":` + strings.Repeat("[", 34) + `0` + strings.Repeat("]", 34) + `}}`, `{"doc":{"x":"` + strings.Repeat("x", protocol.MaxExpression) + `"}}`, `{"doc":{"x":[` + strings.Repeat("0,", 4096) + `0]}}`}
	for _, option := range []string{"script", "upsert", "doc_as_upsert", "scripted_upsert", "routing", "_index", "retry_on_conflict", "pipeline", "query", "detect_noop", "timestamp", "_source"} {
		denied = append(denied, fmt.Sprintf(`{"doc":{},%q:{}}`, option))
	}
	for _, raw := range denied {
		if _, f := a.Prepare(expressionOperation("weir://search/records/s:a", raw)); f == nil {
			t.Fatal("accepted", raw)
		}
	}
	op := expressionOperation("weir://search/records/s:a", `{"doc":{}}`)
	op.GetMutate().GetAtomicTransform().GetBackendExpression().MediaType = "application/unknown"
	if _, f := a.Prepare(op); f.GetCode() != pb.FailureCode_UNSUPPORTED {
		t.Fatal(f)
	}
}

func TestSearchExpressionEvidence(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	n := &plan{index: "records", id: "a"}
	success := `{"_index":"records","_id":"a","_version":1,"_seq_no":0,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`
	noop := strings.ReplaceAll(strings.ReplaceAll(success, `"updated"`, `"noop"`), `:1,"failed"`, `:0,"failed"`)
	noop = strings.ReplaceAll(noop, `"total":1`, `"total":0`)
	for _, raw := range []string{success, noop} {
		opts := expressionReplyOptions{native: n, status: 200, raw: []byte(raw)}
		r, _ := a.expressionReply(opts)
		if r.Outcome != pb.MutationOutcome_APPLIED {
			t.Fatal(raw, r)
		}
	}
	for _, raw := range []string{`{}`, success[:len(success)-1], strings.ReplaceAll(success, `"_id":"a"`, `"_id":"b"`), strings.ReplaceAll(success, `"_version":1,`, ``), strings.ReplaceAll(success, `"successful":1,`, ``), strings.ReplaceAll(success, `"updated"`, `"created"`), strings.ReplaceAll(success, `"failed":0`, `"failed":null`), strings.ReplaceAll(success, `"failed":0`, `"failed":0,"failed":0`), strings.ReplaceAll(success, `"total":1,"successful":1,"failed":0`, `"total":1,"successful":9223372036854775807,"failed":9223372036854775807`)} {
		opts := expressionReplyOptions{native: n, status: 200, raw: []byte(raw)}
		r, _ := a.expressionReply(opts)
		if r.Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal(raw, r)
		}
	}
	contradictory := []byte(`{"status":409,"error":{"type":"version_conflict_engine_exception"},"result":"updated","_seq_no":1}`)
	opts := expressionReplyOptions{native: n, status: 409, raw: contradictory}
	r, _ := a.expressionReply(opts)
	if r.Outcome != pb.MutationOutcome_UNKNOWN {
		t.Fatal("contradictory evidence", r)
	}
	cases := []struct {
		status int
		kind   string
		code   pb.FailureCode
	}{
		{404, "document_missing_exception", pb.FailureCode_PRECONDITION_FAILED},
		{409, "version_conflict_engine_exception", pb.FailureCode_CONFLICT},
		{400, "mapper_parsing_exception", pb.FailureCode_PRECONDITION_FAILED},
		{429, "es_rejected_execution_exception", pb.FailureCode_UNAVAILABLE},
	}
	for _, tc := range cases {
		raw := []byte(fmt.Sprintf(`{"status":%d,"error":{"type":%q}}`, tc.status, tc.kind))
		opts := expressionReplyOptions{native: n, status: tc.status, raw: raw}
		r, _ := a.expressionReply(opts)
		if r.Outcome != pb.MutationOutcome_NOT_APPLIED || r.GetFailure().GetCode() != tc.code {
			t.Fatal(r)
		}
	}
}

func FuzzSearchExpression(f *testing.F) {
	f.Add([]byte(`{"doc":{"n":9007199254740993}}`))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > protocol.MaxExpression {
			return
		}
		d := &pb.Document{MediaType: ExpressionMedia, Data: raw}
		_ = prepareExpression(d)
	})
}
