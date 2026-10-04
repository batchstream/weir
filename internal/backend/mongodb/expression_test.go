package mongodb

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func expressionOperation(resource string, raw []byte) *pb.ExecuteRequest {
	doc := &pb.Document{ContentType: ExpressionContentType, Data: raw}
	form := &pb.Transform_BackendExpression{BackendExpression: doc}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	req := &pb.MutateRequest{Resource: resource, Action: action}
	opOperation := &pb.Command_Mutate{Mutate: req}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	return op
}

func expressionBSON(t testing.TB, doc bson.D) []byte {
	t.Helper()
	raw, err := bson.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMongoExpressionValidation(t *testing.T) {
	cfg := Config{Store: "mongo"}
	a := &Adapter{config: cfg}
	decimal, _ := bson.ParseDecimal128("1.25")
	nonfinite, _ := bson.ParseDecimal128("Infinity")
	cases := []struct {
		name  string
		doc   bson.D
		valid bool
	}{
		{"set", bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: bson.D{{Key: "$literal", Value: "data"}}}}}}, true},
		{"empty operand", bson.D{{Key: "$set", Value: bson.D{}}}, true},
		{"unset", bson.D{{Key: "$unset", Value: bson.D{{Key: "a", Value: nil}}}}, true},
		{"numbers", bson.D{{Key: "$inc", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int64(9007199254740993)}, {Key: "c", Value: 1.25}, {Key: "d", Value: decimal}}}}, true},
		{"empty", bson.D{}, false},
		{"decimal infinity", bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: nonfinite}}}}, false},
		{"invalid UTF8", bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: string([]byte{255})}}}}, false},
		{"invalid regex UTF8", bson.D{{Key: "$set", Value: bson.D{{Key: "n", Value: bson.Regex{Pattern: string([]byte{255})}}}}}, false},
		{"identity", bson.D{{Key: "$set", Value: bson.D{{Key: "_id.x", Value: 1}}}}, false},
		{"duplicate operator", bson.D{{Key: "$set", Value: bson.D{}}, {Key: "$set", Value: bson.D{}}}, false},
		{"duplicate data", bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: bson.D{{Key: "x", Value: 1}, {Key: "x", Value: 2}}}}}}, false},
		{"conflict", bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}, {Key: "a-b", Value: 2}}}, {Key: "$unset", Value: bson.D{{Key: "a.b", Value: ""}}}}, false},
		{"same path", bson.D{{Key: "$set", Value: bson.D{{Key: "a", Value: 1}}}, {Key: "$inc", Value: bson.D{{Key: "a", Value: 1}}}}, false},
		{"operator", bson.D{{Key: "$rename", Value: bson.D{{Key: "a", Value: "b"}}}}, false},
		{"wrong operand", bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: nil}}}}, false},
		{"nan", bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: math.NaN()}}}}, false},
		{"infinity", bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: math.Inf(1)}}}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := expressionBSON(t, tc.doc)
			op := expressionOperation("db/records/s:a", raw)
			p, f := prepareTestRecord(a, op)
			if (f == nil) != tc.valid {
				t.Fatal(f)
			}
			if p != nil && (p.ResultBytes != execution.ResultOverheadBytes || !bytes.Equal(p.Backend.(*plan).document, raw)) {
				t.Fatal("plan/byte fidelity", p)
			}
		})
	}
	for _, path := range []string{"_id", "a..b", ".a", "a.", "a.$", "a.$[]", "a.$[x]", "a.01", strings.Repeat("a", 1025), strings.Repeat("a.", 33) + "b"} {
		doc := bson.D{{Key: "$set", Value: bson.D{{Key: path, Value: 1}}}}
		if f := a.prepareExpression(expressionOperation("x", expressionBSON(t, doc)).Command.GetMutate().GetAtomicTransform().GetBackendExpression()); f == nil {
			t.Fatal(path)
		}
	}
	nested := bson.D{{Key: "x", Value: 1}}
	for i := 0; i < 33; i++ {
		nested = bson.D{{Key: "x", Value: nested}}
	}
	doc := bson.D{{Key: "$set", Value: nested}}
	op := expressionOperation("db/records/s:a", expressionBSON(t, doc))
	if _, f := prepareTestRecord(a, op); f == nil {
		t.Fatal("depth")
	}
	doc = bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: strings.Repeat("x", protocol.MaxExpression)}}}}
	op = expressionOperation("db/records/s:a", expressionBSON(t, doc))
	if _, f := prepareTestRecord(a, op); f == nil {
		t.Fatal("bytes")
	}
	doc = bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}
	op = expressionOperation("db/records/s:a", expressionBSON(t, doc))
	op.Command.GetMutate().GetAtomicTransform().GetBackendExpression().ContentType = "application/unknown"
	if _, f := prepareTestRecord(a, op); f.GetCode() != pb.FailureCode_UNSUPPORTED {
		t.Fatal("unknown profile", f)
	}
	op.Command.GetMutate().GetAtomicTransform().GetBackendExpression().ContentType = ExpressionContentType
	many := make(bson.D, 129)
	for i := range many {
		many[i] = bson.E{Key: fmt.Sprintf("field%d", i), Value: 1}
	}
	excessive := bson.D{{Key: "$set", Value: many}}
	if _, f := prepareTestRecord(a, expressionOperation("db/records/s:a", expressionBSON(t, excessive))); f == nil {
		t.Fatal("path count")
	}
	nodes := make(bson.A, 4096)
	for i := range nodes {
		nodes[i] = nil
	}
	excessive = bson.D{{Key: "$set", Value: bson.D{{Key: "x", Value: nodes}}}}
	d := &pb.Document{ContentType: ExpressionContentType, Data: expressionBSON(t, excessive)}
	if a.prepareExpression(d) == nil {
		t.Fatal("node/byte limit")
	}
}

func TestMongoExpressionEvidence(t *testing.T) {
	cases := []struct {
		name    string
		doc     bson.D
		outcome pb.MutationOutcome
	}{
		{"matched", bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}}, pb.MutationOutcome_APPLIED},
		{"noop", bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 0}}, pb.MutationOutcome_APPLIED},
		{"missing", bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "nModified", Value: 0}}, pb.MutationOutcome_NOT_APPLIED},
		{"missing n", bson.D{{Key: "ok", Value: 1}, {Key: "nModified", Value: 0}}, pb.MutationOutcome_UNKNOWN},
		{"bad ok", bson.D{{Key: "ok", Value: "bad"}}, pb.MutationOutcome_UNKNOWN},
		{"contradictory", bson.D{{Key: "ok", Value: 0}, {Key: "code", Value: 112}, {Key: "n", Value: 1}}, pb.MutationOutcome_UNKNOWN},
		{"impossible", bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 0}, {Key: "nModified", Value: 1}}, pb.MutationOutcome_UNKNOWN},
		{"concern", bson.D{{Key: "ok", Value: 1}, {Key: "n", Value: 1}, {Key: "nModified", Value: 1}, {Key: "writeConcernError", Value: bson.D{{Key: "code", Value: 64}}}}, pb.MutationOutcome_UNKNOWN},
		{"conflict", bson.D{{Key: "ok", Value: 0}, {Key: "n", Value: 0}, {Key: "code", Value: 112}}, pb.MutationOutcome_NOT_APPLIED},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fields, err := scanFields(expressionBSON(t, tc.doc))
			if err != nil {
				t.Fatal(err)
			}
			item := &plan{action: "expression"}
			r := bulkItemReply(item, fields, fields["writeConcernError"].Type != 0)
			if r.Outcome != tc.outcome {
				t.Fatal(r)
			}
		})
	}
}

func FuzzMongoExpression(f *testing.F) {
	f.Add([]byte{5, 0, 0, 0, 0})
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > protocol.MaxExpression {
			return
		}
		a := &Adapter{}
		d := &pb.Document{ContentType: ExpressionContentType, Data: raw}
		_ = a.prepareExpression(d)
		fields, err := scanFields(raw)
		if err == nil {
			item := &plan{action: "expression"}
			_ = bulkItemReply(item, fields, false)
		}
	})
}
