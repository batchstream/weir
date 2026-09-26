package protocol

import (
	pb "github.com/batchstream/weir/api/weir/v1"
	"strings"
	"testing"
)

func TestCanonicalURI(t *testing.T) {
	valid := []string{"weir://mongo", "weir://mongo/db/c/s:a%2Fb", "weir://a-b/a/s:%E4%B8%AD", "weir://m/a/i:-3"}
	for _, s := range valid {
		if _, _, err := ParseResource(s); err != nil {
			t.Errorf("%s: %v", s, err)
		}
	}
	invalid := []string{"weir://mongo/", "WEIR://mongo", "weir://mongo:42/x", "weir://m/a//b", "weir://m/a/..", "weir://m/a/%61", "weir://m/a/%2f", "weir://m/a/%FF", "weir://m/a/%00", "weir://a--b/x", "weir://m/x?q=a", "weir://m/x#f", "weir://" + strings.Repeat("a", 64)}
	for _, s := range invalid {
		if _, _, err := ParseResource(s); err == nil {
			t.Errorf("accepted %q", s)
		}
	}
}
func TestValidationAndUnsupported(t *testing.T) {
	req := &pb.ReadRequest{Resource: "weir://other/db/c/s:a"}
	v := &pb.BulkOperation_Read{Read: req}
	op := &pb.BulkOperation{Operation: v}
	if f := Validate(op, "mongo"); f == nil || f.Code != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal(f)
	}
	program := &pb.ProgramTransform{Runtime: "unqualified"}
	programForm := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: programForm}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	m := &pb.MutateRequest{Resource: "weir://mongo/db/c/s:a", Action: action}
	mv := &pb.BulkOperation_Mutate{Mutate: m}
	op.Operation = mv
	if f := Validate(op, "mongo"); f == nil || f.Code != pb.FailureCode_UNSUPPORTED {
		t.Fatal(f)
	}
}
func FuzzResource(f *testing.F) {
	f.Add("weir://mongo/db/c/s:a%2Fb")
	f.Fuzz(func(t *testing.T, s string) { _, _, _ = ParseResource(s) })
}

func TestExpressionWireBoundaryIsOpaque(t *testing.T) {
	doc := &pb.Document{MediaType: "application/unknown", Data: []byte("not BSON or JSON")}
	form := &pb.Transform_BackendExpression{BackendExpression: doc}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "weir://mongo/db/c/s:a", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	op := &pb.BulkOperation{Operation: variant}
	if f := Validate(op, "mongo"); f != nil {
		t.Fatal("Core interpreted opaque expression", f)
	}
	for _, raw := range [][]byte{nil, make([]byte, MaxExpression+1)} {
		doc.Data = raw
		if f := Validate(op, "mongo"); f.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
			t.Fatal(f)
		}
	}
	transform.Form = nil
	if f := Validate(op, "mongo"); f.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal(f)
	}
}
