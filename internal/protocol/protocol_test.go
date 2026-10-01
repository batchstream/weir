package protocol

import (
	"bytes"
	"strings"
	"testing"
	"unicode/utf8"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
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
	v := &pb.Operation_Read{Read: req}
	op := &pb.Operation{Operation: v}
	if f := Validate(op, "mongo"); f == nil || f.Code != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal(f)
	}
	program := &pb.ProgramTransform{Runtime: "unqualified", Source: []byte("return weir.keep()")}
	programForm := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: programForm}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	m := &pb.MutateRequest{Resource: "weir://mongo/db/c/s:a", Action: action}
	mv := &pb.Operation_Mutate{Mutate: m}
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
	variant := &pb.Operation_Mutate{Mutate: mutation}
	op := &pb.Operation{Operation: variant}
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

func TestRouteEnvelopeIDsDestinationsAndFragments(t *testing.T) {
	request := &pb.Request{Id: 9, Destination: "records", Payload: []byte{1}}
	if err := ValidateRequest(request, "records", 2); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.Request){
		func(r *pb.Request) { r.Id = 0 }, func(r *pb.Request) { r.Id = 2 }, func(r *pb.Request) { r.Destination = "other" }, func(r *pb.Request) { r.Payload = nil }, func(r *pb.Request) { r.ProtoReflect().SetUnknown([]byte{0x20, 1}) },
	} {
		copied := proto.Clone(request).(*pb.Request)
		mutate(copied)
		if err := ValidateRequest(copied, "records", 2); err == nil {
			t.Fatal("accepted invalid route envelope", copied)
		}
	}
	chunk := &pb.Response{Id: 9, Payload: []byte{1}}
	end := &pb.Response{Id: 9, End: true}
	for _, response := range []*pb.Response{chunk, end} {
		if err := ValidateResponse(response); err != nil {
			t.Fatal(err)
		}
	}
	for _, response := range []*pb.Response{{Id: 9}, {Id: 9, End: true, Payload: []byte{1}}, {Id: 0, End: true}, {Id: 9, Payload: make([]byte, NativeChunk+1)}} {
		if err := ValidateResponse(response); err == nil {
			t.Fatal("accepted invalid response")
		}
	}
}

func TestCallVersionUnknownFieldsAndRelativeTarget(t *testing.T) {
	request := &pb.ReadRequest{Resource: "data/s:key"}
	value := &pb.Call_Read{Read: request}
	call := &pb.Call{Version: 1, Operation: value}
	encode := func(c *pb.Call) []byte {
		data, err := proto.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	if _, err := DecodeCall(encode(call)); err != nil {
		t.Fatal(err)
	}
	for _, mutate := range []func(*pb.Call){func(c *pb.Call) { c.Version = 2 }, func(c *pb.Call) { c.Operation = nil }, func(c *pb.Call) { c.GetRead().Resource = "weir://records/data/s:key" }, func(c *pb.Call) { c.GetRead().Resource = "/data/s:key" }, func(c *pb.Call) { c.GetRead().Resource = "data/%6B" }, func(c *pb.Call) { c.GetRead().ProtoReflect().SetUnknown([]byte{0x20, 1}) }} {
		copied := proto.Clone(call).(*pb.Call)
		mutate(copied)
		if _, err := DecodeCall(encode(copied)); err == nil {
			t.Fatal("accepted invalid call", copied)
		}
	}
	if _, err := DecodeCall([]byte{0xff}); err == nil {
		t.Fatal("malformed encoding accepted")
	}
}

func TestEventValidationAndBoundedEncoding(t *testing.T) {
	document := &pb.Document{MediaType: "application/octet-stream", Data: make([]byte, MaxDocument)}
	read := ReadDocument(document)
	value := &pb.Result_Read{Read: read}
	result := &pb.Result{Index: 4, Result: value}
	event := &pb.Event{Version: 1, Value: &pb.Event_Result{Result: result}}
	encoded, err := MarshalEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	decoded := &pb.Event{}
	reader := bytes.NewBuffer(encoded)
	if err := protodelim.UnmarshalFrom(reader, decoded); err != nil || reader.Len() != 0 || !proto.Equal(event, decoded) {
		t.Fatal("not one exact delimited event", err, reader.Len())
	}
	if err := ValidateEvent(decoded); err != nil {
		t.Fatal(err)
	}
	failure := Fail(pb.FailureCode_INTERNAL, strings.Repeat("中", 600))
	if len(failure.Message) > 1024 || !utf8.ValidString(failure.Message) {
		t.Fatal("failure envelope not bounded UTF8")
	}
	invalid := []*pb.Event{
		{Version: 1, Value: &pb.Event_Result{Result: &pb.Result{Index: 4}}},
		{Version: 1, Value: &pb.Event_Document{Document: &pb.Document{MediaType: "invalid"}}},
		{Version: 1, Value: &pb.Event_Chunk{Chunk: make([]byte, NativeChunk+1)}},
		{Version: 1, Value: &pb.Event_NativeEnd{NativeEnd: &pb.NativeEnd{Completion: pb.NativeCompletion(99)}}},
		{Version: 1, Value: &pb.Event_NativeEnd{NativeEnd: &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_INCOMPLETE}}},
	}
	for _, event := range invalid {
		if err := ValidateEvent(event); err == nil {
			t.Fatal("accepted invalid event")
		}
	}
}
