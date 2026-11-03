package search

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/backend"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestJSONBoundsAndExactNumbers(t *testing.T) {
	for _, raw := range []string{`{"n":9223372036854775807}`, `{"n":1e400}`, `{"unicode":"\ud83d\ude00"}`, `{"literal":"\\ud800"}`, `{"a":[true,null,{},[]]}`} {
		if !validJSON([]byte(raw)) {
			t.Fatal("rejected valid JSON", raw)
		}
	}
	for _, raw := range []string{`{"a":1,"a":2}`, `{"a":1,"\u0061":2}`, `{} {}`, `{"n":NaN}`, `{"unicode":"\ud800"}`, `{"unicode":"\udc00"}`, `{"a":"` + string([]byte{255}) + `"}`} {
		if validJSON([]byte(raw)) {
			t.Fatal("accepted invalid JSON", raw)
		}
	}
	if !validJSON([]byte(`[1,2,3,4]`)) {
		t.Fatal("valid array rejected")
	}
}
func TestSearchPrepareRejectsUnsupportedInputs(t *testing.T) {
	cfg := Config{Store: "search"}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg}
	for _, resource := range []string{"weir://other/records/s:a", "records/i:1", "records/s:", "Records/s:a", "records/s:a?routing=x"} {
		read := &pb.ReadRequest{Resource: resource}
		opOperation := &pb.Command_Read{Read: read}
		opCommand := &pb.Command{Operation: opOperation}
		op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
		if _, failure := prepareTestRecord(a, op); failure == nil {
			t.Fatal("unsupported URI accepted", resource)
		}
	}
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":9223372036854775807}`)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	opOperation := &pb.Command_Mutate{Mutate: mutation}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	work, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	if string(work.Backend.(*plan).source) != string(document.Data) {
		t.Fatal("source modified")
	}
	// An empty source would leave a bulk action line without its source line.
	empty := &pb.Document{ContentType: "application/json"}
	writes := map[string]func(){
		"put":     func() { mutation.Action = &pb.MutateRequest_Put{Put: empty} },
		"create":  func() { mutation.Action = &pb.MutateRequest_Create{Create: empty} },
		"replace": func() { mutation.Action = &pb.MutateRequest_Replace{Replace: empty} },
	}
	for name, write := range writes {
		write()
		if _, failure := prepareTestRecord(a, op); failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT {
			t.Fatal(name, "without a JSON object source accepted", failure)
		}
	}
	deletion := &pb.MutateRequest_Delete{Delete: &pb.Empty{}}
	mutation.Action = deletion
	if _, failure := prepareTestRecord(a, op); failure != nil {
		t.Fatal("Delete without a source rejected", failure)
	}
	program := &pb.LuaTransform{Source: []byte("return function(current, incoming) return weir.keep() end")}
	programForm := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: programForm}
	mutation.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	if _, failure := prepareTestRecord(a, op); failure != nil {
		t.Fatal("built-in Lua program transform rejected", failure)
	}
}
func TestResponseAndRequestLimits(t *testing.T) {
	for _, mode := range []string{"large_body", "large_header", "malformed", "deep", "encoding"} {
		t.Run(mode, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "large_body":
					_, _ = fmt.Fprint(w, `{"x":"`+strings.Repeat("x", (256<<10))+`"}`)
				case "large_header":
					w.Header().Set("X-Large", strings.Repeat("x", 64<<10))
					_, _ = fmt.Fprint(w, `{}`)
				case "malformed":
					_, _ = fmt.Fprint(w, `{"unfinished":`)
				case "deep":
					_, _ = fmt.Fprint(w, strings.Repeat("[", 10001)+"0"+strings.Repeat("]", 10001))
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = fmt.Fprint(w, `{}`)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			transport := newTransport(time.Second, backend.DefaultOptions().IdleTimeout)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			cfg := Config{URL: server.URL}
			a := &Adapter{dialect: ElasticsearchProduct, config: cfg, client: client, nativeClient: client, ctx: ctx, cancel: cancel}
			defer a.Close(context.Background())
			call := exchange{path: "/"}
			_, _, err := a.request(ctx, call)
			valid := mode == "large_body" || mode == "large_header"
			if (err == nil) != valid {
				t.Fatal("response framing or data size", mode, err)
			}
		})
	}
}
func TestBulkEvidenceIsNotHTTPStatus(t *testing.T) {
	cfg := Config{Store: "search"}
	a := &Adapter{dialect: ElasticsearchProduct, config: cfg}
	empty := &pb.Empty{}
	action := &pb.MutateRequest_Delete{Delete: empty}
	mutation := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	opOperation := &pb.Command_Mutate{Mutate: mutation}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	work, failure := prepareTestRecord(a, op)
	if failure != nil {
		t.Fatal(failure)
	}
	for _, raw := range []string{`{}`, `{"errors":false,"took":0,"items":[]}`, `{"errors":false,"took":0,"items":[{"delete":{"_index":"records","_id":"a","status":404}}]}`, `{"errors":false,"took":0,"items":[{"delete":{"_index":"records","_id":"other","status":200}}]}`} {
		results, _ := a.bulkResults([]*execution.Plan{work}, 200, []byte(raw), nil)
		if results[0].GetMutationResult().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal("invented acknowledgement", raw)
		}
	}
}

func TestNativeErrorStatusAndPositiveAcknowledgement(t *testing.T) {
	a := &Adapter{dialect: ElasticsearchProduct}
	for _, code := range []int{200, 400, 404, 429, 500} {
		failure := a.reject("version_conflict_engine_exception", code)
		if failure != nil {
			t.Fatal("mismatched error/status treated as definite", code)
		}
	}
	empty := &pb.Empty{}
	action := &pb.MutateRequest_Delete{Delete: empty}
	mutation := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	opOperation := &pb.Command_Mutate{Mutate: mutation}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	native := &plan{index: "records", id: "a", action: "delete"}
	work := &execution.Plan{Command: op.Command, Backend: native}
	raw := []byte(`{"errors":false,"took":1,"items":[{"delete":{"_index":"records","_id":"a","status":200,"result":"deleted","_seq_no":1,"_primary_term":1,"_shards":{"total":2,"successful":1,"failed":1}}}]}`)
	results, _ := a.bulkResults([]*execution.Plan{work}, 200, raw, nil)
	if results[0].GetMutationResult().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("positive primary acknowledgement discarded", results)
	}
}

func TestFiniteCongestionProducts(t *testing.T) {
	for _, product := range []string{ElasticsearchProduct, OpenSearchProduct} {
		a := &Adapter{dialect: product}
		for _, name := range []string{"es_rejected_execution_exception", "rejected_execution_exception"} {
			failure := a.reject(name, 429)
			matches := product == ElasticsearchProduct && name == "es_rejected_execution_exception" || product == OpenSearchProduct && name == "rejected_execution_exception"
			if (failure != nil) != matches {
				t.Fatal(product, name, failure)
			}
		}
	}
}
