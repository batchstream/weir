package searchstore

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

func TestJSONBoundsAndExactNumbers(t *testing.T) {
	for _, raw := range []string{`{"n":9223372036854775807}`, `{"n":1e400}`, `{"unicode":"\ud83d\ude00"}`, `{"literal":"\\ud800"}`, `{"a":[true,null,{},[]]}`} {
		if err := validateJSON([]byte(raw), 4096); err != nil {
			t.Fatal(raw, err)
		}
	}
	for _, raw := range []string{`{"a":1,"a":2}`, `{} {}`, `{"n":NaN}`, `{"unicode":"\ud800"}`, `{"unicode":"\udc00"}`, strings.Repeat("[", 34) + "0" + strings.Repeat("]", 34), `{"a":"` + string([]byte{255}) + `"}`} {
		if validateJSON([]byte(raw), 4096) == nil {
			t.Fatal("accepted invalid/excessive JSON", raw)
		}
	}
	if validateJSON([]byte(`[1,2,3,4]`), 4) == nil {
		t.Fatal("node bound")
	}
}
func TestSearchPrepareRejectsUnsupportedInputs(t *testing.T) {
	cfg := Config{Store: "search", Index: "records"}
	a := &Adapter{config: cfg}
	for _, resource := range []string{"weir://other/records/s:a", "weir://search/records/i:1", "weir://search/records/s:", "weir://search/alias/s:a", "weir://search/records/s:a?routing=x"} {
		read := &pb.ReadRequest{Resource: resource}
		variant := &pb.BulkOperation_Read{Read: read}
		op := &pb.BulkOperation{Operation: variant}
		if _, failure := a.Prepare(op); failure == nil {
			t.Fatal("unsupported URI accepted", resource)
		}
	}
	document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":9223372036854775807}`)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:a", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	op := &pb.BulkOperation{Operation: variant}
	work, failure := a.Prepare(op)
	if failure != nil {
		t.Fatal(failure)
	}
	if string(work.Backend.(*plan).source) != string(document.Data) {
		t.Fatal("source modified")
	}
	options := &pb.Document{MediaType: "application/json", Data: []byte(`{"routing":"x"}`)}
	mutation.AdapterOptions = options
	if _, failure := a.Prepare(op); failure == nil || failure.Code != pb.FailureCode_UNSUPPORTED {
		t.Fatal("options accepted")
	}
	mutation.AdapterOptions = nil
	transform := &pb.Transform{}
	mutation.Action = &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	if _, failure := a.Prepare(op); failure == nil || failure.Code != pb.FailureCode_UNSUPPORTED {
		t.Fatal("transform accepted")
	}
}
func TestResponseAndRequestLimits(t *testing.T) {
	for _, mode := range []string{"large_body", "large_header", "malformed", "deep", "encoding"} {
		t.Run(mode, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "large_body":
					_, _ = fmt.Fprint(w, `{"x":"`+strings.Repeat("x", metadataLimit)+`"}`)
				case "large_header":
					w.Header().Set("X-Large", strings.Repeat("x", 64<<10))
					_, _ = fmt.Fprint(w, `{}`)
				case "malformed":
					_, _ = fmt.Fprint(w, `{"unfinished":`)
				case "deep":
					_, _ = fmt.Fprint(w, strings.Repeat("[", 40)+"0"+strings.Repeat("]", 40))
				case "encoding":
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = fmt.Fprint(w, `{}`)
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			transport := newTransport(1)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
			cfg := Config{URL: server.URL}
			a := &Adapter{config: cfg, transport: transport, client: client, ctx: ctx, cancel: cancel}
			defer a.Close()
			call := exchange{path: "/", limit: metadataLimit}
			if _, _, err := a.request(ctx, call); err == nil {
				t.Fatal("invalid/excessive response accepted")
			}
		})
	}
}
func TestBulkEvidenceIsNotHTTPStatus(t *testing.T) {
	cfg := Config{Store: "search", Index: "records"}
	a := &Adapter{config: cfg}
	empty := &pb.Empty{}
	action := &pb.MutateRequest_Delete{Delete: empty}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:a", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	op := &pb.BulkOperation{Operation: variant}
	work, failure := a.Prepare(op)
	if failure != nil {
		t.Fatal(failure)
	}
	for _, raw := range []string{`{}`, `{"errors":false,"took":0,"items":[]}`, `{"errors":false,"took":0,"items":[{"delete":{"_index":"records","_id":"a","status":404}}]}`, `{"errors":false,"took":0,"items":[{"delete":{"_index":"records","_id":"other","status":200}}]}`} {
		results, _ := a.bulkResults([]*execution.Plan{work}, 200, []byte(raw), nil)
		if results[0].GetMutation().Outcome != pb.MutationOutcome_UNKNOWN {
			t.Fatal("invented acknowledgement", raw)
		}
	}
	if protocol.MaxDocument >= responseLimit {
		t.Fatal("read/framing bound relationship")
	}
}

func TestNativeErrorStatusAndPositiveAcknowledgement(t *testing.T) {
	cfg := Config{Index: "records", Profile: "elasticsearch-8.17.0"}
	a := &Adapter{config: cfg}
	for _, code := range []int{200, 400, 404, 429, 500} {
		failure, _ := a.reject("version_conflict_engine_exception", code)
		if failure != nil {
			t.Fatal("mismatched error/status treated as definite", code)
		}
	}
	empty := &pb.Empty{}
	action := &pb.MutateRequest_Delete{Delete: empty}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:a", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	op := &pb.BulkOperation{Operation: variant}
	native := &plan{id: "a", action: "delete"}
	work := &execution.Plan{Operation: op, Backend: native}
	raw := []byte(`{"errors":false,"took":1,"items":[{"delete":{"_index":"records","_id":"a","status":200,"result":"deleted","_seq_no":1,"_primary_term":1,"_shards":{"total":2,"successful":1,"failed":1}}}]}`)
	results, _ := a.bulkResults([]*execution.Plan{work}, 200, raw, nil)
	if results[0].GetMutation().Outcome != pb.MutationOutcome_APPLIED || results[0].GetMutation().GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("positive primary acknowledgement discarded", results)
	}
}

func TestFiniteCongestionProfiles(t *testing.T) {
	for _, profile := range []string{"elasticsearch-8.17.0", "opensearch-2.19.0"} {
		cfg := Config{Profile: profile}
		a := &Adapter{config: cfg}
		for _, name := range []string{"es_rejected_execution_exception", "rejected_execution_exception"} {
			failure, feedback := a.reject(name, 429)
			matches := profile == "elasticsearch-8.17.0" && name == "es_rejected_execution_exception" || profile == "opensearch-2.19.0" && name == "rejected_execution_exception"
			if (failure != nil) != matches || (feedback == execution.Congested) != matches {
				t.Fatal(profile, name, failure, feedback)
			}
		}
	}
}
