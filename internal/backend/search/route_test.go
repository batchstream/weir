package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

func TestRouteCallRelativeTargetAndLargeRead(t *testing.T) {
	source := `{"pad":"` + strings.Repeat("x", protocol.MaxDocument-len(`{"pad":""}`)) + `"}`
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			_, _ = fmt.Fprintf(w, `{"docs":[{"_index":"records","_id":"a","found":true,"_seq_no":1,"_primary_term":1,"_source":%s}]}`, source)
		default:
			t.Errorf("unexpected unbatched request %s", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
	request := &pb.ReadRequest{Resource: "records/s:a"}
	variant := &pb.Call_Read{Read: request}
	call := &pb.Call{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCall(9, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "records/s:a" || work.Operation.Index != 9 || work.ID != 9 || work.BatchKey != "records" {
		t.Fatal("wire Call mutated or association lost", work)
	}
	events := 0
	emit := func(plan *execution.Plan, event *pb.Event) error {
		events++
		if plan != work || event.Version != 1 || event.GetResult().Index != 9 || string(event.GetResult().GetRead().GetDocument().Data) != source {
			t.Fatal("large read lost source or association")
		}
		return nil
	}
	feedback := adapter.Execute(context.Background(), []*execution.Plan{work}, emit)
	if events != 1 || feedback != execution.Healthy {
		t.Fatal("large legal record failed", events, feedback)
	}
	request.Resource = "weir://search/records/s:a"
	if _, failure := adapter.PrepareCall(10, call); failure == nil {
		t.Fatal("accepted obsolete absolute wire resource")
	}
	request.Resource = "records/s:a"
	call.Version = 2
	if _, failure := adapter.PrepareCall(10, call); failure == nil {
		t.Fatal("accepted unknown payload version")
	}
	call.Version = 1
	if _, failure := adapter.PrepareCall(0, call); failure == nil {
		t.Fatal("accepted zero ID")
	}
}

func TestRouteLuaUsesSingletonCASBoundary(t *testing.T) {
	config := Config{Store: "search"}
	adapter := &Adapter{config: config}
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.keep()`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	variant := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: variant}
	work, failure := adapter.PrepareCall(1, call)
	if failure != nil {
		t.Fatal(failure)
	}
	if !work.Singleton || work.Backend.(*plan).program == nil {
		t.Fatal("Lua escaped its bounded CAS execution")
	}
}
