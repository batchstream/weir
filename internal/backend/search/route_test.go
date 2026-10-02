package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir/api/protocol"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
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
	config := Config{Store: "search", URL: server.URL, MaxReadSize: protocol.MaxDocument}
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

func TestRouteNativeBackendIOBudgetAndOutputBackpressure(t *testing.T) {
	for _, phase := range []string{"qualification", "headers", "body", "cumulative_body", "slow_output"} {
		t.Run(phase, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					if phase == "qualification" {
						<-r.Context().Done()
						return
					}
					_, _ = io.WriteString(w, testIndexReply)
					return
				}
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				switch phase {
				case "headers":
					<-r.Context().Done()
				case "body":
					_, _ = io.WriteString(w, "prefix")
					w.(http.Flusher).Flush()
					<-r.Context().Done()
				case "cumulative_body":
					w.(http.Flusher).Flush()
					for range 3 {
						timer := time.NewTimer(60 * time.Millisecond)
						select {
						case <-r.Context().Done():
							timer.Stop()
							return
						case <-timer.C:
						}
						_, _ = io.WriteString(w, "part")
						w.(http.Flusher).Flush()
					}
				case "slow_output":
					_, _ = io.WriteString(w, "complete")
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{Store: "search", URL: server.URL}
			client := server.Client()
			adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: client, nativeClient: client, ctx: context.Background()}
			open := nativeOpen(t, "records", "GET", "/_doc/x")
			open.Resource = "records"
			native := &pb.NativeCall{Open: open}
			variant := &pb.Call_Native{Native: native}
			call := &pb.Call{Version: 1, Operation: variant}
			work, failure := adapter.PrepareCall(1, call)
			if failure != nil {
				t.Fatal(failure)
			}
			work.BackendTimeout = 100 * time.Millisecond
			var end *pb.NativeEnd
			var body strings.Builder
			emit := func(_ *execution.Plan, event *pb.Event) error {
				if phase == "slow_output" && (event.GetHead() != nil || event.GetChunk() != nil) {
					time.Sleep(150 * time.Millisecond)
				}
				if chunk := event.GetChunk(); chunk != nil {
					body.Write(chunk)
				}
				if event.GetNativeEnd() != nil {
					end = event.GetNativeEnd()
				}
				return nil
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			started := time.Now()
			adapter.Execute(ctx, []*execution.Plan{work}, emit)
			elapsed := time.Since(started)
			if end == nil {
				t.Fatal("missing native completion")
			}
			if phase == "slow_output" {
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || body.String() != "complete" || elapsed < 3*work.BackendTimeout {
					t.Fatal("output backpressure consumed backend I/O budget", end, body.String(), elapsed)
				}
				return
			}
			completion := pb.NativeCompletion_RESPONSE_INCOMPLETE
			expectedCalls := int32(1)
			if phase == "qualification" {
				completion = pb.NativeCompletion_NATIVE_NOT_STARTED
				expectedCalls = 0
			}
			if end.Completion != completion || end.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || calls.Load() != expectedCalls || elapsed > 5*work.BackendTimeout {
				t.Fatal("backend phase escaped cumulative I/O deadline", phase, end, calls.Load(), elapsed)
			}
		})
	}
}
