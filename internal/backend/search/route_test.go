package search

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestRouteCallRelativeTargetAndLargeRead(t *testing.T) {
	source := `{"pad":"` + strings.Repeat("x", (2<<20)-len(`{"pad":""}`)) + `"}`
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
	operation := &pb.Command_Read{Read: request}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", 9, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	if request.Resource != "records/s:a" || work.ID != 9 || work.BatchKey != "records" {
		t.Fatal("wire Command mutated or association lost", work)
	}
	events := 0
	emit := func(plan *execution.Plan, event *pb.Event) error {
		events++
		if plan != work || string(event.GetReadResult().GetDocument().Data) != source {
			t.Fatal("large read lost source or association")
		}
		return nil
	}
	adapter.Execute(context.Background(), []*execution.Plan{work}, emit)
	if events != 1 {
		t.Fatal("large legal record failed", events)
	}
	emptyRecord := &execution.Record{}
	if _, failure := adapter.PrepareRecord(context.Background(), emptyRecord); failure == nil {
		t.Fatal("accepted unconstructed record")
	}
}

func TestRouteLuaParticipatesInNativeRecordBatch(t *testing.T) {
	config := Config{Store: "search"}
	adapter := &Adapter{config: config}
	program := &pb.LuaTransform{Source: []byte(`return function(current, incoming) return weir.keep() end`)}
	form := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "records/s:a", Action: action}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	if work.Command.GetNative() != nil || work.BatchKey != "records" || work.Backend.(*plan).program == nil {
		t.Fatal("Lua cannot join its index's bounded conditional record batch")
	}
}

func TestRouteNativeReleasesHTTPWorkingSetBeforeTerminalOutput(t *testing.T) {
	for _, failChunk := range []bool{false, true} {
		name := "complete"
		if failChunk {
			name = "delivery_failure"
		}
		t.Run(name, func(t *testing.T) {
			header := strings.Repeat("h", 128<<10)
			body := strings.Repeat("b", 512<<10)
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.Header().Set("X-Large", header)
				_, _ = io.WriteString(w, body)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			config := Config{URL: server.URL}
			adapter := &Adapter{config: config, nativeClient: server.Client()}
			request := nativeRequest(t, "records", "GET", "/_search")
			variant := &pb.Command_Native{Native: request}
			call := &pb.Command{Operation: variant}
			work, failure := adapter.PrepareCommand(t.Context(), 1, call)
			if failure != nil {
				t.Fatal(failure)
			}
			chunks, terminal := 0, false
			emit := func(work *execution.Plan, event *pb.Event) error {
				if end := event.GetNativeEnd(); end != nil {
					terminal = true
					if work.WorkingBytes != execution.EntryOverheadBytes {
						t.Fatal("terminal output retained released HTTP working buffers", work.WorkingBytes)
					}
					if failChunk && end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || !failChunk && end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
						t.Fatal("working-set cleanup changed Native delivery evidence", end)
					}
					return nil
				}
				if work.WorkingBytes < 2*len(header)+(64<<10) {
					t.Fatal("HTTP output discounted still-held headers and chunk buffer", work.WorkingBytes)
				}
				if event.GetChunk() != nil {
					chunks++
					if failChunk {
						return errors.New("test output canceled")
					}
				}
				return nil
			}
			works := []*execution.Plan{work}
			adapter.Execute(t.Context(), works, emit)
			if chunks == 0 || !terminal {
				t.Fatal("Native reply did not exercise chunk and terminal ownership", chunks, terminal)
			}
		})
	}
}

func TestRouteNativeBackendIOBudgetAndOutputBackpressure(t *testing.T) {
	for _, phase := range []string{"headers", "body", "cumulative_body", "slow_output"} {
		t.Run(phase, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
			open := nativeRequest(t, "records", "GET", "/_doc/x")
			open.Resource = "records"
			native := open
			variant := &pb.Command_Native{Native: native}
			call := &pb.Command{Operation: variant}
			work, failure := adapter.PrepareCommand(context.Background(), 1, call)
			if failure != nil {
				t.Fatal(failure)
			}

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
			callerTimeout := 100 * time.Millisecond
			if phase == "slow_output" {
				callerTimeout = time.Second
			}
			ctx, cancel := context.WithTimeout(context.Background(), callerTimeout)
			defer cancel()
			started := time.Now()
			adapter.Execute(ctx, []*execution.Plan{work}, emit)
			elapsed := time.Since(started)
			if end == nil {
				t.Fatal("missing native completion")
			}
			if phase == "slow_output" {
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil || body.String() != "complete" || elapsed < 300*time.Millisecond {
					t.Fatal("output backpressure consumed backend I/O budget", end, body.String(), elapsed)
				}
				return
			}
			completion := pb.NativeCompletion_RESPONSE_INCOMPLETE
			expectedCalls := int32(1)
			if end.Completion != completion || end.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || calls.Load() != expectedCalls || elapsed > 2*callerTimeout {
				t.Fatal("backend phase escaped cumulative I/O deadline", phase, end, calls.Load(), elapsed)
			}
		})
	}
}
