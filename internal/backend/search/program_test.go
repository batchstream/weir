//go:build !race

package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/luaworker"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "--weir-lua-worker" {
		if err := luaengine.Serve(os.Stdin, os.Stdout); err != nil {
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestProgramTransformReevaluatesAfterSearchVersionConflict(t *testing.T) {
	var gets, puts atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = io.WriteString(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`)
			return
		}
		if r.URL.Path != "/records/_doc/item" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			if r.URL.Query().Get("realtime") != "true" {
				t.Errorf("transform read was not realtime: %s", r.URL.RawQuery)
			}
			attempt := gets.Add(1)
			sequence := attempt
			count := 1
			if attempt > 1 {
				count = 10
			}
			_, _ = fmt.Fprintf(w, `{"_index":"records","_id":"item","found":true,"_seq_no":%d,"_primary_term":1,"_source":{"n":%d}}`, sequence, count)
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				t.Errorf("read replacement: %v", err)
				return
			}
			attempt := puts.Add(1)
			wantBody, wantSequence := `{"n":2}`, "1"
			if attempt == 2 {
				wantBody, wantSequence = `{"n":11}`, "2"
			}
			if string(body) != wantBody || r.URL.Query().Get("if_seq_no") != wantSequence || r.URL.Query().Get("if_primary_term") != "1" {
				t.Errorf("conditional replacement %d: body=%s query=%s", attempt, body, r.URL.RawQuery)
			}
			if attempt == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"error":{"type":"version_conflict_engine_exception"},"status":409}`)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"_index":"records","_id":"item","_version":3,"_seq_no":3,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}`)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	runnerConfig := luaworker.Config{Executable: os.Args[0], Timeout: time.Second}
	runner, err := luaworker.New(runnerConfig)
	if err != nil {
		t.Fatal(err)
	}
	a := &Adapter{
		config: Config{Store: "search", URL: server.URL, Index: "records", LuaRunner: runner},
		client: server.Client(),
		ctx:    context.Background(),
	}
	program := &pb.ProgramTransform{
		Runtime: "lua.v1",
		Source:  []byte(`return weir.replace(weir.object("n", weir.add(weir.get(current, "n"), weir.get(input, "step"))))`),
		Input:   &pb.Document{MediaType: "application/json", Data: []byte(`{"step":1}`)},
	}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:item", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	operation := &pb.BulkOperation{Operation: variant}
	work, failure := a.Prepare(operation)
	if failure != nil {
		t.Fatal(failure)
	}
	results, _ := a.Execute(context.Background(), []*execution.Plan{work})
	if len(results) != 1 {
		t.Fatalf("unexpected transform result: %#v", results)
	}
	if result := results[0].GetMutation(); result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
		t.Fatalf("unexpected transform result: outcome=%s failure=%+v gets=%d puts=%d", result.GetOutcome(), result.GetFailure(), gets.Load(), puts.Load())
	}
	if gets.Load() != 2 || puts.Load() != 2 {
		t.Fatalf("conflict did not cause a fresh read and evaluation: gets=%d puts=%d", gets.Load(), puts.Load())
	}
}

func TestProgramTransformRejectsUnqualifiedPipelines(t *testing.T) {
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte("return weir.keep()")}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "weir://search/records/s:item", Action: action}
	variant := &pb.BulkOperation_Mutate{Mutate: mutation}
	operation := &pb.BulkOperation{Operation: variant}
	for name, pipeline := range map[string]string{"default": "index.default_pipeline", "final": "index.final_pipeline"} {
		t.Run(name, func(t *testing.T) {
			var documentReads atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = fmt.Fprintf(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1",%q:"route"},"mappings":{"_source":{"enabled":true}}}}`, pipeline)
					return
				}
				if r.URL.Path == "/records/_doc/item" {
					documentReads.Add(1)
				}
				http.NotFound(w, r)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			runner := &luaworker.Runner{}
			a := &Adapter{
				config: Config{Store: "search", URL: server.URL, Index: "records", LuaRunner: runner},
				client: server.Client(),
				ctx:    context.Background(),
			}
			work, failure := a.Prepare(operation)
			if failure != nil {
				t.Fatal(failure)
			}
			results, _ := a.Execute(context.Background(), []*execution.Plan{work})
			if len(results) != 1 || results[0].GetMutation().GetFailure().GetCode() != pb.FailureCode_UNSUPPORTED {
				t.Fatalf("pipeline was not rejected: %#v", results)
			}
			if documentReads.Load() != 0 {
				t.Fatal("program accessed a document before pipeline qualification")
			}
		})
	}
}
