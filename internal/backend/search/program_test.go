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

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestProgramTransformReevaluatesAfterSearchVersionConflict(t *testing.T) {
	var gets, puts atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = io.WriteString(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`)
			return
		}
		switch r.URL.Path {
		case "/records/_mget":
			if r.URL.Query().Get("realtime") != "true" {
				t.Errorf("transform read was not realtime: %s", r.URL.RawQuery)
			}
			attempt := gets.Add(1)
			sequence := attempt
			count := 1
			if attempt > 1 {
				count = 10
			}
			_, _ = fmt.Fprintf(w, `{"docs":[{"_index":"records","_id":"item","found":true,"_seq_no":%d,"_primary_term":1,"_source":{"n":%d}}]}`, sequence, count)
		case "/_bulk":
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
			lines := strings.Split(strings.TrimSpace(string(body)), "\n")
			if len(lines) != 2 || lines[1] != wantBody || !strings.Contains(lines[0], `"if_seq_no":`+wantSequence) || !strings.Contains(lines[0], `"if_primary_term":1`) {
				t.Errorf("conditional replacement %d: body=%s query=%s", attempt, body, r.URL.RawQuery)
			}
			if attempt == 1 {
				_, _ = io.WriteString(w, `{"errors":true,"took":1,"items":[{"index":{"_index":"records","_id":"item","error":{"type":"version_conflict_engine_exception"},"status":409}}]}`)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"item","status":200,"_version":3,"_seq_no":3,"_primary_term":1,"result":"updated","_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		default:
			http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	a := &Adapter{dialect: ElasticsearchProduct,
		config: Config{Store: "search", URL: server.URL},
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
	mutation := &pb.MutateRequest{Resource: "records/s:item", Action: action}
	operation := &execution.Operation{Mutate: mutation}
	work, failure := prepareTestRecord(a, operation)
	if failure != nil {
		t.Fatal(failure)
	}
	results, _ := a.executeRecords(context.Background(), []*execution.Plan{work})
	if len(results) != 1 {
		t.Fatalf("unexpected transform result: %#v", results)
	}
	if result := results[0].Mutation; result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
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
	mutation := &pb.MutateRequest{Resource: "records/s:item", Action: action}
	operation := &execution.Operation{Mutate: mutation}
	for name, pipeline := range map[string]string{"default": "index.default_pipeline", "final": "index.final_pipeline"} {
		t.Run(name, func(t *testing.T) {
			var documentReads atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = fmt.Fprintf(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1",%q:"route"},"mappings":{"_source":{"enabled":true}}}}`, pipeline)
					return
				}
				if r.URL.Path == "/records/_mget" {
					documentReads.Add(1)
				}
				http.NotFound(w, r)
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			a := &Adapter{dialect: ElasticsearchProduct,
				config: Config{Store: "search", URL: server.URL},
				client: server.Client(),
				ctx:    context.Background(),
			}
			work, failure := prepareTestRecord(a, operation)
			if failure != nil {
				t.Fatal(failure)
			}
			results, _ := a.executeRecords(context.Background(), []*execution.Plan{work})
			if len(results) != 1 || results[0].Mutation.GetFailure().GetCode() != pb.FailureCode_UNSUPPORTED {
				t.Fatalf("pipeline was not rejected: %#v", results)
			}
			if documentReads.Load() != 0 {
				t.Fatal("program accessed a document before pipeline qualification")
			}
		})
	}
}
