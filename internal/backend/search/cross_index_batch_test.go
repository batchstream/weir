package search

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type crossIndexReadTarget struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

type crossIndexWriteMetadata struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
	Seq   *int64 `json:"if_seq_no"`
	Term  *int64 `json:"if_primary_term"`
}

func TestCrossIndexRecordBatchIsolatesTargetFailures(t *testing.T) {
	for _, phase := range []string{"qualification", "preread", "write"} {
		for _, visible := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/visible=%t", phase, visible), func(t *testing.T) {
				var mu sync.Mutex
				inspections := make(map[string]int)
				reads, writes := 0, 0
				handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					mu.Lock()
					defer mu.Unlock()
					switch request.URL.Path {
					case "/left", "/denied", "/right":
						index := strings.TrimPrefix(request.URL.Path, "/")
						inspections[index]++
						metadata := strings.ReplaceAll(testIndexReply, "records", index)
						if index == "denied" && phase == "qualification" {
							metadata = strings.ReplaceAll(metadata, `"enabled":true`, `"enabled":false`)
						}
						fmt.Fprint(w, metadata)
					case "/_mget":
						reads++
						var body struct{ Docs []crossIndexReadTarget }
						if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
							t.Error(err)
							return
						}
						docs := make([]string, len(body.Docs))
						for i, target := range body.Docs {
							if target.ID != "same" {
								t.Error("pre-read changed record ID", target)
							}
							if target.Index == "denied" && phase == "qualification" {
								t.Error("unqualified target reached pre-read")
							}
							if target.Index == "denied" && phase == "preread" {
								docs[i] = `{"_index":"denied","_id":"same","error":{"type":"index_not_found_exception"},"status":404}`
								continue
							}
							value := 10
							if target.Index == "right" {
								value = 20
							}
							docs[i] = fmt.Sprintf(`{"_index":%q,"_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":%d}}`, target.Index, target.ID, value)
						}
						fmt.Fprintf(w, `{"docs":[%s]}`, strings.Join(docs, ","))
					case "/_bulk":
						writes++
						refresh := "false"
						if visible {
							refresh = "wait_for"
						}
						if request.URL.Query().Get("refresh") != refresh {
							t.Error("mixed-index bulk changed acknowledgement visibility", request.URL.RawQuery)
						}
						decoder := json.NewDecoder(request.Body)
						items := make([]string, 0, 3)
						for {
							var header map[string]crossIndexWriteMetadata
							err := decoder.Decode(&header)
							if err == io.EOF {
								break
							}
							if err != nil || len(header) != 1 {
								t.Error("invalid bulk framing", err)
								return
							}
							metadata, exists := header["index"]
							if !exists || metadata.Seq == nil || *metadata.Seq != 1 || metadata.Term == nil || *metadata.Term != 1 {
								t.Error("write lost its target's OCC condition", header)
							}
							var document struct{ N int }
							if err := decoder.Decode(&document); err != nil {
								t.Error(err)
								return
							}
							value := 11
							if metadata.Index == "right" {
								value = 21
							}
							if document.N != value {
								t.Error("Lua used a different index's observation", metadata.Index, document)
							}
							if metadata.Index == "denied" {
								if phase != "write" {
									t.Error("rejected target reached a write", phase)
								}
								items = append(items, `{"index":{"_index":"denied","_id":"same","status":403,"error":{"type":"security_exception"}}}`)
								continue
							}
							item := fmt.Sprintf(`{"index":{"_index":%q,"_id":%q,"status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, metadata.Index, metadata.ID)
							items = append(items, item)
						}
						fmt.Fprintf(w, `{"errors":%t,"took":1,"items":[%s]}`, phase == "write", strings.Join(items, ","))
					default:
						t.Error("unexpected mixed-index request", request.URL.Path)
						http.NotFound(w, request)
					}
				})
				server := httptest.NewServer(handler)
				defer server.Close()
				config := Config{Store: "search", URL: server.URL}
				adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
				works := make([]*execution.Plan, 0, 3)
				for i, index := range []string{"left", "denied", "right"} {
					options := visibilityPlanOptions{action: "program", resource: index + "/s:same", wait: visible, program: `return function(current, incoming) current.n = current.n + 1; return current end`}
					work := visibilityPlan(t, adapter, options)
					work.ID = uint64(i + 1)
					works = append(works, work)
					if work.BatchKey != works[0].BatchKey {
						t.Fatal("record grouping still depends on the index")
					}
				}
				results := adapter.executeRecords(t.Context(), works)
				for i, result := range results {
					mutation := result.GetMutationResult()
					if i != 1 {
						if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
							t.Fatal("one target failure contaminated its peer", i, mutation)
						}
						continue
					}
					code := pb.FailureCode_UNSUPPORTED
					if phase == "preread" {
						code = pb.FailureCode_TARGET_NOT_FOUND
					} else if phase == "write" {
						code = pb.FailureCode_PERMISSION_DENIED
					}
					if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != code {
						t.Fatal("failed target lost its own rejection evidence", mutation)
					}
				}
				mu.Lock()
				defer mu.Unlock()
				if writes != 1 || reads != 1 || len(inspections) != 3 {
					t.Fatal("mixed target work was split or replayed", writes, reads, inspections)
				}
				for index, count := range inspections {
					if count != 1 {
						t.Fatal("target qualification was not local to one execution", index, count)
					}
				}
			})
		}
	}
}

func TestCrossIndexAliasCollisionReevaluatesOnlyTheConfirmedConflict(t *testing.T) {
	var mu sync.Mutex
	value, sequence := 10, int64(1)
	reads, writes := 0, 0
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch request.URL.Path {
		case "/left_alias", "/right_alias":
			alias := strings.TrimPrefix(request.URL.Path, "/")
			fmt.Fprintf(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"aliases":{%q:{}},"mappings":{}}}`, alias)
		case "/_mget":
			reads++
			var body struct{ Docs []crossIndexReadTarget }
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Error(err)
				return
			}
			want := 2
			if reads > 1 {
				want = 1
			}
			if len(body.Docs) != want {
				t.Error("retry reread an already-applied alias peer", body.Docs)
			}
			docs := make([]string, len(body.Docs))
			for i, target := range body.Docs {
				if target.Index != "records" || target.ID != "same" {
					t.Error("pre-read did not pin the resolved physical target", target)
				}
				docs[i] = fmt.Sprintf(`{"_index":"records","_id":"same","found":true,"_seq_no":%d,"_primary_term":1,"_source":{"n":%d}}`, sequence, value)
			}
			fmt.Fprintf(w, `{"docs":[%s]}`, strings.Join(docs, ","))
		case "/_bulk":
			writes++
			if request.URL.Query().Get("refresh") != "wait_for" {
				t.Error("alias conflict retry changed visibility acknowledgement")
			}
			decoder := json.NewDecoder(request.Body)
			items := make([]string, 0, 2)
			conflict := false
			for {
				var header map[string]crossIndexWriteMetadata
				err := decoder.Decode(&header)
				if err == io.EOF {
					break
				}
				if err != nil || len(header) != 1 {
					t.Error("invalid alias bulk framing", err)
					return
				}
				metadata, exists := header["index"]
				var document struct{ N int }
				if err := decoder.Decode(&document); err != nil {
					t.Error(err)
					return
				}
				if !exists || metadata.Index != "records" || metadata.ID != "same" || metadata.Seq == nil || metadata.Term == nil || *metadata.Term != 1 {
					t.Error("alias write lost the resolved target or OCC condition", header)
					return
				}
				if *metadata.Seq != sequence {
					conflict = true
					items = append(items, `{"index":{"_index":"records","_id":"same","status":409,"error":{"type":"version_conflict_engine_exception"}}}`)
					continue
				}
				if document.N != value+1 {
					t.Error("conflict retry reused the stale Lua replacement", document, value)
				}
				value = document.N
				sequence++
				item := fmt.Sprintf(`{"index":{"_index":"records","_id":"same","status":200,"result":"updated","_version":%d,"_seq_no":%d,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, sequence, sequence)
				items = append(items, item)
			}
			want := 2
			if writes > 1 {
				want = 1
			}
			if len(items) != want {
				t.Error("retry replayed an already-applied alias peer", len(items), want)
			}
			fmt.Fprintf(w, `{"errors":%t,"took":1,"items":[%s]}`, conflict, strings.Join(items, ","))
		default:
			t.Error("unexpected alias collision request", request.URL.Path)
			http.NotFound(w, request)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: config, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
	works := make([]*execution.Plan, 0, 2)
	for _, alias := range []string{"left_alias", "right_alias"} {
		options := visibilityPlanOptions{action: "program", resource: alias + "/s:same", wait: true, program: `return function(current, incoming) current.n = current.n + 1; return current end`}
		work := visibilityPlan(t, adapter, options)
		works = append(works, work)
	}
	if works[0].BatchKey != works[1].BatchKey || works[0].Key == works[1].Key {
		t.Fatal("independent aliases cannot share a record batch")
	}
	for _, result := range adapter.executeRecords(t.Context(), works) {
		mutation := result.GetMutationResult()
		if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil {
			t.Fatal("confirmed alias conflict did not reevaluate successfully", mutation)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if reads != 2 || writes != 2 || value != 12 || sequence != 3 {
		t.Fatal("alias collision lost an update or replayed its successful peer", reads, writes, value, sequence)
	}
	for i, alias := range []string{"left_alias", "right_alias"} {
		if works[i].Backend.(*plan).index != alias {
			t.Fatal("resolved execution mutated a reusable alias plan")
		}
	}
}
