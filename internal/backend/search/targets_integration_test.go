//go:build integration

package search

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchMultipleRequestTargets(t *testing.T) {
	a, backend := setupSearch(t)
	if a.dialect != backend.Product {
		t.Fatal("backend product was not detected automatically", a.dialect, backend.Product)
	}
	other := backend.Index + "_other"
	backend.Create(t, other, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"n":{"type":"long"}}}}`)
	indexes := []string{backend.Index, other}
	for i, index := range indexes {
		for _, id := range []string{"same", "expression"} {
			body := fmt.Sprintf(`{"n":%d}`, 10+i*10)
			status, raw := backend.Do(t, "PUT", "/"+index+"/_doc/"+id, body)
			if status != 201 {
				t.Fatal("seed request target", status, string(raw))
			}
		}
	}
	leftRead := batchTestPlan(t, a, "read", searchResource(indexes[0], "same"))
	rightRead := batchTestPlan(t, a, "read", searchResource(indexes[1], "same"))
	works := []*execution.Plan{leftRead, rightRead}
	for i, work := range works {
		work.ID = uint64(i)
	}
	results, _ := a.executeRecords(context.Background(), works)
	if len(results) != len(works) {
		t.Fatal("mixed target cardinality", len(results))
	}
	for i, result := range results {
		want := fmt.Sprintf(`{"n":%d}`, 10+i*10)
		if string(result.GetReadResult().GetDocument().GetData()) != want {
			t.Fatal("same ID crossed indexes during pre-read", result)
		}
	}

	// The owned backends deliberately have a one-entry write queue. Even two
	// target shards can exceed it alongside the coordinating bulk task. Only a
	// confirmed capacity rejection may be submitted again by this test; the
	// production adapter never replays it. Offline coverage combines every action.
	phases := []struct{ left, right, id string }{
		{left: "replace", right: "program", id: "same"},
		{left: "expression", right: "expression", id: "expression"},
		{left: "put", right: "put", id: "put"},
		{left: "create", right: "create", id: "create"},
		{left: "delete", right: "delete", id: "delete"},
	}
	for phase, actions := range phases {
		left := batchTestPlan(t, a, actions.left, searchResource(indexes[0], actions.id))
		right := batchTestPlan(t, a, actions.right, searchResource(indexes[1], actions.id))
		works := []*execution.Plan{left, right}
		for i, work := range works {
			work.ID = uint64(100 + phase*2 + i)
		}
		results, batchFeedback := a.executeRecords(context.Background(), works)
		if len(results) != len(works) {
			t.Fatal("cross-index mutation cardinality", actions, len(results))
		}
		for i, result := range results {
			feedback := batchFeedback
			for attempt := 1; ; attempt++ {
				mutation := result.GetMutationResult()
				if mutation.GetOutcome() == pb.MutationOutcome_APPLIED && mutation.GetFailure() == nil {
					break
				}
				failure := mutation.GetFailure()
				if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED ||
					failure.GetCode() != pb.FailureCode_UNAVAILABLE ||
					failure.GetMessage() != "backend capacity unavailable" ||
					feedback != execution.Congested {
					t.Fatal("cross-index mutation failed without confirmed capacity rejection", actions, result, feedback)
				}
				if attempt == 3 {
					t.Fatal("target mutation remained capacity-rejected after three attempts", actions, result)
				}
				t.Logf("target=%s action=%s attempt=%d confirmed not applied; submitting one item after capacity rejection", indexes[i], works[i].Backend.(*plan).action, attempt)
				time.Sleep(50 * time.Millisecond)
				single := []*execution.Plan{works[i]}
				var replies []*pb.Event
				replies, feedback = a.executeRecords(context.Background(), single)
				if len(replies) != 1 {
					t.Fatal("single target mutation result position", actions, replies)
				}
				result = replies[0]
			}
		}
	}

	var concurrent sync.WaitGroup
	for range 8 {
		left := batchTestPlan(t, a, "read", searchResource(indexes[0], "same"))
		right := batchTestPlan(t, a, "read", searchResource(indexes[1], "same"))
		concurrent.Go(func() {
			results, _ := a.executeRecords(context.Background(), []*execution.Plan{left, right})
			if string(results[0].GetReadResult().GetDocument().GetData()) != `{"n":2}` || string(results[1].GetReadResult().GetDocument().GetData()) != `{"n":3}` {
				t.Error("same service concurrent index isolation failed", results)
			}
		})
	}
	concurrent.Wait()

	for i, index := range indexes {
		bulk := nativeOpen(t, index, "POST", "/_bulk")
		body := fmt.Sprintf("{\"index\":{\"_index\":%q,\"_id\":\"native\"}}\n{\"n\":%d}\n", index, 7+i)
		end, capture := runNative(t, a, bulk, []byte(body))
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), `"_index":"`+index+`"`) {
			t.Fatal("Native bulk request target", end, capture.body.String())
		}
		get := nativeOpen(t, index, "GET", "/_doc/native")
		end, capture = runNative(t, a, get, nil)
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), fmt.Sprintf(`"n":%d`, 7+i)) {
			t.Fatal("Native GET request target", end, capture.body.String())
		}
		status, raw := backend.Do(t, "POST", "/"+index+"/_refresh", "")
		if status != 200 {
			t.Fatal("target refresh", status, string(raw))
		}
		work := scanWork(t, a, index)
		seenSame, seenNative := false, false
		for step := 0; ; step++ {
			if step > 10 {
				t.Fatal("target Scan failed to exhaust")
			}
			page, _ := a.fetchScan(context.Background(), work)
			if page.Failure != nil {
				t.Fatal("target Scan", page.Failure)
			}
			for _, document := range page.Documents {
				var hit struct {
					Index string `json:"_index"`
					ID    string `json:"_id"`
				}
				if err := json.Unmarshal(document.Data, &hit); err != nil {
					t.Fatal(err)
				}
				if hit.Index != index {
					t.Fatal("PIT hit crossed request target", string(document.Data))
				}
				if hit.ID == "same" {
					seenSame = true
				} else if hit.ID == "native" {
					seenNative = true
				}
			}
			if page.Exhausted {
				break
			}
		}
		if failure := a.closeScan(context.Background(), work); failure != nil || !seenSame || !seenNative {
			t.Fatal("target PIT traversal/cleanup", failure, seenSame, seenNative)
		}
	}
}
