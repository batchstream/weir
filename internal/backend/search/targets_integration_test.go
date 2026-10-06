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
	results := a.executeRecords(context.Background(), works)
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
		results := a.executeRecords(context.Background(), works)
		if len(results) != len(works) {
			t.Fatal("cross-index mutation cardinality", actions, len(results))
		}
		for i, result := range results {

			for attempt := 1; ; attempt++ {
				mutation := result.GetMutationResult()
				if mutation.GetOutcome() == pb.MutationOutcome_APPLIED && mutation.GetFailure() == nil {
					break
				}
				failure := mutation.GetFailure()
				if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED ||
					failure.GetCode() != pb.FailureCode_UNAVAILABLE ||
					failure.GetMessage() != "backend capacity unavailable" {
					t.Fatal("cross-index mutation failed without confirmed capacity rejection", actions, result)
				}
				if attempt == 3 {
					t.Fatal("target mutation remained capacity-rejected after three attempts", actions, result)
				}
				t.Logf("target=%s action=%s attempt=%d confirmed not applied; submitting one item after capacity rejection", indexes[i], works[i].Backend.(*plan).action, attempt)
				time.Sleep(50 * time.Millisecond)
				single := []*execution.Plan{works[i]}
				var replies []*pb.Event
				replies = a.executeRecords(context.Background(), single)
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
			results := a.executeRecords(context.Background(), []*execution.Plan{left, right})
			if string(results[0].GetReadResult().GetDocument().GetData()) != `{"n":2}` || string(results[1].GetReadResult().GetDocument().GetData()) != `{"n":3}` {
				t.Error("same service concurrent index isolation failed", results)
			}
		})
	}
	concurrent.Wait()

	for i, index := range indexes {
		bulk := nativeRequest(t, index, "POST", "/_bulk")
		body := fmt.Sprintf("{\"index\":{\"_index\":%q,\"_id\":\"native\"}}\n{\"n\":%d}\n", index, 7+i)
		end, capture := runNative(t, a, bulk, []byte(body))
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || !strings.Contains(capture.body.String(), `"_index":"`+index+`"`) {
			t.Fatal("Native bulk request target", end, capture.body.String())
		}
		get := nativeRequest(t, index, "GET", "/_doc/native")
		end, capture = runNative(t, a, get, nil)
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE ||
			!strings.Contains(capture.body.String(), `"_index":"`+index+`"`) ||
			!strings.Contains(capture.body.String(), `"_id":"native"`) ||
			!strings.Contains(capture.body.String(), fmt.Sprintf(`"n":%d`, 7+i)) {
			t.Fatal("Native GET request target", end, capture.body.String())
		}
		expected := map[string]int{"same": 2 + i, "expression": 2, "put": 2, "create": 2, "native": 7 + i}
		// Business markers identify each source without exposing native hit metadata.
		for record := range expected {
			body := fmt.Sprintf(`{"doc":{"target":%q,"record":%q}}`, index, record)
			status, raw := backend.Do(t, "POST", "/"+index+"/_update/"+record, body)
			if status != 200 {
				t.Fatal("mark Scan source", record, status, string(raw))
			}
		}
		status, raw := backend.Do(t, "POST", "/"+index+"/_refresh", "")
		if status != 200 {
			t.Fatal("target refresh", status, string(raw))
		}
		work := scanWork(t, a, index)
		seen := make(map[string]bool, len(expected))
		for step := 0; ; step++ {
			if step > 10 {
				t.Fatal("target Scan failed to exhaust")
			}
			page := a.fetchScan(context.Background(), work)
			if page.Failure != nil {
				t.Fatal("target Scan", page.Failure)
			}
			for _, document := range page.Documents {
				var source struct {
					Target string `json:"target"`
					Record string `json:"record"`
					N      *int   `json:"n"`
				}
				if err := json.Unmarshal(document.Data, &source); err != nil {
					t.Fatal(err)
				}
				if source.Target != index {
					t.Fatal("Scan source crossed request target", string(document.Data))
				}
				want, exists := expected[source.Record]
				if !exists || source.N == nil || *source.N != want {
					t.Fatal("Scan published an unexpected record or value", string(document.Data))
				}
				if seen[source.Record] {
					t.Fatal("Scan duplicated a record", string(document.Data))
				}
				seen[source.Record] = true
			}
			if page.Exhausted {
				break
			}
		}
		if failure := a.closeScan(context.Background(), work); failure != nil || len(seen) != len(expected) {
			t.Fatal("target PIT traversal/cleanup", failure, seen)
		}
	}
}
