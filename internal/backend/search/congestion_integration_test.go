//go:build integration

package search

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchRealBackendCongestion(t *testing.T) {
	a, b := setupSearch(t)
	type sample struct {
		works   []*execution.Plan
		results []*pb.Event
	}
	var samples []sample
	for wave := 0; wave < 10; wave++ {
		start := make(chan struct{})
		replies := make(chan sample, 4)
		var workers sync.WaitGroup
		for worker := 0; worker < 4; worker++ {
			works := make([]*execution.Plan, 128)
			for i := range works {
				works[i] = searchPlan(t, a, "create", searchResource(b.Index, fmt.Sprintf("load-%d-%d-%d", wave, worker, i)))
				works[i].ID = uint64(i)
			}
			workers.Go(func() {
				<-start
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				results := a.executeRecords(ctx, works)
				sample := sample{works: works, results: results}
				replies <- sample
			})
		}
		close(start)
		workers.Wait()
		close(replies)
		congested := false
		for result := range replies {
			samples = append(samples, result)
			for _, event := range result.results {
				mutation := event.GetMutationResult()
				congested = congested || mutation.GetOutcome() == pb.MutationOutcome_NOT_APPLIED && mutation.GetFailure().GetCode() == pb.FailureCode_UNAVAILABLE && mutation.GetFailure().GetMessage() == "backend capacity unavailable"
			}
		}
		if congested {
			break
		}
	}
	applied, rejected := 0, 0
	for _, sample := range samples {

		for i, result := range sample.results {
			mutation := result.GetMutationResult()
			if mutation.GetOutcome() == pb.MutationOutcome_APPLIED {
				applied++
				continue
			}
			if mutation.GetOutcome() != pb.MutationOutcome_NOT_APPLIED || mutation.GetFailure().GetCode() != pb.FailureCode_UNAVAILABLE || mutation.GetFailure().GetMessage() != "backend capacity unavailable" {
				t.Fatal("unexpected native overload result", result)
			}
			// Inspect actual state, not just a returned native error label.
			if rejected < 3 {
				id := sample.works[i].Backend.(*plan).id
				status, _ := b.Do(t, "GET", "/"+b.Index+"/_doc/"+id, "")
				if status != 404 {
					t.Fatal("definitely rejected write persisted", id, status)
				}
			}
			rejected++
		}
	}
	if rejected == 0 || applied == 0 {
		t.Fatal("real rejection not exercised; use the task's bounded write-pool test profile", applied, rejected)
	}
	t.Log("applied", applied, "definite native capacity rejections", rejected)
}
