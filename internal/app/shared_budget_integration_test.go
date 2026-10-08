//go:build integration

package app

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

// This observation is inside the owned wire proxy, not a second scheduler.
// All command transitions for these independent processes share this one lock.
// Held replies have already executed in Mongo; held requests have not.
type budgetObservation struct {
	mu                               sync.Mutex
	active, peak, started, completed int
	gate                             <-chan struct{}
	delay                            time.Duration
}

func (o *budgetObservation) start(ctx context.Context, e *event.CommandStartedEvent) {
	if !budgetCommand(e.CommandName) {
		return
	}
	o.begin(ctx)
}

func (o *budgetObservation) begin(ctx context.Context) {
	o.mu.Lock()
	o.active++
	o.peak = max(o.peak, o.active)
	o.started++
	gate, delay := o.gate, o.delay
	o.mu.Unlock()
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
		}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	}
}

func (o *budgetObservation) finish(_ context.Context, e *event.CommandSucceededEvent) {
	if !budgetCommand(e.CommandName) {
		return
	}
	o.end()
}

func (o *budgetObservation) end() {
	o.mu.Lock()
	o.active--
	o.completed++
	o.mu.Unlock()
}

func budgetCommand(name string) bool {
	switch name {
	case "find", "count", "findAndModify", "insert", "update", "delete", "bulkWrite", "getMore", "killCursors", "endSessions", "killSessions":
		return true
	}
	return false
}

func (o *budgetObservation) hold(gate <-chan struct{}, delay time.Duration) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.gate, o.delay = gate, delay
}

func (o *budgetObservation) snapshot() (active, peak, started, completed int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.active, o.peak, o.started, o.completed
}

func budgetPut(root, id string) *pb.MutateRequest {
	doc := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int64(1)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: root + "/s:" + id, Action: action}
	return request
}

func budgetWait(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	until := time.Now().Add(1500 * time.Millisecond)
	for !predicate() {
		if time.Now().After(until) {
			t.Fatal("barrier not reached:", label)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func budgetLoadCall(ctx context.Context, client pb.StoreServiceClient, fixture testutil.RecordFixture, mode int) bool {
	request := fixture.Command.GetMutate()
	switch mode {
	case 0:
		response, err := testutil.ExecuteRecord(ctx, client, fixture)
		if err != nil || response == nil {
			return false
		}
		result := response.GetMutationResult()
		return result.GetFailure() == nil && result.GetOutcome() == pb.MutationOutcome_APPLIED
	case 1:
		read := &pb.ReadRequest{Resource: request.Resource}
		response, err := testutil.ExecuteRecord(ctx, client, testutil.RecordRequest(fixture.StoreName, read))
		if err != nil || response == nil {
			return false
		}
		result := response.GetReadResult()
		return result.GetFailure() == nil
	default:
		batch := []*pb.MutateRequest{fixture.Command.GetMutate(), fixture.Command.GetMutate()}
		response, err := testutil.MutateRecords(ctx, client, fixture.StoreName, batch)
		if err != nil || len(response) != len(batch) {
			return false
		}
		for _, result := range response {
			if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.GetFailure() != nil {
				return false
			}
		}
		return true
	}
}
