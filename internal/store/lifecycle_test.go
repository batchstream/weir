package store

import (
	"context"
	"github.com/batchstream/weir-protocol/api/protocol"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type lifecycleAdapter struct{ closes atomic.Int32 }

func (a *lifecycleAdapter) PrepareCommand(uint64, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, nil
}
func (a *lifecycleAdapter) Execute(context.Context, []*execution.Plan, execution.Emit) execution.Feedback {
	return execution.Neutral
}
func (a *lifecycleAdapter) Close() error { a.closes.Add(1); return nil }
func TestRuntimeOwnsAdapterExactlyOnce(t *testing.T) {
	for _, valid := range []bool{false, true} {
		a := &lifecycleAdapter{}
		limits := DefaultLimits()
		if !valid {
			limits.Concurrency = 0
		}
		runtime, err := New(a, limits)
		if !valid {
			if err == nil || a.closes.Load() != 1 {
				t.Fatal("invalid startup leaked adapter")
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		var wait sync.WaitGroup
		for range 5 {
			wait.Go(func() {
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			})
		}
		wait.Wait()
		cancel()
		if a.closes.Load() != 1 {
			t.Fatal("adapter closed more than once", a.closes.Load())
		}
	}
}

func (*lifecycleAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure { return nil }

func (a *lifecycleAdapter) PrepareRecord(record *execution.Record) (*execution.Plan, *pb.Failure) {
	operation := record.Operation()
	prepared := &execution.Plan{ID: operation.Index, Operation: operation, Key: protocol.Resource(operation), BatchKey: "records", Bytes: 1024, ResultBytes: protocol.ResultOverhead, WorkingBytes: 1024}
	return prepared, nil
}
