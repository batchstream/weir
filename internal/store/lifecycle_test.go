package store

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type lifecycleAdapter struct{ closes atomic.Int32 }

func (a *lifecycleAdapter) Prepare(*pb.BulkOperation) (*execution.Plan, *pb.Failure) { return nil, nil }
func (a *lifecycleAdapter) Execute(context.Context, []*execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	return nil, execution.Neutral
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
