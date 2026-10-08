package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type lifecycleAdapter struct{ closes atomic.Int32 }

func (a *lifecycleAdapter) PrepareCommand(uint64, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, nil
}
func (a *lifecycleAdapter) Execute(context.Context, []*execution.Plan, execution.Emit) bool {
	return false
}
func (a *lifecycleAdapter) Close() error { a.closes.Add(1); return nil }
func TestRuntimeOwnsAdapterExactlyOnce(t *testing.T) {
	for _, valid := range []bool{false, true} {
		a := &lifecycleAdapter{}
		limits := DefaultLimits()
		if !valid {
			limits.QueueOperations = -1
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
	command := record.Command()
	prepared := &execution.Plan{ID: record.Index(), Command: command, Key: record.Key(), BatchKey: "records", Bytes: 1024, ResultBytes: execution.ResultOverheadBytes, WorkingBytes: 1024}
	return prepared, nil
}

type forcedCloseAdapter struct {
	lifecycleAdapter
	started chan struct{}
}

func (adapter *forcedCloseAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) bool {
	for _, work := range plans {
		outcome := pb.MutationOutcome_APPLIED
		var failure *pb.Failure
		if work.Key == "active" {
			close(adapter.started)
			<-ctx.Done()
			outcome = pb.MutationOutcome_UNKNOWN
			failure = protocol.ContextFailure(ctx)
		}
		event := execution.FailedEvent(work.Command, outcome, failure)
		_ = emit(work, event)
	}
	return false
}
func TestForcedClosePreservesRecordEvidenceUntilAckOrSessionClose(t *testing.T) {
	for _, sessionOwned := range []bool{false, true} {
		for _, closeSession := range []bool{false, true} {
			if !sessionOwned && closeSession {
				continue
			}
			t.Run(fmt.Sprintf("session=%t/close=%t", sessionOwned, closeSession), func(t *testing.T) {
				adapter := &forcedCloseAdapter{started: make(chan struct{})}
				limits := DefaultLimits()

				limits.BatchOperations = 1
				runtime, err := New(adapter, limits)
				if err != nil {
					t.Fatal(err)
				}
				var session *Session
				if sessionOwned {
					session = runtime.NewSession()
					defer session.Close()
				}
				readyPlan := plan(1, "ready", false)
				ready, failure, _ := runtime.Submit(t.Context(), readyPlan, session)
				if failure != nil {
					t.Fatal(failure)
				}
				if event := recordEvent(t, ready); event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
					t.Fatal("completed write did not provide evidence", event)
				}
				activePlan := plan(2, "active", false)
				active, failure, _ := runtime.Submit(t.Context(), activePlan, session)
				if failure != nil {
					t.Fatal(failure)
				}
				select {
				case <-adapter.started:
				case <-time.After(time.Second):
					t.Fatal("active execution did not start")
				}
				// Forced cancellation keeps both completed and active record evidence
				// until their consumer acknowledges it or closes the session.
				closeContext, cancel := context.WithCancel(t.Context())
				cancel()
				if err := runtime.Close(closeContext); err != nil {
					t.Fatal(err)
				}
				tickets := []*Ticket{ready, active}
				outcomes := []pb.MutationOutcome{pb.MutationOutcome_APPLIED, pb.MutationOutcome_UNKNOWN}
				for index, ticket := range tickets {
					event, err := ticket.Wait(t.Context())
					if err != nil || event == nil || event.GetMutationResult().GetOutcome() != outcomes[index] {
						t.Fatal("forced shutdown discarded or changed caller-owned write evidence", index, event, err)
					}
					if index > 0 && event.GetMutationResult().GetFailure() == nil {
						t.Fatal("canceled write lost its failure evidence", index, event)
					}
				}
				if snapshot := runtime.Snapshot(); snapshot.Retained != 2 || snapshot.Ready != 2 || snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.ResultBytes != 2*execution.ResultOverheadBytes || snapshot.WorkingBytes != 0 || snapshot.Publishers != 0 {
					t.Fatal("forced shutdown released record consumer ownership", snapshot)
				}
				if closeSession {
					session.Close()
				} else {
					ready.Ack()
					if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.ResultBytes != execution.ResultOverheadBytes {
						t.Fatal("first acknowledgement released its peer", snapshot)
					}
					active.Ack()
				}
				if snapshot := runtime.Snapshot(); snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
					t.Fatal("consumer release leaked record ownership", snapshot)
				}
			})
		}
	}
}
