package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
)

func TestManySlowNativeOutputsLeaveBackendCreditForReadsAndShortNative(t *testing.T) {
	for _, maxInFlight := range []int{1, 16} {
		t.Run(fmt.Sprint(maxInFlight), func(t *testing.T) {
			limits := DefaultLimits()
			limits.MaxInFlight = maxInFlight
			adapter := &singleExecutionAdapter{}
			runtime, err := New(adapter, limits, nil)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			t.Cleanup(func() {
				closing, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := runtime.Close(closing); err != nil {
					t.Error(err)
				}
			})
			for index := range 32 {
				session := runtime.NewSession()
				defer session.Close()
				work := plan(uint64(index), fmt.Sprint(index), false)
				work.Command = nativeCall()
				work.WorkingBytes = 2048
				if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
					t.Fatal(failure)
				}
				select {
				case <-session.Events:
				case <-ctx.Done():
					t.Fatal("small stalled Native streams consumed all backend credit", runtime.Snapshot())
				}
			}
			work := plan(100, "healthy", true)
			ticket, failure, _ := runtime.Submit(ctx, work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			if event, err := ticket.Wait(ctx); err != nil || event.GetReadResult() == nil {
				t.Fatal("healthy typed read did not finish beside slow Native output", event, err)
			}
			ticket.Ack()
			session := runtime.NewSession()
			defer session.Close()
			work = plan(101, "short-native", false)
			work.Command = nativeCall()
			work.WorkingBytes = 2048
			short, failure, _ := runtime.Submit(ctx, work, session)
			if failure != nil {
				t.Fatal(failure)
			}
			var complete bool
			for {
				select {
				case emission := <-session.Events:
					emission.Release()
					if emission.Event == nil {
						short.Ack()
						goto finished
					}
					complete = complete || emission.Event.GetNativeEnd().GetCompletion() == pb.NativeCompletion_RESPONSE_COMPLETE
				case <-ctx.Done():
					t.Fatal("short Native query did not finish beside slow Native output", runtime.Snapshot())
				}
			}
		finished:
			if snapshot := runtime.Snapshot(); !complete || snapshot.Paused != 32 || snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.PausedBytes > limits.QueueBytes {
				t.Fatal("slow streams lost their bounded independent ownership", complete, snapshot)
			}
		})
	}
}

func TestNativeHardCapacityRejectsBeforeBackendAndLeavesTypedHealthy(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight = 1
	limits.QueueBytes = 1024
	memory := NewNativeMemory()
	memory.known, memory.trusted = true, true
	memory.budget = 128 << 20
	adapter := &singleExecutionAdapter{}
	runtime, err := New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
	})
	for index := range 2 {
		session := runtime.NewSession()
		defer session.Close()
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.Bytes = 512
		work.Command = nativeCall()
		work.WorkingBytes = 4096
		if index == 0 {
			publication := max(work.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes) - nativeQueuedWindow
			work.WorkingBytes = int(memory.budget-memory.inputTargetLocked()) - publication
		}
		ticket, failure, _ := runtime.Submit(ctx, work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		select {
		case emission := <-session.Events:
			if index == 0 {
				if emission.Event.GetHead() == nil {
					t.Fatal("first bounded response did not start", emission.Event)
				}
			} else {
				end := emission.Event.GetNativeEnd()
				if end.GetCompletion() != pb.NativeCompletion_NATIVE_NOT_STARTED || end.Failure.GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
					t.Fatal("full actual budget changed pre-attempt evidence", emission.Event)
				}
				emission.Release()
				marker := <-session.Events
				marker.Release()
				ticket.Ack()
			}
		case <-ctx.Done():
			t.Fatal("Native hard capacity became an output wait", runtime.Snapshot())
		}
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Paused != 1 {
		t.Fatal("bounded output still owned backend IO", snapshot)
	}
	if got := testmetrics.Sum(testmetrics.Gather(t, runtime), "weir_store_executions_total"); got != 1 {
		t.Fatal("hard capacity rejection invoked the backend", got)
	}
	work := plan(100, "healthy", true)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if event, err := ticket.Wait(ctx); err != nil || event.GetReadResult() == nil {
		t.Fatal("full Native budget blocked typed work", event, err)
	}
	ticket.Ack()
}

type nativeResumeAdapter struct {
	singleExecutionAdapter
	entered chan string
	gate    <-chan struct{}
}

func (adapter *nativeResumeAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	work := plans[0]
	adapter.entered <- work.Key
	if work.Command.GetNative() != nil {
		head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
		value := &pb.Event_Head{Head: head}
		event := &pb.Event{Value: value}
		if err := emit(work, event); err != nil {
			return
		}
		if err := work.NativeIO.BeginIO(ctx, 4096); err != nil {
			return
		}
		adapter.entered <- "resume-native"
		if err := work.NativeIO.EndIO(work.WorkingBytes, 0); err != nil {
			return
		}
		end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
		endValue := &pb.Event_NativeEnd{NativeEnd: end}
		endEvent := &pb.Event{Value: endValue}
		_ = emit(work, endEvent)
		return
	}
	if work.Key == "active" {
		select {
		case <-adapter.gate:
		case <-ctx.Done():
		}
	}
	adapter.singleExecutionAdapter.Execute(ctx, plans, emit)
}

func TestNativeResumeSharesFIFOWithNewBackendExecutions(t *testing.T) {
	gate := make(chan struct{})
	adapter := &nativeResumeAdapter{entered: make(chan string, 8), gate: gate}
	limits := DefaultLimits()
	limits.MaxInFlight, limits.BatchOperations = 1, 1
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
	})
	session := runtime.NewSession()
	defer session.Close()
	work := plan(1, "native", false)
	work.Command = nativeCall()
	if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
		t.Fatal(failure)
	}
	var head *Emission
	select {
	case head = <-session.Events:
	case <-ctx.Done():
		t.Fatal("Native did not reach its consumer")
	}
	if entered := <-adapter.entered; entered != "native" {
		t.Fatal(entered)
	}
	var tickets []*Ticket
	for index, key := range []string{"active", "older"} {
		work := plan(uint64(index+2), key, true)
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
		if index == 0 {
			select {
			case entered := <-adapter.entered:
				if entered != key {
					t.Fatal(entered)
				}
			case <-ctx.Done():
				t.Fatal("active backend did not start")
			}
		}
	}
	head.Release()
	for {
		runtime.mu.Lock()
		resuming := runtime.resumeHead != nil
		runtime.mu.Unlock()
		if resuming {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("Native continuation did not queue")
		}
		time.Sleep(time.Millisecond)
	}
	work = plan(4, "newer", true)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	tickets = append(tickets, ticket)
	close(gate)
	for _, expected := range []string{"older", "resume-native", "newer"} {
		select {
		case entered := <-adapter.entered:
			if entered != expected {
				t.Fatal("Native continuation changed FIFO dispatch order", entered, expected)
			}
		case <-ctx.Done():
			t.Fatal("queued backend executions did not resume", runtime.Snapshot())
		}
	}
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
	session.Close()
	waitReleased(t, runtime)
	if snapshot := runtime.Snapshot(); snapshot.Paused != 0 || snapshot.PausedBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("Native resume retained reservation credit", snapshot)
	}
}

func TestDeliveredNativeEndCompletesWithoutReacquiringBusyBackendSlot(t *testing.T) {
	gate := make(chan struct{})
	adapter := &nativeResumeAdapter{entered: make(chan string, 8), gate: gate}
	limits := DefaultLimits()
	limits.MaxInFlight, limits.BatchOperations = 1, 1
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
	})
	session := runtime.NewSession()
	defer session.Close()
	work := plan(1, "native", false)
	work.Command = nativeCall()
	native, failure, _ := runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	var end *Emission
	for end == nil {
		select {
		case emission := <-session.Events:
			if emission.Event.GetNativeEnd() != nil {
				end = emission
			} else {
				emission.Release()
			}
		case <-ctx.Done():
			t.Fatal("Native did not produce its terminal result")
		}
	}
	work = plan(2, "active", true)
	active, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	for {
		select {
		case entered := <-adapter.entered:
			if entered == "active" {
				goto occupied
			}
		case <-ctx.Done():
			t.Fatal("typed backend did not occupy the execution slot")
		}
	}
occupied:
	end.Release()
	select {
	case emission := <-session.Events:
		if emission.Event != nil {
			t.Fatal("Native emitted another event after its terminal result", emission.Event)
		}
		emission.Release()
		native.Ack()
	case <-ctx.Done():
		t.Fatal("delivered Native result waited to reacquire a busy backend slot", runtime.Snapshot())
	}
	for {
		snapshot := runtime.Snapshot()
		if snapshot.Retained == 1 {
			if snapshot.Active != 1 || snapshot.Paused != 0 {
				t.Fatal("Native completion consumed another request's backend credit", snapshot)
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal("Native completion retained its publication", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	close(gate)
	if _, err := active.Wait(ctx); err != nil {
		t.Fatal(err)
	}
	active.Ack()
	waitReleased(t, runtime)
}

type nativeWorkingSetAdapter struct {
	singleExecutionAdapter
}

func (adapter *nativeWorkingSetAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	work := plans[0]
	if work.Command.GetNative() == nil {
		adapter.singleExecutionAdapter.Execute(ctx, plans, emit)
		return
	}
	work.WorkingBytes = 1 << 20
	head := &pb.NativeHead{BodyContentType: "application/octet-stream"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return
	}
	work.WorkingBytes = execution.EntryOverheadBytes
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	endValue := &pb.Event_NativeEnd{NativeEnd: end}
	endEvent := &pb.Event{Value: endValue}
	_ = emit(work, endEvent)
}

func TestNativeTerminalOutputUsesCurrentWorkingSetInsteadOfReleasedPeak(t *testing.T) {
	limits := DefaultLimits()
	limits.MaxInFlight, limits.QueueBytes = 1, 128<<10
	adapter := &nativeWorkingSetAdapter{}
	runtime, err := New(adapter, limits, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	t.Cleanup(func() {
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
	})
	for index := range 32 {
		session := runtime.NewSession()
		defer session.Close()
		work := plan(uint64(index+1), fmt.Sprint(index), false)
		work.Command = nativeCall()
		if _, failure, _ := runtime.Submit(ctx, work, session); failure != nil {
			t.Fatal(failure)
		}
		select {
		case head := <-session.Events:
			head.Release()
		case <-ctx.Done():
			t.Fatal("Native output did not start")
		}
		select {
		case terminal := <-session.Events:
			if terminal.Event.GetNativeEnd() == nil {
				t.Fatal("expected terminal Native response", terminal.Event)
			}
		case <-ctx.Done():
			t.Fatal("released reply peaks consumed the terminal output budget", runtime.Snapshot())
		}
	}
	snapshot := runtime.Snapshot()
	if snapshot.Paused != 0 || snapshot.Active != 0 || snapshot.WorkingBytes != 0 || snapshot.Publishers != 32 || runtime.nativeMemory.Snapshot().Terminal != 32 {
		t.Fatal("terminal output retained peak working reservations", snapshot)
	}
	work := plan(100, "healthy", true)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if _, err := ticket.Wait(ctx); err != nil {
		t.Fatal("terminal consumers prevented healthy backend progress", err)
	}
	ticket.Ack()
}
