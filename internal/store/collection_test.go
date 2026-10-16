package store

import (
	"context"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestReadBypassesCollectionAndSharesReadyBatch(t *testing.T) {
	for _, test := range []struct {
		name  string
		reads []bool
	}{
		{name: "single read", reads: []bool{true}},
		{name: "compatible reads", reads: []bool{true, true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchWait = time.Hour
			limits.BatchOperations = 3
			runtime := newRuntime(nil, limits)
			base := time.Now()
			var tickets []*Ticket
			for i, read := range test.reads {
				work := plan(uint64(i), string(rune('a'+i)), read)
				ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				ticket.queuedAt = base
				tickets = append(tickets, ticket)
			}
			selected, _ := runtime.selectLocked(base)
			if selected == nil || len(selected.items) != len(tickets) {
				t.Fatal("read waited for collection or lost compatible work", selected)
			}
			for i, ticket := range selected.items {
				if ticket != tickets[i] {
					t.Fatal("read changed compatible admission order")
				}
			}
			finish(runtime, selected)
			for _, ticket := range tickets {
				ticket.Ack()
			}
		})
	}
}

func TestReadsPreserveWriteCollectionAndAggregation(t *testing.T) {
	for _, readFirst := range []bool{false, true} {
		name := "write-first"
		if readFirst {
			name = "read-first"
		}
		t.Run(name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchWait = time.Second
			limits.BatchOperations = 3
			runtime := newRuntime(nil, limits)
			base := time.Now()
			var reads, writes []*Ticket
			for i, read := range []bool{readFirst, !readFirst, readFirst, !readFirst} {
				work := plan(uint64(i), string(rune('a'+i)), read)
				work.BatchKey = "same-namespace"
				session := runtime.NewSession()
				defer session.Close()
				ticket, failure, _ := runtime.Submit(t.Context(), work, session)
				if failure != nil {
					t.Fatal(failure)
				}
				ticket.queuedAt = base
				if read {
					reads = append(reads, ticket)
				} else {
					writes = append(writes, ticket)
				}
			}
			selected, _ := runtime.selectLocked(base)
			if selected == nil || len(selected.items) != 2 || selected.items[0] != reads[0] || selected.items[1] != reads[1] {
				t.Fatal("reads waited for compatible writes or lost cross-RPC aggregation", selected)
			}
			finish(runtime, selected)
			selected, next := runtime.selectLocked(base.Add(100 * time.Millisecond))
			if selected != nil || !next.Equal(base.Add(time.Second)) {
				t.Fatal("reads flushed writes or changed their original collection deadline", selected, next)
			}
			work := plan(4, "last-write", false)
			work.BatchKey = "same-namespace"
			ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			ticket.queuedAt = base.Add(100 * time.Millisecond)
			writes = append(writes, ticket)
			selected, _ = runtime.selectLocked(ticket.queuedAt)
			if selected == nil || len(selected.items) != 3 {
				t.Fatal("later compatible write did not fill the retained write batch", selected)
			}
			for i, ticket := range selected.items {
				if ticket != writes[i] {
					t.Fatal("writes lost cross-RPC order or identity")
				}
			}
			finish(runtime, selected)
			for _, ticket := range append(reads, writes...) {
				ticket.Ack()
			}
			if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("separate groups leaked queue, execution or result credits", snapshot)
			}
		})
	}
}

func TestReadyWritesKeepProgressAheadOfLaterReads(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Second
	limits.MaxInFlight = 1
	runtime := newRuntime(nil, limits)
	base := time.Now()
	var tickets []*Ticket
	for i, read := range []bool{false, true, false, true} {
		work := plan(uint64(i), string(rune('a'+i)), read)
		work.BatchKey = "same-namespace"
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket.queuedAt = base
		tickets = append(tickets, ticket)
	}
	for _, want := range [][]*Ticket{{tickets[0], tickets[2]}, {tickets[1], tickets[3]}} {
		selected, _ := runtime.selectLocked(base.Add(limits.BatchWait))
		if selected == nil || len(selected.items) != len(want) {
			t.Fatal("ready write group lost priority or read/write groups mixed", selected)
		}
		for i, ticket := range selected.items {
			if ticket != want[i] {
				t.Fatal("later reads overtook an eligible write group")
			}
		}
		if blocked, _ := runtime.selectLocked(base.Add(limits.BatchWait)); blocked != nil {
			t.Fatal("separate groups bypassed their shared execution budget", blocked)
		}
		finish(runtime, selected)
		for _, ticket := range selected.items {
			ticket.Ack()
		}
	}
}

func TestImmediateReadsRespectBatchCountAndByteBounds(t *testing.T) {
	for _, test := range []struct {
		name       string
		operations int
		bytes      int
	}{
		{name: "count", operations: 2, bytes: 8 << 20},
		{name: "bytes", operations: 8, bytes: 2048},
	} {
		t.Run(test.name, func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchWait = time.Hour
			limits.BatchOperations, limits.BatchBytes = test.operations, test.bytes
			runtime := newRuntime(nil, limits)
			base := time.Now()
			var tickets []*Ticket
			for i := range 3 {
				work := plan(uint64(i), string(rune('a'+i)), true)
				ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				ticket.queuedAt = base
				tickets = append(tickets, ticket)
			}
			first, _ := runtime.selectLocked(base)
			if first == nil || len(first.items) != 2 {
				t.Fatal("immediate reads ignored their batch bound", first)
			}
			finish(runtime, first)
			last, _ := runtime.selectLocked(base)
			if last == nil || len(last.items) != 1 || last.items[0] != tickets[2] {
				t.Fatal("remaining read waited for collection", last)
			}
			finish(runtime, last)
			for _, ticket := range tickets {
				ticket.Ack()
			}
		})
	}
}

func TestImmediateReadStillWaitsForExecutionCapacity(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	limits.MaxInFlight = 1
	runtime := newRuntime(nil, limits)
	base := time.Now()
	write := plan(0, "write", false)
	writeTicket, failure, _ := runtime.Submit(t.Context(), write, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	writeTicket.queuedAt = base.Add(-limits.BatchWait)
	active, _ := runtime.selectLocked(base)
	if active == nil {
		t.Fatal("expired write did not occupy the execution slot")
	}
	read := plan(1, "read", true)
	readTicket, failure, _ := runtime.Submit(t.Context(), read, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	readTicket.queuedAt = base
	blocked, _ := runtime.selectLocked(base)
	if blocked != nil {
		t.Fatal("read bypassed the occupied execution slot", blocked)
	}
	finish(runtime, active)
	ready, _ := runtime.selectLocked(base)
	if ready == nil || len(ready.items) != 1 || ready.items[0] != readTicket {
		t.Fatal("read collected after the execution slot was released", ready)
	}
	finish(runtime, ready)
	writeTicket.Ack()
	readTicket.Ack()
}

func TestCollectionExpiresFromOldestAdmissionAndFullBatchBypassesWait(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Second
	limits.BatchOperations = 3
	runtime := newRuntime(nil, limits)
	base := time.Now()
	var tickets []*Ticket
	for i := range 2 {
		work := plan(uint64(i), string(rune('a'+i)), false)
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket.queuedAt = base.Add(time.Duration(i) * time.Millisecond)
		tickets = append(tickets, ticket)
	}
	selected, next := runtime.selectLocked(base.Add(100 * time.Millisecond))
	if selected != nil || !next.Equal(base.Add(time.Second)) {
		t.Fatal("collection deadline moved with a later arrival", selected, next)
	}
	work := plan(2, "c", false)
	ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket.queuedAt = base.Add(100 * time.Millisecond)
	tickets = append(tickets, ticket)
	selected, _ = runtime.selectLocked(base.Add(100 * time.Millisecond))
	if selected == nil || len(selected.items) != 3 {
		t.Fatal("full batch waited for a timer", selected)
	}
	finish(runtime, selected)
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestReadyNamespaceBypassesAnotherCollectingNamespace(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Second
	limits.BatchOperations = 2
	runtime := newRuntime(nil, limits)
	base := time.Now()
	var tickets []*Ticket
	for i, namespace := range []string{"collecting", "ready", "ready"} {
		work := plan(uint64(i), string(rune('a'+i)), false)
		work.BatchKey = namespace
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket.queuedAt = base
		tickets = append(tickets, ticket)
	}
	selected, _ := runtime.selectLocked(base)
	if selected == nil || len(selected.items) != 2 || selected.items[0] != tickets[1] {
		t.Fatal("collecting namespace blocked a full batch", selected)
	}
	finish(runtime, selected)
	selected, next := runtime.selectLocked(base)
	if selected != nil || !next.Equal(base.Add(time.Second)) {
		t.Fatal("remaining namespace lost its collection deadline", selected, next)
	}
	selected, _ = runtime.selectLocked(next)
	if selected == nil || len(selected.items) != 1 || selected.items[0] != tickets[0] {
		t.Fatal("expired batch did not dispatch", selected)
	}
	finish(runtime, selected)
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestCollectionDoesNotConsumeShortCallerDeadline(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	runtime := newRuntime(nil, limits)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	work := plan(1, "record", false)
	ticket, failure, _ := runtime.Submit(ctx, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	selected, _ := runtime.selectLocked(time.Now())
	if selected == nil || selected.items[0] != ticket {
		t.Fatal("collection held a caller past its deadline", selected)
	}
	finish(runtime, selected)
	ticket.Ack()
}

func TestCollectionFlushesAtByteBoundary(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	limits.BatchBytes = 1536
	runtime := newRuntime(nil, limits)
	var tickets []*Ticket
	for i := range 2 {
		work := plan(uint64(i), string(rune('a'+i)), false)
		ticket, failure, _ := runtime.Submit(context.Background(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	selected, _ := runtime.selectLocked(time.Now())
	if selected == nil || len(selected.items) != 1 || selected.items[0] != tickets[0] {
		t.Fatal("byte-limited batch waited or exceeded its limit", selected)
	}
	finish(runtime, selected)
	runtime.BeginDrain()
	selected, _ = runtime.selectLocked(time.Now())
	if selected == nil || len(selected.items) != 1 || selected.items[0] != tickets[1] {
		t.Fatal("byte-limited successor was lost", selected)
	}
	finish(runtime, selected)
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestNativeBypassesCollection(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	runtime := newRuntime(nil, limits)
	session := runtime.NewSession()
	work := plan(1, "stream", false)
	request := &pb.NativeRequest{}
	work.Command.Operation = &pb.Command_Native{Native: request}
	ticket, failure, _ := runtime.Submit(context.Background(), work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	selected, _ := runtime.selectLocked(time.Now())
	if selected == nil || len(selected.items) != 1 || selected.items[0] != ticket {
		t.Fatal("Native command waited for collection", selected)
	}
	finish(runtime, selected)
	ticket.Ack()
	session.Close()
}

func TestCollectionTimerDispatchesAndDrainBypassesWait(t *testing.T) {
	for _, drain := range []bool{false, true} {
		t.Run(map[bool]string{false: "timer", true: "drain"}[drain], func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchWait = 30 * time.Millisecond
			if drain {
				limits.BatchWait = time.Hour
			}
			adapter := &crossRequestAdapter{calls: make(chan crossRequestCall, 1)}
			runtime := newRuntime(adapter, limits)
			var tickets []*Ticket
			for i := range 2 {
				work := plan(uint64(i), string(rune('a'+i)), false)
				ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				tickets = append(tickets, ticket)
			}
			if drain {
				runtime.BeginDrain()
			}
			go runtime.loop()
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			select {
			case call := <-adapter.calls:
				if len(call.plans) != 2 {
					t.Fatal("independent calls did not share one execution", len(call.plans))
				}
			case <-time.After(time.Second):
				t.Fatal("collection timer or drain did not wake the scheduler")
			}
			for _, ticket := range tickets {
				_ = recordEvent(t, ticket)
				ticket.Ack()
			}
		})
	}
}

func TestCollectionPreservesSameResourceOrderingAndCancellation(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Second
	runtime := newRuntime(nil, limits)
	session := runtime.NewSession()
	defer session.Close()
	ctx, cancel := context.WithCancel(t.Context())
	work := plan(1, "same", false)
	first, failure, _ := runtime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	work = plan(2, "same", false)
	second, failure, _ := runtime.Submit(t.Context(), work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	cancel()
	runtime.cancelQueuedLocked()
	if first.state != 2 || first.event.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
		t.Fatal("canceled collection work did not retain NOT_STARTED evidence", first.event)
	}
	selected, _ := runtime.selectLocked(second.queuedAt.Add(time.Second))
	if selected == nil || len(selected.items) != 1 || selected.items[0] != second {
		t.Fatal("canceled predecessor blocked or reordered its successor", selected)
	}
	finish(runtime, selected)
	first.Ack()
	second.Ack()
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Retained != 0 {
		t.Fatal("collection cancellation leaked accounting", snapshot)
	}
}
