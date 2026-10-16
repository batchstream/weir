package store

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

func TestSelectionRoundRefillsInAdmissionOrder(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = 0
	limits.BatchOperations = 2
	runtime := newRuntime(nil, limits, nil)
	var tickets []*Ticket
	for index, group := range []string{"a", "a", "a", "b", "a", "c", "b"} {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.BatchKey = group
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	for _, want := range [][]uint64{{0, 1}, {2, 4}, {3, 6}, {5}} {
		selected, _ := runtime.selectLocked(time.Now())
		if selected == nil {
			t.Fatal("admitted group failed to progress", want)
		}
		var got []uint64
		for _, ticket := range selected.items {
			got = append(got, ticket.plan.ID)
		}
		if !slices.Equal(got, want) {
			t.Fatal("refilled group lost oldest admission priority", got, want)
		}
		finish(runtime, selected)
	}
	for _, ticket := range tickets {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("selection round leaked capacity", snapshot)
	}
}

func TestSelectionRoundCancellationRefreshesCachedGroups(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = 0
	limits.BatchOperations = 1
	runtime := newRuntime(nil, limits, nil)
	canceledContext, cancel := context.WithCancel(t.Context())
	defer cancel()
	var tickets []*Ticket
	for index := range 4 {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.BatchKey = fmt.Sprint(index % 3)
		ctx := t.Context()
		if index == 1 {
			ctx = canceledContext
		}
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	first, _ := runtime.selectLocked(time.Now())
	if first == nil || first.items[0] != tickets[0] {
		t.Fatal("oldest operation did not dispatch")
	}
	cancel()
	runtime.cancelQueuedLocked()
	if event := recordEvent(t, tickets[1]); event.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED {
		t.Fatal("cached cancellation lost definitive queued outcome", event)
	}
	for _, want := range []*Ticket{tickets[2], tickets[3]} {
		selected, _ := runtime.selectLocked(time.Now())
		if selected == nil || selected.items[0] != want {
			t.Fatal("cancellation reordered independent cached work")
		}
		finish(runtime, selected)
	}
	finish(runtime, first)
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestSelectionRoundSlotBoundPreservesOmittedOlderGroups(t *testing.T) {
	for _, slots := range []int{1, 2} {
		t.Run(fmt.Sprintf("slots=%d", slots), func(t *testing.T) {
			limits := DefaultLimits()
			limits.BatchWait = 0
			limits.BatchOperations = 2
			limits.MaxInFlight = slots
			runtime := newRuntime(nil, limits, nil)
			var tickets []*Ticket
			for index, group := range []string{"a", "a", "b", "c", "a", "b", "d", "a"} {
				work := plan(uint64(index), "same-record", false)
				work.BatchKey = group
				ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
				if failure != nil {
					t.Fatal(failure)
				}
				tickets = append(tickets, ticket)
			}
			for _, want := range tickets {
				selected, _ := runtime.selectLocked(time.Now())
				if selected == nil || len(selected.items) != 1 || selected.items[0] != want {
					t.Fatal("refilled group overtook an older group outside this round", want.plan.ID)
				}
				finish(runtime, selected)
			}
			for _, ticket := range tickets {
				ticket.Ack()
			}
		})
	}
}

func TestSelectionRoundSlotBoundDoesNotHideReadyReads(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Second
	limits.MaxInFlight = 1
	runtime := newRuntime(nil, limits, nil)
	base := time.Now()
	var tickets []*Ticket
	for index := range 3 {
		work := plan(uint64(index), fmt.Sprint(index), index > 0)
		work.BatchKey = fmt.Sprint(index)
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket.queuedAt = base
		tickets = append(tickets, ticket)
	}
	for _, want := range tickets[1:] {
		selected, _ := runtime.selectLocked(base)
		if selected == nil || selected.items[0] != want {
			t.Fatal("collecting write hid an immediately ready read beyond the slot bound", want.plan.ID)
		}
		finish(runtime, selected)
	}
	selected, next := runtime.selectLocked(base)
	if selected != nil || !next.Equal(base.Add(time.Second)) {
		t.Fatal("read dispatch changed the remaining write collection window", next)
	}
	selected, _ = runtime.selectLocked(next)
	if selected == nil || selected.items[0] != tickets[0] {
		t.Fatal("remaining write did not dispatch at its original deadline")
	}
	finish(runtime, selected)
	for _, ticket := range tickets {
		ticket.Ack()
	}
}

func TestSelectionRoundPreservesSessionOrderUnderFragmentation(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueOperations = 512
	limits.BatchOperations = 7
	limits.BatchWait = 0
	limits.MaxInFlight = 4
	runtime := newRuntime(nil, limits, nil)
	var sessions []*Session
	for range 8 {
		session := runtime.NewSession()
		sessions = append(sessions, session)
		defer session.Close()
	}
	randomSource := rand.NewPCG(37, 91)
	random := rand.New(randomSource)
	var tickets []*Ticket
	var stops []context.CancelFunc
	for index := range 384 {
		work := plan(uint64(index), fmt.Sprintf("record-%d", random.IntN(6)), false)
		work.BatchKey = fmt.Sprintf("group-%d", random.IntN(19))
		ctx, cancel := context.WithCancel(t.Context())
		ticket, failure, _ := runtime.Submit(ctx, work, sessions[random.IntN(len(sessions))])
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
		stops = append(stops, cancel)
		defer cancel()
	}
	var active []*batch
	for remaining := len(tickets); remaining > 0; {
		if random.IntN(3) == 0 {
			stops[random.IntN(len(stops))]()
		}
		runtime.cancelQueuedLocked()
		selected, _ := runtime.selectLocked(time.Now())
		if selected != nil {
			for _, ticket := range selected.items {
				for _, earlier := range tickets[:ticket.plan.ID] {
					if earlier.session == ticket.session && earlier.plan.Key == ticket.plan.Key && earlier.state != 2 {
						t.Fatal("fragmented selection overtook a same-session predecessor", ticket.plan.ID, earlier.plan.ID)
					}
				}
			}
			active = append(active, selected)
		} else if len(active) != 0 {
			index := random.IntN(len(active))
			finish(runtime, active[index])
			active = append(active[:index], active[index+1:]...)
		} else {
			t.Fatal("pending admitted work stopped making progress", runtime.Snapshot())
		}
		remaining = 0
		for _, ticket := range tickets {
			if ticket.state != 2 {
				remaining++
			}
		}
	}
	for _, ticket := range tickets {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Active != 0 || snapshot.Retained != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("fragmented cancellation leaked execution or memory credit", snapshot)
	}
}
