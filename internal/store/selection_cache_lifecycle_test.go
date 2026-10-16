package store

import (
	"fmt"
	"slices"
	"testing"
	"time"
)

func TestWaitingSelectionRefreshesAfterAdmissionAndFlushesOnDrain(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = time.Hour
	limits.BatchOperations = 3
	runtime := newRuntime(nil, limits, nil)
	firstWork := plan(1, "first", false)
	first, failure, _ := runtime.Submit(t.Context(), firstWork, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	selected, next := runtime.selectLocked(time.Now())
	if selected != nil || next.IsZero() {
		t.Fatal("partial mutation batch did not collect", selected, next)
	}

	var tickets []*Ticket
	tickets = append(tickets, first)
	for index := uint64(2); index <= 3; index++ {
		work := plan(index, fmt.Sprint(index), false)
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	selected, _ = runtime.selectLocked(time.Now())
	if selected == nil || !slices.Equal(selected.items, tickets) {
		t.Fatal("new admissions did not fill the collecting batch", selected)
	}
	finish(runtime, selected)
	for _, ticket := range tickets {
		ticket.Ack()
	}

	lastWork := plan(4, "last", false)
	last, failure, _ := runtime.Submit(t.Context(), lastWork, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	selected, next = runtime.selectLocked(time.Now())
	if selected != nil || next.IsZero() {
		t.Fatal("new partial batch did not collect", selected, next)
	}
	runtime.BeginDrain()
	selected, _ = runtime.selectLocked(time.Now())
	if selected == nil || len(selected.items) != 1 || selected.items[0] != last {
		t.Fatal("drain did not release a cached collecting batch", selected)
	}
	finish(runtime, selected)
	last.Ack()
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("collecting batch lifecycle leaked capacity", snapshot)
	}
}

func TestCachedOversizeSuccessorSurvivesEarlierAcknowledgements(t *testing.T) {
	limits := DefaultLimits()
	limits.BatchWait = 0
	limits.BatchBytes = 1024
	runtime := newRuntime(nil, limits, nil)
	var tickets []*Ticket
	for index, size := range []int{512, 2048, 512} {
		work := plan(uint64(index), fmt.Sprint(index), false)
		work.Bytes = size
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	first, _ := runtime.selectLocked(time.Now())
	if first == nil || len(first.items) != 2 || first.items[0] != tickets[0] || first.items[1] != tickets[2] {
		t.Fatal("byte overflow prevented compatible small records from batching", first)
	}
	finish(runtime, first)
	tickets[0].Ack()
	tickets[2].Ack()
	second, _ := runtime.selectLocked(time.Now())
	if second == nil || len(second.items) != 1 || second.items[0] != tickets[1] {
		t.Fatal("acknowledging earlier members corrupted the cached oversized successor", second)
	}
	finish(runtime, second)
	tickets[1].Ack()
	if snapshot := runtime.Snapshot(); snapshot.Pending != 0 || snapshot.PendingBytes != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("cached oversized successor leaked capacity", snapshot)
	}
}
