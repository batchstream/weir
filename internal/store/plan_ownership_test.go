package store

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type growingReadResultAdapter struct {
	lifecycleAdapter
	events []*pb.Event
}

func (a *growingReadResultAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) {
	for index, work := range plans {
		_ = emit(work, a.events[index])
	}
}

func TestReusablePlanResultReservationsRemainIndependent(t *testing.T) {
	var events []*pb.Event
	for _, size := range []int{4096, 8192} {
		document := &pb.Document{ContentType: "application/octet-stream", Data: make([]byte, size)}
		result := protocol.ReadDocument(document)
		value := &pb.Event_ReadResult{ReadResult: result}
		event := &pb.Event{Value: value}
		events = append(events, event)
	}
	adapter := &growingReadResultAdapter{events: events}
	runtime := newRuntime(adapter, DefaultLimits())
	work := plan(1, "shared", true)
	work.ResultBytes = execution.ResultOverheadBytes
	estimate := work.ResultBytes
	firstContext, cancelFirst := context.WithCancel(t.Context())
	defer cancelFirst()
	secondContext, cancelSecond := context.WithCancel(t.Context())
	defer cancelSecond()
	first, failure, _ := runtime.Submit(firstContext, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	second, failure, _ := runtime.Submit(secondContext, work, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != 2*estimate {
		t.Fatal("reused plan shared its initial result reservation", snapshot)
	}
	selected, _ := runtime.selectLocked(time.Now())
	if selected == nil || len(selected.items) != 2 {
		t.Fatal("compatible reused read plan did not share one batch")
	}
	runtime.execute(selected)
	firstResult, secondResult := recordEvent(t, first), recordEvent(t, second)
	if firstResult != events[0] || secondResult != events[1] {
		t.Fatal("reused plan crossed caller results")
	}
	firstBytes, secondBytes := proto.Size(firstResult), proto.Size(secondResult)
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != firstBytes+secondBytes {
		t.Fatal("growing results did not reserve each caller independently", snapshot, firstBytes, secondBytes)
	}
	if work.ResultBytes != estimate || work.Context != nil {
		t.Fatal("admission or result accounting mutated a reusable prepared plan")
	}
	first.Ack()
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != secondBytes || snapshot.Retained != 1 {
		t.Fatal("first acknowledgement consumed another caller's result reservation", snapshot)
	}
	second.Ack()
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("reused plan leaked or over-released result reservations", snapshot)
	}
}

func TestReusablePlanConcurrentBatchesPreserveResultAccounting(t *testing.T) {
	document := &pb.Document{ContentType: "application/octet-stream", Data: make([]byte, 4096)}
	result := protocol.ReadDocument(document)
	value := &pb.Event_ReadResult{ReadResult: result}
	event := &pb.Event{Value: value}
	adapter := &growingReadResultAdapter{events: []*pb.Event{event}}
	limits := DefaultLimits()
	limits.BatchOperations = 1
	limits.MaxInFlight = 0
	runtime := newRuntime(adapter, limits)
	work := plan(1, "shared", true)
	work.ResultBytes = execution.ResultOverheadBytes
	var tickets []*Ticket
	for range 64 {
		ticket, failure, _ := runtime.Submit(t.Context(), work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	var batches []*batch
	for range tickets {
		selected, _ := runtime.selectLocked(time.Now())
		if selected == nil || len(selected.items) != 1 {
			t.Fatal("reused plan did not produce independent bounded batches")
		}
		batches = append(batches, selected)
	}
	var executions sync.WaitGroup
	for _, selected := range batches {
		executions.Add(1)
		go func() {
			defer executions.Done()
			runtime.execute(selected)
		}()
	}
	executions.Wait()
	bytes := proto.Size(event)
	if snapshot := runtime.Snapshot(); snapshot.ResultBytes != len(tickets)*bytes || snapshot.Active != 0 {
		t.Fatal("concurrent growing results shared accounting", snapshot)
	}
	for index, ticket := range tickets {
		if recordEvent(t, ticket) != event {
			t.Fatal("concurrent reused plan lost its result")
		}
		ticket.Ack()
		if snapshot := runtime.Snapshot(); snapshot.ResultBytes != (len(tickets)-index-1)*bytes {
			t.Fatal("acknowledgement over-released a concurrent reservation", snapshot)
		}
	}
}
