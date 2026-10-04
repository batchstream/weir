//go:build integration

package store

import (
	"bytes"
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
)

func TestNativeCoalescedRecordsKeepLargeResultsAndRetainedOwners(t *testing.T) {
	fixture := testmongo.Open(t)
	document := bson.D{{Key: "_id", Value: "same"}, {Key: "pad", Value: strings.Repeat("x", 1100<<10)}}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(t.Context(), document); err != nil {
		t.Fatal(err)
	}
	raw, err := bson.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	var finds atomic.Int32
	proxy := testmongo.StartProxy(t, fixture)
	proxy.Monitor = &event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
		if e.CommandName == "find" {
			finds.Add(1)
		}
	}}
	config := mongodb.Config{URI: proxy.URI(), Store: "mongo", Pool: 1, MaxReadSize: len(raw)}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := adapter.Close(); err != nil {
			t.Error(err)
		}
	}()
	limits := DefaultLimits()
	limits.Concurrency = 1
	limits.ResultBytes = 64 << 20
	runtime := newRuntime(adapter, limits)
	var tickets []*Ticket
	for index := range 32 {
		read := &pb.ReadRequest{Resource: fixture.DB + "/records/s:same"}
		operation := &pb.Command_Read{Read: read}
		command := &pb.Command{Operation: operation}
		ordinal := uint64(index + 1)
		if index == 31 {
			ordinal = 1
		}
		record, err := execution.NewRecord("mongo", ordinal, command)
		if err != nil {
			t.Fatal(err)
		}
		work, failure := runtime.PrepareRecord(record)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket, failure, _ := runtime.Submit(ctx, work, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	selected := selectCrossBatch(runtime)
	if selected == nil || len(selected.items) != 32 {
		t.Fatal("compatible actual database RPCs did not coalesce")
	}
	runtime.execute(selected)
	successes := 0
	for position, ticket := range tickets[:31] {
		result, err := ticket.Wait(ctx)
		if err != nil || ticket.plan.ID != uint64(position+1) {
			t.Fatal("first caller results lost association", err, ticket.plan.ID)
		}
		document := result.GetReadResult().GetDocument()
		if !bytes.Equal(document.GetData(), raw) {
			t.Fatal("admitted read lost its reserved result or changed the document", result)
		}
		successes++
	}
	peer, err := tickets[31].Wait(ctx)
	if err != nil || successes*len(raw) <= 32<<20 || finds.Load() != 1 || tickets[31].plan.ID != 1 || !bytes.Equal(peer.GetReadResult().GetDocument().GetData(), raw) {
		t.Fatal("large aggregate was truncated or lost peer isolation", err, successes, finds.Load(), peer)
	}
	for _, ticket := range tickets[:31] {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.ResultBytes != execution.ResultOverheadBytes+len(raw) || snapshot.WorkingBytes != 0 {
		t.Fatal("first caller did not release only its own response charges", snapshot)
	}
	if !bytes.Equal(peer.GetReadResult().GetDocument().GetData(), raw) {
		t.Fatal("first caller release invalidated slower peer's document")
	}
	tickets[31].Ack()
	waitReleased(t, runtime)
	if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("aggregated native read leaked byte ownership", snapshot)
	}
}
