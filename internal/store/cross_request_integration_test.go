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

func TestNativeCoalescedWindowsKeepLargeResultsAndRetainedOwners(t *testing.T) {
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
	for _, count := range []int{31, 1} {
		request := &pb.ReadBatch{}
		for range count {
			read := &pb.ReadRequest{Resource: fixture.DB + "/records/s:same"}
			request.Requests = append(request.Requests, read)
		}
		records, failure := execution.NewReadRecords("mongo", request.Requests, runtime.PendingByteLimit())
		if failure != nil {
			t.Fatal(failure)
		}
		prepared, failure := runtime.PrepareBatch(records)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, submitCrossBatch(t, runtime, ctx, prepared))
	}
	selected := selectCrossBatch(runtime)
	if selected == nil || len(selected.items) != 2 {
		t.Fatal("compatible actual database RPCs did not coalesce")
	}
	runtime.execute(selected)
	results := crossResults(t, tickets[0])
	successes := 0
	for position, result := range results {
		if result.Index != uint64(position+1) {
			t.Fatal("first caller results were reordered")
		}
		if document := result.Read.GetDocument(); document != nil {
			successes++
			if !bytes.Equal(document.Data, raw) {
				t.Fatal("stored document changed in aggregated response")
			}
		} else {
			t.Fatal("admitted window lost its reserved read result", result)
		}
	}
	peer := crossResults(t, tickets[1])[0]
	if successes != len(results) || successes*len(raw) <= 32<<20 || finds.Load() != 1 || peer.Index != 1 || !bytes.Equal(peer.Read.GetDocument().GetData(), raw) {
		t.Fatal("large result window was truncated or lost peer isolation", successes, len(results), finds.Load(), peer.Read.GetFailure())
	}
	tickets[0].Ack()
	if snapshot := runtime.Snapshot(); snapshot.Retained != 1 || snapshot.ResultBytes != execution.ResultOverheadBytes+len(raw) || snapshot.WorkingBytes != 0 {
		t.Fatal("first caller did not release only its own response charge", snapshot)
	}
	if !bytes.Equal(peer.Read.GetDocument().GetData(), raw) {
		t.Fatal("first caller release invalidated slower peer's document")
	}
	tickets[1].Ack()
	waitReleased(t, runtime)
	if snapshot := runtime.Snapshot(); snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 {
		t.Fatal("aggregated native read leaked byte ownership", snapshot)
	}
}
