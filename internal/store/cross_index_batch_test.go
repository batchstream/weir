package store

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
)

func crossIndexSearchRuntime(t *testing.T) *Runtime {
	t.Helper()
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			t.Error("preparing and selecting a record performed backend I/O", request.URL.Path)
			http.NotFound(w, request)
			return
		}
		fmt.Fprint(w, `{"version":{"build_flavor":"default"}}`)
	})
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	config := search.Config{Store: "search", URL: server.URL}
	adapter, err := search.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = adapter.Close() })
	runtime := newRuntime(adapter, DefaultLimits())
	return runtime
}

type crossIndexRecordOptions struct {
	resource string
	index    uint64
	visible  bool
}

func prepareCrossIndexRecord(t *testing.T, runtime *Runtime, options crossIndexRecordOptions) *execution.Plan {
	t.Helper()
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	action := &pb.MutateRequest_Put{Put: document}
	mutation := &pb.MutateRequest{Resource: options.resource, Action: action}
	if options.visible {
		mutation.Options = &pb.MutationOptions{Visibility: pb.MutationVisibility_QUERY_VISIBLE}
	}
	operation := &pb.Command_Mutate{Mutate: mutation}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", options.index, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := runtime.PrepareRecord(t.Context(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	return work
}

func TestSearchRecordBatchesSpanIndicesAndPreserveSessionOrder(t *testing.T) {
	runtime := crossIndexSearchRuntime(t)
	session := runtime.NewSession()
	defer session.Close()
	options := []crossIndexRecordOptions{
		{resource: "products/s:same", index: 1},
		{resource: "sellers/s:same", index: 2},
		{resource: "products/s:same", index: 3},
	}
	tickets := make([]*Ticket, len(options))
	for i, recordOptions := range options {
		work := prepareCrossIndexRecord(t, runtime, recordOptions)
		ticket, failure, _ := runtime.Submit(t.Context(), work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets[i] = ticket
	}
	first := selectCrossBatch(runtime)
	if first == nil || len(first.items) != 2 || first.items[0] != tickets[0] || first.items[1] != tickets[1] {
		t.Fatal("different indices did not share the physical batch", first)
	}
	if next := selectCrossBatch(runtime); next != nil {
		t.Fatal("same-session successor bypassed its earlier mutation", next)
	}
	runtime.mu.Lock()
	finish(runtime, first)
	runtime.mu.Unlock()
	second := selectCrossBatch(runtime)
	if second == nil || len(second.items) != 1 || second.items[0] != tickets[2] {
		t.Fatal("completion did not release the same-session successor", second)
	}
	runtime.mu.Lock()
	finish(runtime, second)
	runtime.mu.Unlock()
	for _, ticket := range tickets {
		ticket.Ack()
	}
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.WorkingBytes != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("cross-index ordering retained execution or result credit", snapshot)
	}
}

func TestSearchRecordBatchesKeepVisibilityAndStreamingBoundaries(t *testing.T) {
	runtime := crossIndexSearchRuntime(t)
	session := runtime.NewSession()
	defer session.Close()
	options := []crossIndexRecordOptions{
		{resource: "products/s:first", index: 1},
		{resource: "sellers/s:visible", index: 2, visible: true},
		{resource: "sellers/s:second", index: 3},
	}
	tickets := make([]*Ticket, 0, 5)
	for _, recordOptions := range options {
		work := prepareCrossIndexRecord(t, runtime, recordOptions)
		ticket, failure, _ := runtime.Submit(t.Context(), work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	for i, command := range []*pb.Command{scanCall(), nativeCall()} {
		work := plan(uint64(i+4), fmt.Sprintf("stream-%d", i), false)
		work.Command = command
		// Singleton commands must remain separate even when their grouping key
		// happens to equal that of record work.
		work.BatchKey = tickets[0].plan.BatchKey
		ticket, failure, _ := runtime.Submit(context.Background(), work, session)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	expected := [][]*Ticket{{tickets[0], tickets[2]}, {tickets[1]}, {tickets[3]}, {tickets[4]}}
	for _, group := range expected {
		selected := selectCrossBatch(runtime)
		if selected == nil || len(selected.items) != len(group) {
			t.Fatal("visibility or singleton command joined an incompatible batch", selected, len(group))
		}
		for i, ticket := range group {
			if selected.items[i] != ticket {
				t.Fatal("cross-index selection lost oldest compatible ordering")
			}
		}
		runtime.mu.Lock()
		finish(runtime, selected)
		runtime.mu.Unlock()
	}
	for _, ticket := range tickets {
		ticket.Ack()
	}
}
