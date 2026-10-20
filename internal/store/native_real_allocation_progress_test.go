package store

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
)

// Observe real Native reservation boundaries without replacing allocation,
// transport, execution, or publication with synthetic adapter behavior.
type realAllocationAdapter struct {
	execution.Adapter
	grows chan int
	ends  chan error
}

type realAllocationIO struct {
	execution.NativeIO
	observer *realAllocationAdapter
}

func (a *realAllocationAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	for _, work := range plans {
		if work.NativeIO != nil {
			observer := &realAllocationIO{NativeIO: work.NativeIO, observer: a}
			work.NativeIO = observer
		}
	}
	a.Adapter.Execute(ctx, plans, emit)
}

func (observer *realAllocationIO) GrowIO(bytes int) error {
	err := observer.NativeIO.GrowIO(bytes)
	if err == nil {
		observer.observer.grows <- bytes
	}
	return err
}

func (observer *realAllocationIO) EndIO(working, eventBytes int) error {
	err := observer.NativeIO.EndIO(working, eventBytes)
	observer.observer.ends <- err
	return err
}

func TestRealSearchNativeInitialIOReleasesOldestRecordAllocation(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	releaseHead := make(chan struct{})
	readEntered := make(chan struct{})
	releaseRead := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
		case "/records":
			_, _ = io.WriteString(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`)
		case "/native":
			connection, buffered, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer connection.Close()
			_, _ = buffered.WriteString("HTTP/1.1 200 OK\r\nX-Witness: initial\r\n")
			_ = buffered.Flush()
			select {
			case <-releaseHead:
			case <-ctx.Done():
				return
			}
			_, _ = buffered.WriteString("Content-Length: 1\r\n\r\nx")
			_ = buffered.Flush()
		case "/records/_mget":
			close(readEntered)
			select {
			case <-releaseRead:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"item","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
		default:
			t.Error("unexpected backend request", request.URL.Path)
			http.NotFound(w, request)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer cancel()
	memory := NewExecutionMemory()
	memory.known, memory.trusted, memory.budget = true, true, 96<<20
	limits := DefaultLimits()
	limits.BatchWait = 0
	openRuntime := func() (*Runtime, *realAllocationAdapter) {
		config := search.Config{Store: "search", URL: server.URL}
		adapter, err := search.Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		observer := &realAllocationAdapter{Adapter: adapter, grows: make(chan int, 16), ends: make(chan error, 16)}
		runtime, err := New(observer, limits, memory)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			cancel()
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if err := runtime.Close(cleanup); err != nil {
				t.Error(err)
			}
		})
		return runtime, observer
	}
	nativeRuntime, observer := openRuntime()
	recordRuntime, _ := openRuntime()
	session := nativeRuntime.NewSession()
	defer session.Close()
	document := &pb.Document{ContentType: "application/http", Data: []byte("GET /native HTTP/1.1\r\nHost: witness\r\n\r\n")}
	request := &pb.NativeRequest{Request: document}
	operation := &pb.Command_Native{Native: request}
	command := &pb.Command{Operation: operation}
	work, failure := nativeRuntime.PrepareCommand(ctx, 1, command)
	if failure != nil {
		t.Fatal(failure)
	}
	native, failure, _ := nativeRuntime.Submit(ctx, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	// One GrowIO is executeNative's real initial allocation; the second comes
	// from bytes entering its real HTTP header parser before the first EndIO.
	for index := range 2 {
		select {
		case bytes := <-observer.grows:
			t.Logf("real Native GrowIO index=%d working=%d", index, bytes)
		case <-ctx.Done():
			t.Fatal("real Native initial/header GrowIO not reached", memory.Snapshot())
		}
	}
	lua := &pb.LuaTransform{Source: []byte(`return function() local s = 'x'; for i=1,18 do s=s..s end; return weir.keep() end`)}
	form := &pb.Transform_Lua{Lua: lua}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: "records/s:item", Action: action}
	mutationOperation := &pb.Command_Mutate{Mutate: mutation}
	mutationCommand := &pb.Command{Operation: mutationOperation}
	record, err := execution.NewRecord("search", 2, mutationCommand)
	if err != nil {
		t.Fatal(err)
	}
	recordWork, failure := recordRuntime.PrepareRecord(ctx, record)
	if failure != nil {
		t.Fatal(failure)
	}
	ticket, failure, _ := recordRuntime.Submit(ctx, recordWork, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	defer ticket.Ack()
	select {
	case <-readEntered:
	case <-ctx.Done():
		t.Fatal("real record pre-read not reached", memory.Snapshot())
	}
	// Test-only capacity shrink, not a production quota or workload change.
	// Keep all existing owners charged and leave insufficient room for Lua's
	// real allocation. Only the already preparing Native can donate working.
	memory.mu.Lock()
	memory.budget = memory.used + 8192
	memory.mu.Unlock()
	close(releaseRead)
	poll := time.NewTicker(time.Millisecond)
	defer poll.Stop()
	for {
		memory.mu.Lock()
		waits := len(memory.workingWaits)
		captured := false
		if waits == 1 {
			_, captured = memory.workingWaits[0].donors[native.executionLease]
		}
		preparing := native.executionLease.preparing
		memory.mu.Unlock()
		if captured && preparing {
			t.Logf("real allocation waiting: claims=%d preparing=%v ledger=%+v", waits, preparing, memory.Snapshot())
			break
		}
		select {
		case <-ticket.ready:
			event, waitErr := ticket.Wait(ctx)
			t.Fatal("record finished without the intended real allocation wait", event, waitErr, memory.Snapshot())
		case <-poll.C:
		case <-ctx.Done():
			t.Fatal("record did not capture initial Native donor", memory.Snapshot(), recordRuntime.Snapshot())
		}
	}
	close(releaseHead)
	select {
	case err := <-observer.ends:
		if err != nil {
			t.Fatal("real initial EndIO failed", err, memory.Snapshot())
		}
	case <-ctx.Done():
		t.Fatal("real Native EndIO blocked behind record growth", memory.Snapshot(), nativeRuntime.Snapshot(), recordRuntime.Snapshot())
	}
	// Neither consume nor acknowledge Native output until the record progresses.
	event, err := ticket.Wait(ctx)
	if err != nil || event.GetMutationResult().GetFailure() != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("oldest real record allocation did not progress before Native Ack", event, err, memory.Snapshot())
	}
	memory.mu.Lock()
	waits, preparing := len(memory.workingWaits), native.executionLease.preparing
	memory.mu.Unlock()
	if waits != 0 || preparing {
		t.Fatal("real EndIO/record completion retained initial wait state", waits, preparing, memory.Snapshot())
	}
	t.Log(fmt.Sprintf("real EndIO and oldest Lua record completed before Native Ack/new starts: ledger=%+v", memory.Snapshot()))
}
