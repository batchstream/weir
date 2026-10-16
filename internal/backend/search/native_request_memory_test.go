package search

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	goruntime "runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
)

func nativeManyHeaderRequest(headers int, body []byte) *pb.NativeRequest {
	var raw strings.Builder
	fmt.Fprintf(&raw, "GET /_search HTTP/1.1\r\nContent-Length: %d\r\n", len(body))
	for i := range headers {
		fmt.Fprintf(&raw, "X-%06d: v\r\n", i)
	}
	raw.WriteString("\r\n")
	raw.Write(body)
	document := &pb.Document{ContentType: "application/http", Data: []byte(raw.String())}
	request := &pb.NativeRequest{Resource: "records", Request: document}
	return request
}

func TestNativePrepareDoesNotAllocateParsedHeadersBeforeAdmission(t *testing.T) {
	adapter := &Adapter{}
	small, large := nativeManyHeaderRequest(1, nil), nativeManyHeaderRequest(100000, nil)
	measure := func(request *pb.NativeRequest) float64 {
		return testing.AllocsPerRun(3, func() {
			work, failure := adapter.prepareNative(request)
			if failure != nil || work.Backend.(*nativePlan).raw == nil {
				t.Fatal(failure)
			}
		})
	}
	smallAllocs, largeAllocs := measure(small), measure(large)
	if largeAllocs > smallAllocs+2 || largeAllocs > 20 {
		t.Fatal("pre-admission allocations grew with header count", smallAllocs, largeAllocs)
	}
	// Isolate prepared-plan ownership from the already borrowed raw bytes. GC
	// here measures allocations; it is unrelated to process recovery acceptance.
	goruntime.GC()
	var before, after goruntime.MemStats
	goruntime.ReadMemStats(&before)
	var plans [3]*execution.Plan
	for i := range plans {
		work, failure := adapter.prepareNative(large)
		if failure != nil {
			t.Fatal(failure)
		}
		plans[i] = work
	}
	goruntime.GC()
	goruntime.ReadMemStats(&after)
	retained := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	if retained > 256<<10 {
		t.Fatal("prepared plans retained parsed header maps before admission", retained)
	}
	native := plans[0].Backend.(*nativePlan)
	if native.working <= len(large.Request.Data)*8 || plans[0].WorkingBytes != nativeBudget+native.working {
		t.Fatal("future parser and request-clone working peak was not reserved", native.working, plans[0].WorkingBytes)
	}
	t.Logf("raw=%d unique_headers=100000 allocations_small=%.0f allocations_large=%.0f prepared3_retained=%d initial_working=%d request_working=%d", len(large.Request.Data), smallAllocs, largeAllocs, retained, plans[0].WorkingBytes, native.working)
	goruntime.KeepAlive(plans)
	goruntime.KeepAlive(large)
}

func TestNativeRequestCapacityFailurePrecedesParserAndHTTP(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, "body")
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, nativeClient: server.Client()}
	request := nativeManyHeaderRequest(100000, nil)
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	probe := &nativeMemoryProbe{limit: work.WorkingBytes - 1, active: true}
	work.Command, work.NativeIO = testutil.NativeCommand(request), probe
	var before, after goruntime.MemStats
	goruntime.ReadMemStats(&before)
	capture := &nativeCapture{}
	end := adapter.executeNative(t.Context(), work, capture.Emit)
	goruntime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || calls.Load() != 0 || capture.head != nil || allocated > 256<<10 {
		t.Fatal("request parsed or sent before owning its working reservation", end, calls.Load(), allocated)
	}
	t.Logf("rejected_before_parse allocated=%d attempted_http=%d", allocated, calls.Load())
}

type nativeRequestLifetimeTransport struct {
	delegate http.RoundTripper
	request  chan weak.Pointer[http.Request]
	closed   atomic.Bool
}

func (t *nativeRequestLifetimeTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	t.request <- weak.Make(request)
	response, err := t.delegate.RoundTrip(request)
	if response != nil {
		body := &nativeRequestLifetimeBody{ReadCloser: response.Body, closed: &t.closed}
		response.Body = body
	}
	return response, err
}

type nativeRequestLifetimeBody struct {
	io.ReadCloser
	closed *atomic.Bool
}

func (b *nativeRequestLifetimeBody) Close() error {
	err := b.ReadCloser.Close()
	b.closed.Store(true)
	return err
}

type nativeRequestReachability struct {
	alive, canceled bool
}

// Keep the temporary strong pointer returned by Value in a completed frame,
// rather than making the diagnostic itself retain a Request across the next GC.
//
//go:noinline
func nativeRequestState(pointer weak.Pointer[http.Request]) nativeRequestReachability {
	state := nativeRequestReachability{}
	if request := pointer.Value(); request != nil {
		state.alive = true
		state.canceled = request.Context().Err() != nil
	}
	return state
}

func TestNativeParsedRequestWorkingLivesThroughSlowHeadAndEndsBeforeTerminal(t *testing.T) {
	const headers = 100000
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if len(request.Header) < headers {
			t.Error("valid Native request headers changed", len(request.Header))
		}
		_, _ = io.WriteString(w, "body")
	})
	server := httptest.NewUnstartedServer(handler)
	server.Config.MaxHeaderBytes = 4 << 20
	server.Start()
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	transport := newTransport(time.Second, time.Second)
	transport.DisableKeepAlives = true
	lifetime := &nativeRequestLifetimeTransport{delegate: transport, request: make(chan weak.Pointer[http.Request], 1)}
	client := &http.Client{Transport: lifetime}
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, client: client, nativeClient: client, transport: transport, nativeTransport: transport, ctx: ctx, cancel: cancel}
	memory := store.NewNativeMemory()
	sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: 768 << 20}
	memory.ObserveMemory(sample)
	limits := store.DefaultLimits()
	limits.MaxInFlight = 1
	runtime, err := store.New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(context.Background())
	session := runtime.NewSession()
	defer session.Close()
	request := nativeManyHeaderRequest(headers, nil)
	command := testutil.NativeCommand(request)
	work, failure := adapter.PrepareCommand(ctx, 1, command)
	if failure != nil {
		t.Fatal(failure)
	}
	native := work.Backend.(*nativePlan)
	goruntime.GC()
	var baseline goruntime.MemStats
	goruntime.ReadMemStats(&baseline)
	deadline, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	ticket, failure, _ := runtime.Submit(deadline, work, session)
	if failure != nil {
		t.Fatal(failure)
	}
	var head *store.Emission
	select {
	case head = <-session.Events:
		if head.Event.GetHead() == nil {
			t.Fatal("valid large-header request was not sent", head.Event)
		}
	case <-deadline.Done():
		t.Fatal("large-header request did not produce Head")
	}
	held := memory.Snapshot()
	weakRequest := <-lifetime.request
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Paused != 1 || snapshot.WorkingBytes < native.working || held.UsedBytes < uint64(work.Bytes+native.working) {
		t.Fatal("slow Head discarded parsed or cloned request working memory", snapshot, held, native.working)
	}
	goruntime.GC()
	var slow, terminal goruntime.MemStats
	goruntime.ReadMemStats(&slow)
	stateAtHead := nativeRequestState(weakRequest)
	head.Release()
	for {
		select {
		case emission := <-session.Events:
			if emission.Event.GetNativeEnd() != nil {
				if emission.Event.GetNativeEnd().Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
					t.Fatal("large request did not complete", emission.Event)
				}
				defer emission.Release()
				goto ended
			}
			emission.Release()
		case <-deadline.Done():
			t.Fatal("large request did not reach terminal publication")
		}
	}
ended:
	ended := memory.Snapshot()
	if snapshot := runtime.Snapshot(); snapshot.Active != 0 || snapshot.Paused != 0 || snapshot.WorkingBytes != 0 || ended.Terminal != 1 || ended.UsedBytes >= held.UsedBytes-uint64(native.working)/2 {
		t.Fatal("cleaned request objects remained in terminal working credit", snapshot, held, ended)
	}
	goruntime.GC()
	goruntime.ReadMemStats(&terminal)
	released := int64(slow.HeapAlloc) - int64(terminal.HeapAlloc)
	stateAtTerminal := nativeRequestState(weakRequest)
	t.Logf("heap pre=%d head=%d terminal=%d; objects pre=%d head=%d terminal=%d; allocated pre=%d head=%d terminal=%d; request_alive head=%t terminal_first_gc=%t", baseline.HeapAlloc, slow.HeapAlloc, terminal.HeapAlloc, baseline.HeapObjects, slow.HeapObjects, terminal.HeapObjects, baseline.TotalAlloc, slow.TotalAlloc, terminal.TotalAlloc, stateAtHead.alive, stateAtTerminal.alive)
	// The RoundTripper contract permits request reuse after Body.Close. It does
	// not promise a goroutine's stack or Go's headerSorterPool is collected in
	// the very first overlapping GC. Observe reachability separately, without
	// Ack or Session.Close, so a ticket-owned request would remain detectable.
	if !lifetime.closed.Load() || stateAtTerminal.alive && !stateAtTerminal.canceled {
		t.Fatal("terminal was published before request ownership cleanup", lifetime.closed.Load(), stateAtTerminal)
	}
	quiescence := time.Now().Add(2 * time.Second)
	observations := 0
	for stateAtTerminal.alive && time.Now().Before(quiescence) {
		time.Sleep(10 * time.Millisecond)
		goruntime.GC()
		stateAtTerminal = nativeRequestState(weakRequest)
		observations++
	}
	if stateAtTerminal.alive {
		t.Fatal("unacknowledged terminal retained its outbound request after transport quiescence")
	}
	t.Logf("request_working=%d slow_used=%d terminal_used=%d immediate_heap_drop=%d later_weak_gc_observations=%d body_closed=%t", native.working, held.UsedBytes, ended.UsedBytes, released, observations, lifetime.closed.Load())
	session.Close()
	ticket.Ack()
	for memory.Snapshot().Count != 0 {
		select {
		case <-deadline.Done():
			t.Fatal("terminal cancellation retained its input or working reservation", memory.Snapshot())
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestNativeRequestWorkingExcludesBorrowedBodyAndChecksOverflow(t *testing.T) {
	adapter := &Adapter{}
	small := nativeManyHeaderRequest(1, nil)
	large := nativeManyHeaderRequest(1, bytes.Repeat([]byte("x"), 4<<20))
	first, failure := adapter.prepareNative(small)
	if failure != nil {
		t.Fatal(failure)
	}
	second, failure := adapter.prepareNative(large)
	if failure != nil {
		t.Fatal(failure)
	}
	if second.WorkingBytes-first.WorkingBytes > 128 || second.Bytes-first.Bytes < 4<<20 {
		t.Fatal("borrowed input body was duplicated in parser working credit", first.Bytes, first.WorkingBytes, second.Bytes, second.WorkingBytes)
	}
	for _, frame := range []nativeRequestFrame{{headerBytes: math.MaxInt, lines: 1}, {headerBytes: 1, lines: math.MaxInt}, {headerBytes: math.MaxInt - 8, lines: 1}} {
		if _, valid := adapter.nativeRequestWorking(frame, "resource"); valid {
			t.Fatal("request working arithmetic overflowed", frame)
		}
	}
	probe := &nativeMemoryProbe{}
	memory := &nativeHeaderMemory{io: probe, base: math.MaxInt - 8192}
	if memory.read([]byte("header\r\n")) != execution.ErrNativeMemory {
		t.Fatal("response parser growth overflowed its still-held request base")
	}
}
