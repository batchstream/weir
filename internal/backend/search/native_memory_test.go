package search

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
)

type nativeMemoryProbe struct {
	mu      sync.Mutex
	limit   int
	peak    int
	working int
	reads   int
	active  bool
	readErr error
}

func (p *nativeMemoryProbe) GrowIO(working int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limit != 0 && working > p.limit {
		return execution.ErrExecutionMemory
	}
	p.peak = max(p.peak, working)
	return nil
}

func (p *nativeMemoryProbe) BeginIO(ctx context.Context, extra int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if extra < protocol.NativeChunk || p.active {
		return errors.New("invalid body IO boundary")
	}
	p.reads++
	if p.readErr != nil {
		return p.readErr
	}
	p.active = true
	return ctx.Err()
}

func (p *nativeMemoryProbe) EndIO(working, _ int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.working = working
	p.active = false
	return nil
}

func TestNativeHeaderMemoryReservesBeforeHTTPParser(t *testing.T) {
	for _, mode := range []string{"large", "many_lines", "budget_full"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if mode == "many_lines" {
					for range 12000 {
						w.Header().Add("X-Small", "x")
					}
				} else {
					w.Header().Set("X-Large", strings.Repeat("x", 2<<20))
				}
				_, _ = io.WriteString(w, "body")
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			dialer := &connectionDialer{ctx: t.Context(), conns: make(map[*searchConn]struct{})}
			defer dialer.close()
			transport := newTransport(time.Second, time.Second)
			transport.DialContext = dialer.dial
			transport.DisableKeepAlives = true
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			cfg := Config{URL: server.URL}
			adapter := &Adapter{config: cfg, nativeClient: client}
			request := nativeRequest(t, "records", "GET", "/_search")
			work, failure := adapter.prepareNative(request)
			if failure != nil {
				t.Fatal(failure)
			}
			probe := &nativeMemoryProbe{active: true}
			if mode == "budget_full" {
				probe.limit = work.WorkingBytes
			}
			work.Command = testutil.NativeCommand(request)
			work.NativeIO = probe
			capture := &nativeCapture{}
			emit := func(plan *execution.Plan, event *pb.Event) error {
				probe.mu.Lock()
				active := probe.active
				probe.mu.Unlock()
				if active {
					t.Fatal("output still owned a backend execution slot")
				}
				return capture.Emit(plan, event)
			}
			end := adapter.executeNative(t.Context(), work, emit)
			if calls.Load() != 1 {
				t.Fatal("header capacity caused a backend replay", calls.Load())
			}
			if mode == "budget_full" {
				if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || capture.head != nil {
					t.Fatal("parser growth did not terminate an already attempted exchange honestly", end, capture.head)
				}
				return
			}
			if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.body.String() != "body" || probe.reads == 0 || probe.peak <= nativeBudget {
				t.Fatal("large headers were silently capped or escaped memory accounting", end, probe.peak, probe.reads)
			}
			if probe.working >= probe.peak {
				t.Fatal("settled response retained the parser allocation peak", probe.working, probe.peak)
			}
		})
	}
}

func TestNativeHeaderMemoryDetachesFinishedExchange(t *testing.T) {
	probe := &nativeMemoryProbe{limit: 8192}
	memory := &nativeHeaderMemory{io: probe}
	if !errors.Is(memory.read([]byte("header")), execution.ErrExecutionMemory) {
		t.Fatal("header bytes reached a parser without allocation credit")
	}
	memory.finish()
	if memory.io != nil || memory.read([]byte("later body bytes")) != nil {
		t.Fatal("finished exchange retained its Runtime through the transport")
	}
}

func TestNativeBodyWorkspaceExhaustionPreservesAttemptedExchange(t *testing.T) {
	var calls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = io.WriteString(w, "body")
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, nativeClient: server.Client()}
	request := nativeRequest(t, "records", "GET", "/_search")
	work, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	probe := &nativeMemoryProbe{active: true, readErr: execution.ErrExecutionMemory}
	work.Command, work.NativeIO = testutil.NativeCommand(request), probe
	capture := &nativeCapture{}
	end := adapter.executeNative(t.Context(), work, capture.Emit)
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || end.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || capture.head == nil || capture.body.Len() != 0 || calls.Load() != 1 || probe.reads != 1 || probe.active {
		t.Fatal("body allocation pressure was replayed, reported not-started, or held IO", end, calls.Load(), probe, capture)
	}
}
