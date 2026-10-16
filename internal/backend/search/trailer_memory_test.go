package search

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type trailerMemoryStage struct {
	working, modeled, declared, populated int
}

type trailerMemoryTransport struct {
	transport http.RoundTripper
	owner     *recordMemoryProbe
	before    chan trailerMemoryStage
	eof       chan trailerMemoryStage
	guard     *nativeHeaderMemory
}

func (t *trailerMemoryTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	key := nativeHeaderKey{}
	t.guard, _ = request.Context().Value(key).(*nativeHeaderMemory)
	response, err := t.transport.RoundTrip(request)
	if err != nil {
		return response, err
	}
	body := &trailerMemoryBody{ReadCloser: response.Body, response: response, observer: t}
	response.Body = body
	return response, nil
}

type trailerMemoryBody struct {
	io.ReadCloser
	response *http.Response
	observer *trailerMemoryTransport
	first    sync.Once
}

func (b *trailerMemoryBody) stage() trailerMemoryStage {
	owner := b.observer.owner
	owner.mu.Lock()
	stage := trailerMemoryStage{working: owner.working, modeled: 8192 + 4096, declared: len(b.response.Trailer)}
	owner.mu.Unlock()
	for _, fields := range []http.Header{b.response.Header, b.response.Trailer} {
		for name, values := range fields {
			stage.modeled += len(name) + 256 + cap(values)*16
			for _, value := range values {
				stage.modeled += len(value)
			}
		}
	}
	for _, values := range b.response.Trailer {
		stage.populated += len(values)
	}
	return stage
}

func (b *trailerMemoryBody) Read(raw []byte) (int, error) {
	b.first.Do(func() { b.observer.before <- b.stage() })
	n, err := b.ReadCloser.Read(raw)
	if err == io.EOF {
		b.observer.eof <- b.stage()
	}
	return n, err
}

func TestTypedTrailersStayBudgetedThroughEOFAndConnectionReuse(t *testing.T) {
	for _, test := range []struct {
		name                string
		declared, populated int
		cancel              bool
	}{
		{name: "single line declaration", declared: 4096, populated: 300},
		{name: "undeclared EOF", populated: 300},
		{name: "merge new EOF keys", declared: 32, populated: 300},
		{name: "cancel before EOF", declared: 4096, cancel: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			names := make([]string, max(test.declared, test.populated))
			for i := range names {
				names[i] = fmt.Sprintf("X-%04x", i)
			}
			release := make(chan struct{})
			remotes := make(chan string, 2)
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
				w.Header()["Date"] = nil
				w.Header().Set("Content-Type", "application/json")
				if request.URL.Path == "/" {
					_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
					return
				}
				remotes <- request.RemoteAddr
				if request.URL.Path == "/next" {
					_, _ = io.WriteString(w, `{}`)
					return
				}
				calls.Add(1)
				if test.declared != 0 {
					w.Header().Set("Trailer", strings.Join(names[:test.declared], ", "))
				}
				w.(http.Flusher).Flush()
				select {
				case <-release:
				case <-ctx.Done():
					return
				}
				if ctx.Err() != nil {
					return
				}
				_, _ = io.WriteString(w, `{}`)
				for i, name := range names[:test.populated] {
					if i < test.declared {
						w.Header().Set(name, "v")
					} else {
						w.Header().Set(http.TrailerPrefix+name, "v")
					}
				}
			})
			server := httptest.NewServer(handler)
			defer server.Close()
			defer cancel()
			config := Config{Store: "search", URL: server.URL}
			adapter, err := Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()
			transport := adapter.client.Transport
			owner := &recordMemoryProbe{}
			observer := &trailerMemoryTransport{transport: transport, owner: owner, before: make(chan trailerMemoryStage, 1), eof: make(chan trailerMemoryStage, 1)}
			adapter.client.Transport = observer
			memory := &recordMemory{owner: owner, reply: 64 << 10}
			requestContext := context.WithValue(ctx, recordMemoryContextKey, memory)
			done := make(chan error, 1)
			go func() {
				call := exchange{path: "/probe"}
				status, raw, err := adapter.request(requestContext, call)
				if !test.cancel && (status != 200 || string(raw) != `{}` || err != nil) {
					t.Error("complete trailer response was shortened or lost", status, string(raw), err)
				}
				done <- err
			}()
			var before trailerMemoryStage
			select {
			case before = <-observer.before:
			case <-ctx.Done():
				t.Fatal("real response did not reach its body", ctx.Err())
			}
			if before.declared != test.declared || before.populated != 0 || before.working < before.modeled {
				t.Error("declared map or body capacity lost allocation credit", before)
			}
			if test.cancel {
				cancel()
			}
			close(release)
			select {
			case err := <-done:
				if test.cancel && err == nil {
					t.Error("cancelled body completed")
				}
			case <-time.After(time.Second):
				t.Fatal("trailer body did not leave IO after cancellation or EOF")
			}
			observer.guard.mu.Lock()
			detached := observer.guard.completed && observer.guard.records == nil && observer.guard.memory == nil && observer.guard.io == nil
			observer.guard.mu.Unlock()
			if calls.Load() != 1 || !detached {
				t.Fatal("exchange replayed or retained owner/context after release", calls.Load(), detached)
			}
			if test.cancel {
				return
			}
			end := <-observer.eof
			if end.declared != max(test.declared, test.populated) || end.populated != test.populated || end.working < end.modeled {
				t.Fatal("EOF parser/merged map escaped its shared reservation", end)
			}
			oldPeak := owner.peak
			adapter.client.Transport = transport
			next := &recordMemoryProbe{limit: 64 << 10}
			nextMemory := &recordMemory{owner: next, reply: 64 << 10}
			nextContext := context.WithValue(ctx, recordMemoryContextKey, nextMemory)
			call := exchange{path: "/next"}
			status, raw, err := adapter.request(nextContext, call)
			if status != 200 || string(raw) != `{}` || err != nil || owner.peak != oldPeak || <-remotes != <-remotes {
				t.Fatal("reused connection retained trailer credits or the previous owner", status, string(raw), err)
			}
			t.Logf("declaration=%d populated=%d before=%+v EOF=%+v peak=%d nextPeak=%d", test.declared, test.populated, before, end, oldPeak, next.peak)
		})
	}
}

func TestTypedPrefetchedUndeclaredTrailersAreApprovedBeforeGoParser(t *testing.T) {
	var wire strings.Builder
	wire.WriteString("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\n0\r\n")
	for i := range 300 {
		fmt.Fprintf(&wire, "X-%04x: v\r\n", i)
	}
	wire.WriteString("\r\n")
	if wire.Len() >= 4096 {
		t.Fatal("prefetch fixture exceeds the actual bufio read")
	}
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	owner := &recordMemoryProbe{}
	memory := &recordMemory{owner: owner, reply: 64 << 10}
	guard := &nativeHeaderMemory{memory: owner, records: memory}
	key := nativeHeaderKey{}
	ctx := context.WithValue(t.Context(), key, guard)
	connection := &searchConn{Conn: client, header: guard}
	written := make(chan error, 1)
	go func() {
		_, err := io.WriteString(server, wire.String())
		written <- err
	}()
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if reader.Buffered() != wire.Len()-len("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n") {
		t.Fatal("EOF trailer was not already buffered before body setup", reader.Buffered())
	}
	raw, err := readResponseBody(ctx, response.Body, -1, 8192)
	if err != nil || string(raw) != `{}` || len(response.Trailer) != 300 || owner.working < 8192+300*279 {
		t.Fatal("prefetched undeclared EOF nodes bypassed reservation", len(raw), err, len(response.Trailer), owner.working)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	guard.finish()
}

func TestTrailerWorkspaceBoundsDirectBodyReadsAndRejectsOverflow(t *testing.T) {
	owner := &recordMemoryProbe{}
	memory := &recordMemory{owner: owner, reply: 64 << 10}
	guard := &nativeHeaderMemory{memory: owner, records: memory}
	key := nativeHeaderKey{}
	ctx := context.WithValue(t.Context(), key, guard)
	if err := guard.read([]byte("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n")); err != nil {
		t.Fatal(err)
	}
	if err := reserveSearchScratch(ctx, 8192+4096); err != nil {
		t.Fatal(err)
	}
	declarations := guard.elements
	body := bytes.Repeat([]byte{','}, 12<<20)
	allocations := testing.AllocsPerRun(1, func() {
		if err := guard.read(body); err != nil {
			t.Fatal(err)
		}
	})
	if allocations != 0 || guard.elements != declarations || guard.window > 8192+8*8192+512*8192 || guard.currentBytes >= 4096 || guard.previousBytes > 4096 {
		t.Fatal("direct body was copied, accumulated, or treated as trailer declarations", allocations, guard)
	}
	guard.trailer = math.MaxInt
	if err := reserveSearchScratch(ctx, 4096); !errors.Is(err, execution.ErrExecutionMemory) {
		t.Fatal("trailer growth arithmetic overflow was not rejected", err)
	}
	guard.finish()
}

func TestTrailerWindowCoversCrossBlockFragmentedEOF(t *testing.T) {
	var trailer strings.Builder
	for i := range 300 {
		fmt.Fprintf(&trailer, "X-%04x: v\r\n", i)
	}
	trailer.WriteString("\r\n")
	raw := []byte(trailer.String())
	for _, prefix := range []int{0, 4000, 4095, 4096, 7000} {
		owner := &recordMemoryProbe{}
		memory := &recordMemory{owner: owner, reply: 64 << 10}
		guard := &nativeHeaderMemory{memory: owner, records: memory}
		key := nativeHeaderKey{}
		ctx := context.WithValue(t.Context(), key, guard)
		if err := reserveSearchScratch(ctx, 8192+4096); err != nil {
			t.Fatal(err)
		}
		if err := guard.read(bytes.Repeat([]byte{'x'}, prefix)); err != nil {
			t.Fatal(err)
		}
		for offset := 0; offset < len(raw); offset += 37 {
			if err := guard.read(raw[offset:min(len(raw), offset+37)]); err != nil {
				t.Fatal(err)
			}
		}
		want := 8192 + 8*len(raw) + 512*bytes.Count(raw, []byte{'\n'})
		if guard.window < want || owner.working < 8192+4096+want {
			t.Fatal("two-interval window lost prefetched or fragmented EOF bytes", prefix, guard.window, want, owner.working)
		}
		guard.finish()
	}
}

func TestFinishedOrUnownedBodyGuardPreservesContextMemoryOwner(t *testing.T) {
	for _, finished := range []bool{false, true} {
		owner := &recordMemoryProbe{limit: 128 << 10}
		memory := &recordMemory{owner: owner, reply: 64 << 10}
		guard := &nativeHeaderMemory{}
		if finished {
			guard.finish()
		}
		ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
		key := nativeHeaderKey{}
		ctx = context.WithValue(ctx, key, guard)
		if err := reserveSearchScratch(ctx, 128<<10); err != nil || owner.working != 128<<10 {
			t.Fatal("unowned body guard hid the context's actual owner", finished, err, owner.working)
		}
		if err := reserveSearchScratch(ctx, 256<<10); !errors.Is(err, execution.ErrExecutionMemory) {
			t.Fatal("unowned body guard bypassed the context's capacity failure", finished, err)
		}
	}
}

func TestTrailerCapacityAfterMutationSendRemainsUnknownWithoutReplay(t *testing.T) {
	var applied atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/" {
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
			return
		}
		applied.Add(1)
		names := make([]string, 4096)
		for i := range names {
			names[i] = fmt.Sprintf("X-%04x", i)
		}
		w.Header().Set("Trailer", strings.Join(names, ", "))
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, bulkAckMemoryReply([]string{"item"}))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	config := Config{Store: "search", URL: server.URL}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	work := batchTestPlan(t, adapter, "put", "records/s:item")
	reply := work.IOReserve + bulkAckWorking(work.Backend.(*plan).id)
	base := 2*work.Bytes + execution.EntryOverheadBytes
	owner := &recordMemoryProbe{limit: base + reply}
	memory := &recordMemory{owner: owner, base: base, reply: reply}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	call := exchange{path: "/_bulk", body: []byte("{}\n"), mutation: true}
	status, raw, err := adapter.request(ctx, call)
	response := bulkResponse{works: []*execution.Plan{work}, status: status, raw: raw, err: err}
	result := adapter.bulkResults(ctx, response)[0].GetMutationResult()
	if !errors.Is(err, execution.ErrExecutionMemory) || raw != nil || applied.Load() != 1 || result.Outcome != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("post-send trailer capacity changed mutation evidence or replayed", err, applied.Load(), result)
	}
}
