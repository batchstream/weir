package search

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

type recordMemoryProbe struct {
	mu                           sync.Mutex
	limit, resultLimit           int
	working, peak, result, calls int
}

func (p *recordMemoryProbe) SetWorking(bytes int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.limit != 0 && bytes > p.limit {
		return execution.ErrExecutionMemory
	}
	p.working = bytes
	p.peak = max(p.peak, bytes)
	return nil
}

func (p *recordMemoryProbe) ReserveResult(bytes int) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if p.resultLimit != 0 && bytes > p.resultLimit {
		return execution.ErrExecutionMemory
	}
	p.result = bytes
	return nil
}

func TestRecordResponseBodyReservesGrowthBeforeAllocation(t *testing.T) {
	for _, known := range []bool{false, true} {
		t.Run(fmt.Sprint(known), func(t *testing.T) {
			payload := strings.Repeat("x", 2<<20)
			probe := &recordMemoryProbe{limit: 4 << 20}
			memory := &recordMemory{owner: probe}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			length := int64(-1)
			if known {
				length = int64(len(payload))
			}
			data, err := readResponseBody(ctx, strings.NewReader(payload), length, 0)
			if err != nil || string(data) != payload || probe.peak < len(payload) {
				t.Fatal("body escaped allocation credit or was shortened", len(data), err, probe.peak)
			}
			probe.limit = 8192
			reader := &countingRecordReader{Reader: strings.NewReader(payload)}
			data, err = readResponseBody(ctx, reader, length, 0)
			if !errors.Is(err, execution.ErrExecutionMemory) || data != nil {
				t.Fatal("memory denial returned a partial document", len(data), err)
			}
			if known && reader.bytes != 0 || !known && reader.bytes > 4097 {
				t.Fatal("body advanced before growth was approved", reader.bytes)
			}
		})
	}
}

type countingRecordReader struct {
	io.Reader
	bytes int
}

func (r *countingRecordReader) Read(data []byte) (int, error) {
	n, err := r.Reader.Read(data)
	r.bytes += n
	return n, err
}

type recordEOFReader struct {
	raw   []byte
	chunk int
	zero  bool
	err   error
}

func (r *recordEOFReader) Read(data []byte) (int, error) {
	if r.zero {
		r.zero = false
		return 0, nil
	}
	if r.chunk > 0 && len(data) > r.chunk {
		data = data[:r.chunk]
	}
	n := copy(data, r.raw)
	r.raw = r.raw[n:]
	if len(r.raw) == 0 {
		if r.err != nil {
			return n, r.err
		}
		return n, io.EOF
	}
	return n, nil
}

func TestRecordResponseBodyPreservesFinalByteReturnedWithEOF(t *testing.T) {
	payload := []byte(strings.Repeat("x", 4096) + "z")
	for _, length := range []int64{-1, 4096, 4097} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			probe := &recordMemoryProbe{limit: 16 << 10}
			memory := &recordMemory{owner: probe}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			reader := &recordEOFReader{raw: payload}
			data, err := readResponseBody(ctx, reader, length, 0)
			if err != nil || !bytes.Equal(data, payload) || probe.working != cap(data) {
				t.Fatal("final byte was lost or its capacity was uncharged", len(data), err, probe.working)
			}
		})
	}
}

func TestRecordResponseBodyHandlesPartialReadsZeroProgressAndErrors(t *testing.T) {
	payload := []byte(strings.Repeat("x", 4096) + "z")
	cases := []struct {
		name  string
		chunk int
		zero  bool
		err   error
	}{
		{name: "partial", chunk: 3},
		{name: "zero then partial", chunk: 3, zero: true},
		{name: "last byte and read error", err: io.ErrUnexpectedEOF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := &recordMemoryProbe{limit: 16 << 10}
			memory := &recordMemory{owner: probe}
			ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
			reader := &recordEOFReader{raw: payload, chunk: tc.chunk, zero: tc.zero, err: tc.err}
			data, err := readResponseBody(ctx, reader, -1, 0)
			if tc.err != nil {
				if !errors.Is(err, tc.err) || data != nil {
					t.Fatal("body error returned partial success", len(data), err)
				}
			} else if err != nil || !bytes.Equal(data, payload) || probe.working != cap(data) {
				t.Fatal("partial reads changed body or lost accounting", len(data), err, probe.working)
			}
		})
	}
}

func TestRecordUnsupportedResourceArityDoesNotContactBackend(t *testing.T) {
	var calls atomic.Int32
	adapter := testHTTPAdapter(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	read := &pb.ReadRequest{Resource: strings.Repeat("part/", 1_000_000) + "last"}
	operation := &pb.Command_Read{Read: read}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(t.Context(), record)
	if work != nil || failure.GetCode() != pb.FailureCode_INVALID_ARGUMENT || len(record.Segments()) != 4 || calls.Load() != 0 {
		t.Fatal("unsupported arity escaped early target validation", failure, len(record.Segments()), calls.Load())
	}
}

func TestRecordPreparationDefersJSONTreesAndRejectsBeforeBackendIO(t *testing.T) {
	var calls atomic.Int32
	adapter := testHTTPAdapter(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) })
	operation := expressionOperation("records/s:invalid", `{"doc":{"duplicate":1,"duplicate":2}}`)
	record, err := execution.NewRecord("search", 1, operation.Command)
	if err != nil {
		t.Fatal(err)
	}
	work, failure := adapter.PrepareRecord(t.Context(), record)
	if failure != nil {
		t.Fatal("raw-only preparation allocated the expression tree", failure)
	}
	probe := &recordMemoryProbe{limit: 8 << 20}
	work.Memory = probe
	results := adapter.executeRecords(t.Context(), []*execution.Plan{work})
	result := results[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT || calls.Load() != 0 || probe.peak == 0 {
		t.Fatal("invalid admitted record contacted backend or changed evidence", result, calls.Load(), probe.peak)
	}
}

func TestRecordChunkedSourceAndResultCapacity(t *testing.T) {
	for _, deny := range []bool{false, true} {
		t.Run(fmt.Sprint(deny), func(t *testing.T) {
			source := `{"pad":"` + strings.Repeat("x", 2<<20) + `"}`
			adapter := testHTTPAdapter(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/records" {
					_, _ = io.WriteString(w, testIndexReply)
					return
				}
				w.(http.Flusher).Flush()
				_, _ = io.WriteString(w, coalescingReadResponse([]string{"large"}, source))
			})
			work := batchTestPlan(t, adapter, "read", "records/s:large")
			probe := &recordMemoryProbe{limit: 32 << 20}
			if deny {
				probe.resultLimit = 1024
			}
			work.Memory = probe
			results := adapter.executeRecords(t.Context(), []*execution.Plan{work})
			result := results[0].GetReadResult()
			if deny {
				if result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || result.GetDocument() != nil {
					t.Fatal(result)
				}
			} else if !bytes.Equal(result.GetDocument().GetData(), []byte(source)) || probe.result < len(source) || probe.calls == 0 {
				t.Fatal("chunked source escaped ownership or was truncated", len(result.GetDocument().GetData()), probe.result, probe.calls, result.GetFailure())
			}
		})
	}
}

func TestRecordMutationBodyMemoryFailurePreservesUnknownAndDoesNotReplay(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.(http.Flusher).Flush()
		_, _ = io.WriteString(w, `{"pad":"`+strings.Repeat("x", 1<<20)+`"}`)
	}))
	defer server.Close()
	adapter := testHTTPAdapter(t, func(http.ResponseWriter, *http.Request) {})
	adapter.config.URL = server.URL
	probe := &recordMemoryProbe{limit: 128 << 10}
	memory := &recordMemory{owner: probe}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
	call := exchange{path: "/_bulk", body: []byte("{}\n"), mutation: true}
	status, raw, err := adapter.request(ctx, call)
	if !errors.Is(err, execution.ErrExecutionMemory) || raw != nil || calls.Load() != 1 {
		t.Fatal(status, len(raw), err, calls.Load())
	}
	work := batchTestPlan(t, adapter, "put", "records/s:item")
	response := bulkResponse{works: []*execution.Plan{work}, status: status, raw: raw, err: err}
	result := adapter.bulkResults(ctx, response)[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_UNKNOWN || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal(result)
	}
}

func TestRecordResponseHeadersBindCurrentReusedExchange(t *testing.T) {
	var calls atomic.Int32
	var mu sync.Mutex
	var remotes []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		remotes = append(remotes, r.RemoteAddr)
		mu.Unlock()
		if calls.Add(1) == 2 {
			w.Header().Set("X-Large", strings.Repeat("x", 64<<10))
		}
		_, _ = io.WriteString(w, `{}`)
	}))
	defer server.Close()
	dialer := &connectionDialer{ctx: t.Context(), conns: make(map[*searchConn]struct{})}
	defer dialer.close()
	transport := newTransport(connectionTimeout, connectionTimeout)
	transport.DialContext = dialer.dial
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	config := Config{URL: server.URL}
	adapter := &Adapter{config: config, client: client, ctx: t.Context()}
	first := &recordMemoryProbe{limit: 128 << 10}
	firstMemory := &recordMemory{owner: first}
	ctx := context.WithValue(t.Context(), recordMemoryContextKey, firstMemory)
	call := exchange{path: "/first"}
	_, _, err := adapter.request(ctx, call)
	if err != nil {
		t.Fatal(err)
	}
	peak := first.peak
	second := &recordMemoryProbe{limit: 128 << 10}
	secondMemory := &recordMemory{owner: second}
	ctx = context.WithValue(t.Context(), recordMemoryContextKey, secondMemory)
	call.path = "/second"
	_, _, err = adapter.request(ctx, call)
	mu.Lock()
	defer mu.Unlock()
	if !errors.Is(err, execution.ErrExecutionMemory) || len(remotes) != 2 || remotes[0] != remotes[1] || first.peak != peak {
		t.Fatal("reused connection charged a stale exchange or replayed", err, remotes, first.peak, peak)
	}
}

func TestRecordManyFieldsReserveJSONNodesBeforeDecode(t *testing.T) {
	var source strings.Builder
	source.WriteByte('{')
	for i := range 5000 {
		if i != 0 {
			source.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&source, `"field%d":0`, i)
	}
	source.WriteByte('}')
	adapter := testHTTPAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			_, _ = io.WriteString(w, testIndexReply)
			return
		}
		_, _ = io.WriteString(w, coalescingReadResponse([]string{"many"}, source.String()))
	})
	work := batchTestPlan(t, adapter, "read", "records/s:many")
	probe := &recordMemoryProbe{limit: 1 << 20}
	work.Memory = probe
	result := adapter.executeRecords(t.Context(), []*execution.Plan{work})[0].GetReadResult()
	if result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || probe.calls != 0 {
		t.Fatal("JSON allocation nodes escaped parser budget", result, probe.calls)
	}
}

func TestRecordPreparationBorrowsLargeLuaInputWithoutDecoding(t *testing.T) {
	var source strings.Builder
	source.WriteByte('{')
	for i := range 10000 {
		if i != 0 {
			source.WriteByte(',')
		}
		_, _ = fmt.Fprintf(&source, `"field%d":0`, i)
	}
	source.WriteByte('}')
	config := Config{Store: "search"}
	adapter := &Adapter{config: config}
	operation := expressionOperation("records/s:item", `{}`)
	input := &pb.Document{ContentType: "application/json", Data: []byte(source.String())}
	program := &pb.LuaTransform{Source: []byte("return function(current, incoming) return incoming end"), Input: input}
	form := &pb.Transform_Lua{Lua: program}
	operation.Command.GetMutate().GetAtomicTransform().Form = form
	record, err := execution.NewRecord("search", 1, operation.Command)
	if err != nil {
		t.Fatal(err)
	}
	var work *execution.Plan
	allocations := testing.AllocsPerRun(20, func() {
		var failure *pb.Failure
		work, failure = adapter.PrepareRecord(t.Context(), record)
		if failure != nil {
			t.Fatal(failure)
		}
	})
	if allocations > 20 || work.Backend.(*plan).program.Source != "" || work.Backend.(*plan).program.Input.Kind != value.Missing {
		t.Fatal("unadmitted Lua input was decoded", allocations)
	}
	probe := &recordMemoryProbe{limit: 1 << 20}
	work.Memory = probe
	result := adapter.executeRecords(t.Context(), []*execution.Plan{work})[0].GetMutationResult()
	if result.GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED {
		t.Fatal("input decoded before its allocation was approved", result)
	}
}
