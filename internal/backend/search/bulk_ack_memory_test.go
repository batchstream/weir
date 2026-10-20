package search

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
	"github.com/batchstream/weir/internal/store"
)

// The normal reply includes the successful-write metadata already required by
// Elasticsearch and OpenSearch fixtures. It has many small parser nodes even
// though the entire body fits comfortably inside the old 64 KiB IO window.
func bulkAckMemoryReply(ids []string) string {
	var reply strings.Builder
	reply.WriteString(`{"errors":false,"took":1,"items":[`)
	for i, id := range ids {
		if i != 0 {
			reply.WriteByte(',')
		}
		fmt.Fprintf(&reply, `{"index":{"_index":"records","_id":%q,"_type":"_doc","forced_refresh":true,"status":201,"result":"created","_version":1,"_seq_no":%d,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, id, i+1)
	}
	reply.WriteString(`]}`)
	return reply.String()
}

func TestBulkNormalAcknowledgementReservedBeforeMutationIO(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		t.Run(fmt.Sprintf("chunked=%t", chunked), func(t *testing.T) {
			testBulkNormalAcknowledgementMemory(t, chunked, false)
		})
	}
}

func TestBulkNormalAcknowledgementQueuedCancellationAndDrain(t *testing.T) {
	testBulkNormalAcknowledgementMemory(t, false, true)
}

func testBulkNormalAcknowledgementMemory(t *testing.T, chunked, cancelQueued bool) {
	const records = 42
	const nativeRequests = 4
	const initialNativeWorking = 24 << 20
	const inputWindow = 32 << 20
	const headroom = 256 << 10
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	wait, stopWait := context.WithTimeout(ctx, 5*time.Second)
	defer stopWait()
	holdsStarted := make(chan struct{}, nativeRequests)
	releaseNative := make(chan struct{}, nativeRequests)
	nativeCompleted := make(chan error, nativeRequests)
	bulkApplied := make(chan store.ExecutionMemorySnapshot, 1)
	bodyBlocked := make(chan store.ExecutionMemorySnapshot, 1)
	releaseACK := make(chan struct{})
	var effectsMu sync.Mutex
	effects := make(map[string]int)
	bulkCalls := 0
	memory := store.NewExecutionMemory()
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
		case "/hold":
			holdsStarted <- struct{}{}
			select {
			case <-releaseNative:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(w, "released")
		case "/records":
			_, _ = io.WriteString(w, testIndexReply)
		case "/_bulk":
			scanner := bufio.NewScanner(request.Body)
			var ids []string
			for scanner.Scan() {
				var metadata map[string]struct {
					Index string `json:"_index"`
					ID    string `json:"_id"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &metadata); err != nil {
					t.Error("invalid bulk metadata", err)
					return
				}
				item, present := metadata["index"]
				if !present || item.Index != "records" || !scanner.Scan() || !json.Valid(scanner.Bytes()) {
					t.Error("invalid bulk source or action")
					return
				}
				ids = append(ids, item.ID)
			}
			if err := scanner.Err(); err != nil || len(ids) != records {
				t.Error("bulk did not contain all records", len(ids), err)
				return
			}
			effectsMu.Lock()
			bulkCalls++
			for _, id := range ids {
				effects[id]++
			}
			effectsMu.Unlock()
			reply := bulkAckMemoryReply(ids)
			w.Header().Set("Content-Type", "application/json")
			if !chunked {
				w.Header().Set("Content-Length", strconv.Itoa(len(reply)))
			}
			bulkApplied <- memory.Snapshot()
			_, _ = io.WriteString(w, reply[:len(reply)/2])
			w.(http.Flusher).Flush()
			bodyBlocked <- memory.Snapshot()
			select {
			case <-releaseACK:
			case <-ctx.Done():
				return
			}
			_, _ = io.WriteString(w, reply[len(reply)/2:])
		default:
			t.Error("unexpected HTTP request", request.URL.Path)
			http.NotFound(w, request)
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
	limits := store.DefaultLimits()
	limits.BatchOperations = records
	limits.BatchWait = time.Hour
	limits.MaxInFlight = nativeRequests + 1
	runtime, err := store.New(adapter, limits, memory)
	if err != nil {
		t.Fatal(err)
	}
	var sessions []*store.Session
	defer func() {
		cancel()
		for _, session := range sessions {
			session.Close()
		}
		closing, stop := context.WithTimeout(context.Background(), time.Second)
		defer stop()
		if err := runtime.Close(closing); err != nil {
			t.Error(err)
		}
	}()
	var plans []*execution.Plan
	var ids []string
	initialTypedWorking := 64 << 10
	source := []byte(`{"payload":"` + strings.Repeat("x", 1010) + `"}`)
	for i := range records {
		id := fmt.Sprintf("ack-%02d", i)
		ids = append(ids, id)
		document := &pb.Document{ContentType: "application/json", Data: source}
		action := &pb.MutateRequest_Put{Put: document}
		mutation := &pb.MutateRequest{Resource: "records/s:" + id, Action: action}
		operation := &pb.Command_Mutate{Mutate: mutation}
		command := &pb.Command{Operation: operation}
		record, err := execution.NewRecord("search", uint64(i+1), command)
		if err != nil {
			t.Fatal(err)
		}
		plan, failure := runtime.PrepareRecord(ctx, record)
		if failure != nil {
			t.Fatal(failure)
		}
		// Keep this budget fixed when admission learns to include the ACK peak.
		// Raising Plan.WorkingBytes must queue the same workload, not raise B.
		initialTypedWorking += 2*record.Bytes() + execution.EntryOverheadBytes
		plans = append(plans, plan)
	}
	publication := max(2*protocol.NativeChunk+execution.ResultOverheadBytes, execution.ResultOverheadBytes)
	queuedWindow := 2*execution.ResultOverheadBytes + execution.MinimumWireBytes
	nativePublicationGrowth := nativeRequests * (publication - queuedWindow)
	budget := uint64(inputWindow + nativeRequests*initialNativeWorking + nativePublicationGrowth + initialTypedWorking + headroom)
	sample := overload.Snapshot{RecoveryAllowanceKnown: true, RecoveryAllowance: (3*budget + 1) / 2}
	memory.ObserveMemory(sample)
	for i := range nativeRequests {
		request := nativeRequest(t, "", http.MethodGet, "/hold")
		operation := &pb.Command_Native{Native: request}
		command := &pb.Command{Operation: operation}
		plan, failure := runtime.PrepareCommand(ctx, uint64(records+i+1), command)
		if failure != nil || plan.WorkingBytes != initialNativeWorking {
			t.Fatal("unexpected initial Native reservation", plan, failure)
		}
		session := runtime.NewSession()
		sessions = append(sessions, session)
		ticket, failure, _ := runtime.Submit(ctx, plan, session)
		if failure != nil {
			t.Fatal(failure)
		}
		go func() {
			for {
				select {
				case emission := <-session.Events:
					event := emission.Event
					emission.Release()
					if end := event.GetNativeEnd(); end != nil && end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE {
						nativeCompleted <- fmt.Errorf("Native did not complete: %s", end)
						return
					}
					if event == nil {
						ticket.Ack()
						nativeCompleted <- nil
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	for range nativeRequests {
		select {
		case <-holdsStarted:
		case <-wait.Done():
			t.Fatal("Native hold did not reach actual backend IO", wait.Err())
		}
	}
	held := memory.Snapshot()
	if held.Starting != nativeRequests || held.NativeCount != nativeRequests || held.InputReservedBytes != inputWindow || held.ResidentBytes != inputWindow || held.WireBytes != 0 || held.BudgetRejections != 0 || runtime.Snapshot().WorkingBytes != nativeRequests*initialNativeWorking {
		t.Fatal("test did not isolate initial Native working from retained-response pressure", held, runtime.Snapshot())
	}
	ack := []byte(bulkAckMemoryReply(ids))
	decode := jsonDecodeWorking(ack)
	if len(ack) >= 64<<10 || decode <= headroom || uint64(initialTypedWorking+headroom) != held.BudgetBytes-held.UsedBytes {
		t.Fatal("test did not isolate the complete normal ACK parser peak", len(ack), decode, held, initialTypedWorking)
	}
	t.Logf("initial barrier B=%d Q=%d nativeWorking=%d nativePublicationGrowth=%d typedInitial=%d headroom=%d ACKbytes=%d ACKdecode=%d sourceDecode=%d snapshot=%+v", budget, inputWindow, nativeRequests*initialNativeWorking, nativePublicationGrowth, initialTypedWorking, headroom, len(ack), decode, jsonDecodeWorking(source), held)
	typedContext, cancelTyped := context.WithCancel(ctx)
	defer cancelTyped()
	var tickets []*store.Ticket
	plannedWorking, reserve := 0, 0
	for _, plan := range plans {
		plannedWorking += plan.WorkingBytes
		reserve = max(reserve, plan.IOReserve)
		ticket, failure, _ := runtime.Submit(typedContext, plan, nil)
		if failure != nil {
			t.Fatal("bounded typed input admission failed", failure)
		}
		tickets = append(tickets, ticket)
	}
	plannedWorking += reserve
	// A queued batch must not send until it owns the complete normal ACK peak.
	// The hold barrier fixes pressure; this bounded observation lets the actual
	// Runtime dispatch or mark the full batch memory-blocked before IO is freed.
	var atSend *store.ExecutionMemorySnapshot
	observe := time.NewTimer(250 * time.Millisecond)
	defer observe.Stop()
	select {
	case snapshot := <-bulkApplied:
		atSend = &snapshot
	case <-observe.C:
		queued := runtime.Snapshot()
		if queued.Pending != records || memory.Snapshot().BudgetRejections != 0 {
			t.Fatal("typed batch did not wait in its bounded scheduler queue", queued, memory.Snapshot())
		}
		t.Logf("normal ACK reservation kept mutation before IO: runtime=%+v memory=%+v", queued, memory.Snapshot())
		if cancelQueued {
			cancelTyped()
			for _, ticket := range tickets {
				event, err := ticket.Wait(wait)
				if err != nil || event.GetMutationResult().GetOutcome() != pb.MutationOutcome_NOT_STARTED || event.GetMutationResult().GetFailure().GetCode() != pb.FailureCode_CANCELLED {
					t.Fatal("queued cancellation changed pre-attempt evidence", event, err)
				}
				ticket.Ack()
			}
			effectsMu.Lock()
			calls, writes := bulkCalls, len(effects)
			effectsMu.Unlock()
			if calls != 0 || writes != 0 || memory.Snapshot().BudgetRejections != 0 || memory.Snapshot().Count != nativeRequests {
				t.Fatal("canceled queue sent a write or retained its input", calls, writes, memory.Snapshot())
			}
			runtime.BeginDrain()
			for range nativeRequests {
				releaseNative <- struct{}{}
			}
			for range nativeRequests {
				select {
				case err := <-nativeCompleted:
					if err != nil {
						t.Fatal(err)
					}
				case <-wait.Done():
					t.Fatal("draining Native exchanges failed", wait.Err())
				}
			}
			// Releasing the last publisher marker precedes the Execute tail by
			// a few instructions. Close joins that real ownership boundary.
			if err := runtime.Close(wait); err != nil {
				t.Fatal("drain did not join adapter cleanup", err)
			}
			if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.BudgetRejections != 0 {
				t.Fatal("drain retained canceled ACK/input promises", snapshot)
			}
			t.Log("queued cancellation sent zero writes and drain released all shared ownership")
			return
		}
		releaseNative <- struct{}{}
		select {
		case err := <-nativeCompleted:
			if err != nil {
				t.Fatal(err)
			}
		case <-wait.Done():
			t.Fatal("Native convergence did not complete", wait.Err())
		}
	case <-wait.Done():
		t.Fatal(wait.Err())
	}
	if atSend == nil {
		select {
		case snapshot := <-bulkApplied:
			atSend = &snapshot
		case <-wait.Done():
			t.Fatal("normal bulk did not resume after one Native converged", wait.Err())
		}
	}
	select {
	case snapshot := <-bodyBlocked:
		// Header and the first half of the body have arrived. The normal reply
		// scratch must remain owned while the complete JSON is unavailable.
		working := runtime.Snapshot().WorkingBytes
		if snapshot.NativeCount != nativeRequests-1 || working != (nativeRequests-1)*initialNativeWorking+plannedWorking || snapshot.BudgetRejections != 0 {
			t.Fatal("header/body stage shrank or double-charged the promised ACK", snapshot, working, plannedWorking)
		}
		t.Logf("header/body gate chunked=%t plannedTypedWorking=%d memory=%+v runtimeWorking=%d", chunked, plannedWorking, snapshot, working)
	case <-wait.Done():
		t.Fatal("ACK body did not reach controlled partial-read gate", wait.Err())
	}
	close(releaseACK)
	applied, unknown := 0, 0
	for _, ticket := range tickets {
		event, err := ticket.Wait(wait)
		if err != nil {
			t.Fatal("typed mutation did not complete", err, memory.Snapshot())
		}
		result := event.GetMutationResult()
		if result.GetOutcome() == pb.MutationOutcome_APPLIED && result.GetFailure() == nil {
			applied++
		} else if result.GetOutcome() == pb.MutationOutcome_UNKNOWN && result.GetFailure().GetCode() == pb.FailureCode_RESOURCE_EXHAUSTED && result.GetFailure().GetMessage() == execution.ErrExecutionMemory.Error() {
			unknown++
		} else {
			t.Error("unexpected mutation result", event)
		}
		ticket.Ack()
	}
	effectsMu.Lock()
	calls, appliedIDs := bulkCalls, len(effects)
	for _, id := range ids {
		if effects[id] != 1 {
			t.Error("mock backend effect was lost or replayed", id, effects[id])
		}
	}
	effectsMu.Unlock()
	after := memory.Snapshot()
	t.Logf("normal ACK result: bulkCalls=%d backendApplied=%d SDKapplied=%d SDKunknownCapacity=%d rejections=%d beforeReply=%+v after=%+v", calls, appliedIDs, applied, unknown, after.BudgetRejections, atSend, after)
	if calls != 1 || appliedIDs != records {
		t.Error("normal bulk changed exactly-once mock effects", calls, appliedIDs)
	}
	if after.NativeCount != nativeRequests-1 || after.Starting != nativeRequests-1 || after.Count != nativeRequests-1 {
		t.Error("test released more than the single required Native exchange", after)
	}
	if applied != records || unknown != 0 || after.BudgetRejections != 0 {
		t.Errorf("normal acknowledgement was not promised before mutation IO: want %d APPLIED and no hard capacity denial; got applied=%d UNKNOWN_RESOURCE_EXHAUSTED=%d ledgerRejections=%d", records, applied, unknown, after.BudgetRejections)
	}
}

func TestBulkNormalAcknowledgementShapeBoundsCompleteParserPeak(t *testing.T) {
	if got := jsonDecodeWorking([]byte(bulkAckItemShape)); got != 3*len(bulkAckItemShape)+8192+28*1024 {
		t.Fatal("normal item shape changed without updating its structural bound", got)
	}
	if got := jsonDecodeWorking([]byte(bulkAckEnvelopeShape)); got != 3*len(bulkAckEnvelopeShape)+8192+9*1024 {
		t.Fatal("normal envelope shape changed without updating its structural bound", got)
	}
	index := strings.Repeat("a", 255)
	id := strings.Repeat("<", 512)
	quotedID, err := json.Marshal(id)
	if err != nil || len(quotedID) != 6*len(id)+2 || !validIndex(index) {
		t.Fatal("maximum physical index or escaped ID fixture changed", len(quotedID), err)
	}
	item := strings.Replace(bulkAckItemShape, `"_index":""`, `"_index":"`+index+`"`, 1)
	item = strings.Replace(item, `"_id":""`, `"_id":`+string(quotedID), 1)
	item = strings.Replace(item, `"failed":9223372036854775807`, `"failed":0`, 1)
	for _, count := range []int{1, 42, 128} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("items=%d/chunked=%t", count, chunked), func(t *testing.T) {
				items := strings.TrimSuffix(strings.Repeat(item+",", count), ",")
				raw := strings.Replace(bulkAckEnvelopeShape, `"items":[]`, `"items":[`+items+`]`, 1)
				replyWorking := (64 << 10) + bulkAckEnvelopeWorking + count*bulkAckWorking(id)
				const held = 8192
				probe := &recordMemoryProbe{limit: held + replyWorking}
				memory := &recordMemory{owner: probe, base: held, reply: replyWorking}
				ctx := context.WithValue(t.Context(), recordMemoryContextKey, memory)
				length := int64(len(raw))
				if chunked {
					length = -1
				}
				data, err := readResponseBody(ctx, strings.NewReader(raw), length, 8192)
				if err != nil || string(data) != raw || probe.working != probe.limit {
					t.Fatal("normal body lost its pre-approved reply promise", len(data), err, probe.working, probe.limit)
				}
				peak := 8192 + cap(data) + jsonDecodeWorking(data)
				if err := reserveSearchScratch(ctx, peak); err != nil || probe.working != probe.limit {
					t.Fatal("complete normal parser needed more memory or double-charged its forecast", peak, err, probe.working, probe.limit)
				}
				if err := validateJSON(ctx, data); err != nil {
					t.Fatal("normal metadata fixture is invalid", err)
				}
				var works []*execution.Plan
				for i := range count {
					empty := &pb.Empty{}
					action := &pb.MutateRequest_Delete{Delete: empty}
					mutation := &pb.MutateRequest{Resource: index + "/s:" + id, Action: action}
					operation := &pb.Command_Mutate{Mutate: mutation}
					command := &pb.Command{Operation: operation}
					native := &plan{index: index, id: id, action: "delete"}
					work := &execution.Plan{ID: uint64(i + 1), Command: command, Backend: native}
					works = append(works, work)
				}
				adapter := &Adapter{}
				response := bulkResponse{works: works, status: 200, raw: data}
				for _, event := range adapter.bulkResults(ctx, response) {
					if event.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || event.GetMutationResult().GetFailure() != nil {
						t.Fatal("supported maximum normal ACK metadata lost APPLIED", event)
					}
				}
				t.Logf("items=%d chunked=%t indexBytes=%d IDbytes=%d wire=%d cap=%d actualParserPeak=%d normalReplyPromise=%d", count, chunked, len(index), len(id), len(data), cap(data), peak, replyWorking)
			})
		}
	}
}

func TestBulkReplyScratchFloorIncludesDynamicSourcesAndFramesOnce(t *testing.T) {
	const reply = 128 << 10
	probe := &recordMemoryProbe{}
	memory := &recordMemory{owner: probe, base: 4096, reply: reply}
	for _, state := range []struct {
		sources, frame, scratch int
	}{
		{sources: 8192, frame: 16 << 10},
		{sources: 8192, frame: 16 << 10, scratch: 32 << 10},
		{sources: 8192, frame: 16 << 10, scratch: 256 << 10},
		{},
	} {
		want := max(memory.base, state.frame) + state.sources + max(reply, state.scratch)
		probe.limit = want
		err := memory.set(t.Context(), state.sources, state.frame, state.scratch)
		if err != nil || probe.working != want {
			t.Fatal("ACK scratch was shrunk, consumed by held sources, or counted twice", state, err, probe.working, want)
		}
		headerMinimum := max(memory.base, state.frame) + state.sources + reply
		header := &nativeHeaderMemory{memory: probe, base: max(memory.base, state.frame) + state.sources, minimum: headerMinimum}
		if err := header.read([]byte("HTTP/1.1 200 OK\r\nContent-Length: 1\r\n\r\n")); err != nil || probe.working != headerMinimum {
			t.Fatal("typed header callback discarded the full current reply minimum", state, err, probe.working, headerMinimum)
		}
		header.finish()
	}
	for _, state := range []struct {
		base, reply, sources, frame, scratch int
	}{
		{base: math.MaxInt, reply: 1},
		{reply: 1, frame: math.MaxInt},
		{reply: 1, sources: math.MaxInt},
		{base: 1, reply: math.MaxInt},
		{base: -1},
		{reply: -1},
		{reply: reply, scratch: -1},
		{sources: -1},
		{frame: -1},
	} {
		probe := &recordMemoryProbe{}
		memory := &recordMemory{owner: probe, base: state.base, reply: state.reply}
		err := memory.set(t.Context(), state.sources, state.frame, state.scratch)
		if !errors.Is(err, execution.ErrExecutionMemory) || probe.working != 0 {
			t.Fatal("invalid total working reached the shared owner", state, err, probe.working)
		}
	}
}

func TestBulkOversizedUnknownErrorRetainsAttemptAndNeverReplays(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		for _, framingRoom := range []int{0, 64 << 10} {
			t.Run(fmt.Sprintf("chunked=%t/framingRoom=%d", chunked, framingRoom), func(t *testing.T) {
				var calls atomic.Int32
				handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
					if request.URL.Path == "/records" {
						_, _ = io.WriteString(w, testIndexReply)
						return
					}
					if request.URL.Path != "/_bulk" {
						t.Error("unexpected request", request.URL.Path)
						return
					}
					calls.Add(1)
					raw := `{"errors":true,"took":1,"items":[{"index":{"_index":"records","_id":"error","status":400,"error":{"type":"future_error","reason":"` + strings.Repeat("x", 1<<20) + `"}}}]}`
					if chunked {
						w.(http.Flusher).Flush()
					} else {
						w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
					}
					_, _ = io.WriteString(w, raw)
				})
				adapter := testHTTPAdapter(t, handler)
				work := batchTestPlan(t, adapter, "put", "records/s:error")
				// The exact initial promise cannot also hold this tiny request's
				// temporary NDJSON marshal peak: that case must deny before send.
				// The other case approves that known frame and isolates the later
				// unrecognized 1 MiB error, still far beyond available scratch.
				probe := &recordMemoryProbe{limit: work.WorkingBytes + work.IOReserve + framingRoom}
				work.Memory = probe
				works := []*execution.Plan{work}
				var event *pb.Event
				emit := func(_ *execution.Plan, result *pb.Event) error {
					event = result
					return nil
				}
				adapter.Execute(t.Context(), works, emit)
				result := event.GetMutationResult()
				outcome, attempts := pb.MutationOutcome_UNKNOWN, int32(1)
				if framingRoom == 0 {
					outcome, attempts = pb.MutationOutcome_NOT_APPLIED, 0
				}
				if result.GetOutcome() != outcome || result.GetFailure().GetCode() != pb.FailureCode_RESOURCE_EXHAUSTED || calls.Load() != attempts || probe.peak > probe.limit || probe.working != 0 {
					t.Fatal("capacity failure escaped growth approval, changed attempt evidence or replayed", result, calls.Load(), probe.peak, probe.limit, probe.working)
				}
				t.Logf("chunked=%t framingRoom=%d attempts=%d outcome=%s peak=%d limit=%d", chunked, framingRoom, calls.Load(), outcome, probe.peak, probe.limit)
			})
		}
	}
}
