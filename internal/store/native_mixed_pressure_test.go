package store

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"google.golang.org/protobuf/proto"
)

type mixedPressureTrace struct {
	mu      sync.Mutex
	calls   map[string]int
	started map[uint64]bool
	errors  []string
}

func (trace *mixedPressureTrace) record(site string, id uint64, err error) {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	trace.calls[site]++
	if site == "Execute" {
		trace.started[id] = true
	}
	if err != nil && len(trace.errors) < 16 {
		trace.errors = append(trace.errors, fmt.Sprintf("site=%s id=%d error=%v", site, id, err))
	}
}

func (trace *mixedPressureTrace) executed(id uint64) bool {
	trace.mu.Lock()
	defer trace.mu.Unlock()
	return trace.started[id]
}

type mixedPressureAdapter struct {
	execution.Adapter
	trace *mixedPressureTrace
}

type mixedPressureMemory struct {
	execution.Memory
	trace *mixedPressureTrace
	id    uint64
}

func (memory *mixedPressureMemory) SetWorking(ctx context.Context, bytes int) error {
	err := memory.Memory.SetWorking(ctx, bytes)
	memory.trace.record("SetWorking", memory.id, err)
	return err
}

func (memory *mixedPressureMemory) ReserveResult(bytes int) error {
	err := memory.Memory.ReserveResult(bytes)
	memory.trace.record("ReserveResult", memory.id, err)
	return err
}

type mixedPressureIO struct {
	execution.NativeIO
	trace *mixedPressureTrace
	id    uint64
}

func (owner *mixedPressureIO) GrowIO(bytes int) error {
	err := owner.NativeIO.GrowIO(bytes)
	owner.trace.record("GrowIO", owner.id, err)
	return err
}

func (owner *mixedPressureIO) BeginIO(ctx context.Context, bytes int) error {
	err := owner.NativeIO.BeginIO(ctx, bytes)
	owner.trace.record("BeginIO", owner.id, err)
	return err
}

func (owner *mixedPressureIO) EndIO(working, eventBytes int) error {
	err := owner.NativeIO.EndIO(working, eventBytes)
	owner.trace.record("EndIO/settle", owner.id, err)
	return err
}

func (adapter *mixedPressureAdapter) Execute(ctx context.Context, plans []*execution.Plan, emit execution.Emit) {
	for _, work := range plans {
		adapter.trace.record("Execute", work.ID, nil)
		memory := &mixedPressureMemory{Memory: work.Memory, trace: adapter.trace, id: work.ID}
		work.Memory = memory
		if work.NativeIO != nil {
			owner := &mixedPressureIO{NativeIO: work.NativeIO, trace: adapter.trace, id: work.ID}
			work.NativeIO = owner
		}
	}
	adapter.Adapter.Execute(ctx, plans, emit)
}

func TestRealSearchMixedPressureWith32SlowNativeOutputs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	releaseShort := make(chan struct{})
	var shortEntered atomic.Int64
	pad := strings.Repeat("x", 1024)
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/":
			_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
		case "/records":
			_, _ = io.WriteString(w, `{"records":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":true}}}}`)
		case "/slow":
			w.Header().Set("Content-Length", "8388608")
			chunk := strings.Repeat("s", 32<<10)
			for range 256 {
				if _, err := io.WriteString(w, chunk); err != nil {
					return
				}
			}
		case "/short":
			shortEntered.Add(1)
			select {
			case <-releaseShort:
			case <-ctx.Done():
				return
			case <-request.Context().Done():
				return
			}
			_, _ = io.WriteString(w, pad)
		case "/records/_mget":
			var input struct {
				IDs []string `json:"ids"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Error(err)
				return
			}
			_, _ = io.WriteString(w, `{"docs":[`)
			for index, id := range input.IDs {
				if index != 0 {
					_, _ = io.WriteString(w, ",")
				}
				_, _ = fmt.Fprintf(w, `{"_index":"records","_id":%q,"found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1,"pad":%q}}`, id, pad)
			}
			_, _ = io.WriteString(w, "]}")
		case "/_bulk":
			scanner := bufio.NewScanner(request.Body)
			// Finish reading the request before committing the response: net/http
			// may close an unread request body when a large response is flushed.
			var reply strings.Builder
			reply.WriteString(`{"errors":false,"took":1,"items":[`)
			index := 0
			for scanner.Scan() {
				var action map[string]struct {
					ID string `json:"_id"`
				}
				if err := json.Unmarshal(scanner.Bytes(), &action); err != nil {
					t.Error(err)
					return
				}
				for kind, metadata := range action {
					if index != 0 {
						reply.WriteString(",")
					}
					_, _ = fmt.Fprintf(&reply, `{%q:{"_index":"records","_id":%q,"status":200,"result":"updated","_version":2,"_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}`, kind, metadata.ID)
					index++
					if kind != "delete" && !scanner.Scan() {
						t.Error("bulk source missing")
						return
					}
				}
			}
			if err := scanner.Err(); err != nil {
				t.Error(err)
				return
			}
			reply.WriteString("]}")
			_, _ = io.WriteString(w, reply.String())
		default:
			t.Error("unexpected backend request", request.URL.Path)
			http.NotFound(w, request)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer cancel()
	memory := NewExecutionMemory()
	// The CI's observed trusted execution budget and production reserve/default
	// queue, batch and concurrency geometry; no reduced quota or hidden credit.
	memory.known, memory.trusted, memory.budget = true, true, 501079517
	trace := &mixedPressureTrace{calls: make(map[string]int), started: make(map[uint64]bool)}
	runtimes := make([]*Runtime, 0, 2)
	for range 2 {
		config := search.Config{Store: "search", URL: server.URL}
		adapter, err := search.Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		observed := &mixedPressureAdapter{Adapter: adapter, trace: trace}
		runtime, err := New(observed, DefaultLimits(), memory)
		if err != nil {
			t.Fatal(err)
		}
		runtimes = append(runtimes, runtime)
	}
	defer func() {
		cancel()
		cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		for _, runtime := range runtimes {
			if err := runtime.Close(cleanup); err != nil {
				t.Error(err)
			}
		}
		if snapshot := memory.Snapshot(); snapshot.Count != 0 || snapshot.UsedBytes != 0 || snapshot.IngressReservedBytes != 0 {
			t.Error("mixed witness retained actual ownership after cleanup", snapshot)
		}
	}()
	nativeCommand := func(path string) *pb.Command {
		document := &pb.Document{ContentType: "application/http", Data: []byte("GET " + path + " HTTP/1.1\r\nHost: witness\r\n\r\n")}
		request := &pb.NativeRequest{Request: document}
		operation := &pb.Command_Native{Native: request}
		command := &pb.Command{Operation: operation}
		return command
	}
	// Match the decoded-source handoff rather than granting unowned callers an
	// external admission queue. There is no gRPC connection or wire simulation.
	submitOwned := func(caller context.Context, runtime *Runtime, work *execution.Plan, session *Session) (*Ticket, error) {
		account, err := memory.NewIngress(caller, 8192)
		trace.record("NewIngress", work.ID, err)
		if err != nil {
			return nil, err
		}
		defer account.Retire()
		defer account.Close()
		err = account.ReserveMessage(proto.Size(work.Command) + 32)
		trace.record("ReserveMessage", work.ID, err)
		if err != nil {
			return nil, err
		}
		input := account.Input(account.Decoded())
		defer input.Release()
		input.Bind(work)
		for {
			ticket, failure, changed := runtime.Submit(caller, work, session)
			if failure == nil {
				trace.record("admit", work.ID, nil)
				return ticket, nil
			}
			if changed == nil {
				err := fmt.Errorf("%s", failure.GetMessage())
				trace.record("admit/queueCapacity", work.ID, err)
				return nil, err
			}
			trace.record("admit/wait", work.ID, nil)
			select {
			case <-changed:
			case <-caller.Done():
				trace.record("admit/wait", work.ID, caller.Err())
				return nil, caller.Err()
			}
		}
	}
	slowSessions := make([]*Session, 0, 32)
	held := make([]*Emission, 0, 32)
	defer func() {
		for _, emission := range held {
			emission.Release()
		}
		for _, session := range slowSessions {
			session.Close()
		}
	}()
	for index := range 32 {
		runtime := runtimes[index%2]
		session := runtime.NewSession()
		slowSessions = append(slowSessions, session)
		work, failure := runtime.PrepareCommand(ctx, uint64(index+1), nativeCommand("/slow"))
		if failure != nil {
			t.Fatal(failure)
		}
		if _, err := submitOwned(ctx, runtime, work, session); err != nil {
			t.Fatal("slow stream admission", err)
		}
		for {
			select {
			case emission := <-session.Events:
				if emission.Event.GetHead() != nil {
					emission.Release()
					continue
				}
				if len(emission.Event.GetChunk()) == 0 {
					t.Fatal("slow stream did not produce real body bytes", emission.Event)
				}
				held = append(held, emission)
			case <-ctx.Done():
				t.Fatal("slow stream setup deadline", memory.Snapshot())
			}
			break
		}
	}
	logOwners := func(phase string) {
		memory.mu.Lock()
		var preparingNative, heldNative uint64
		for lease := range memory.leases {
			if lease.native {
				if lease.preparing {
					preparingNative += uint64(lease.working)
				} else {
					heldNative += uint64(lease.working)
				}
			}
		}
		claims := len(memory.workingWaits)
		memory.mu.Unlock()
		t.Logf("phase=%s preparing_native_working=%d actual_settled_native_working=%d working_claims=%d ledger=%+v", phase, preparingNative, heldNative, claims, memory.Snapshot())
	}
	logOwners("32-real-8MiB-responses-held-at-first-chunk")
	type callResult struct {
		id   uint64
		kind int
		err  error
	}
	results := make(chan callResult, 256)
	start := make(chan struct{})
	var workers sync.WaitGroup
	for index := range 256 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			caller, stop := context.WithTimeout(ctx, 3*time.Second)
			defer stop()
			id, kind := uint64(1000+index), index%3
			runtime := runtimes[index%2]
			var command *pb.Command
			var work *execution.Plan
			var failure *pb.Failure
			if kind == 2 {
				command = nativeCommand("/short")
				work, failure = runtime.PrepareCommand(caller, id, command)
			} else {
				resource := fmt.Sprintf("records/s:item-%d", id)
				command = &pb.Command{}
				if kind == 0 {
					request := &pb.ReadRequest{Resource: resource}
					operation := &pb.Command_Read{Read: request}
					command.Operation = operation
				} else {
					lua := &pb.LuaTransform{Source: []byte(`return function(current) current.n = current.n + 1; return current end`)}
					form := &pb.Transform_Lua{Lua: lua}
					transform := &pb.Transform{Form: form}
					action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
					request := &pb.MutateRequest{Resource: resource, Action: action}
					operation := &pb.Command_Mutate{Mutate: request}
					command.Operation = operation
				}
				record, err := execution.NewRecord("search", id, command)
				if err != nil {
					result := callResult{id: id, kind: kind, err: err}
					results <- result
					return
				}
				work, failure = runtime.PrepareRecord(caller, record)
			}
			result := callResult{id: id, kind: kind}
			defer func() { results <- result }()
			if failure != nil {
				result.err = fmt.Errorf("prepare: %v", failure)
				return
			}
			var session *Session
			if kind == 2 {
				session = runtime.NewSession()
				defer session.Close()
			}
			ticket, err := submitOwned(caller, runtime, work, session)
			if err != nil {
				result.err = err
				return
			}
			defer ticket.Ack()
			if kind != 2 {
				event, err := ticket.Wait(caller)
				if err != nil {
					result.err = err
				} else if failure := event.GetReadResult().GetFailure(); failure != nil {
					result.err = fmt.Errorf("record read: %v", failure)
				} else if failure := event.GetMutationResult().GetFailure(); failure != nil {
					result.err = fmt.Errorf("record Lua: %v", failure)
				}
				if result.err != nil && !trace.executed(id) {
					trace.record("start/not-dispatched", id, result.err)
				}
				return
			}
			complete := false
			for {
				select {
				case emission := <-session.Events:
					event := emission.Event
					emission.Release()
					if event == nil {
						if !complete {
							result.err = fmt.Errorf("Native ended without completion")
						}
						return
					}
					if end := event.GetNativeEnd(); end != nil {
						complete = end.GetCompletion() == pb.NativeCompletion_RESPONSE_COMPLETE
						if end.GetFailure() != nil {
							result.err = fmt.Errorf("Native: %v", end.GetFailure())
							if !trace.executed(id) {
								trace.record("start/not-dispatched", id, result.err)
							}
						}
					}
				case <-caller.Done():
					result.err = caller.Err()
					return
				}
			}
		}()
	}
	close(start)
	// Gate real initial Native IO only until the bounded mixed burst has built
	// pressure; never hold it for a caller deadline or require consumer progress.
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	gateDeadline := time.NewTimer(250 * time.Millisecond)
	defer gateDeadline.Stop()
	peakUsed, maxClaims, maxHeadDonors := uint64(0), 0, 0
	gateOpen := false
	completed, failed := [3]int{}, 0
	for received := 0; received < 256; {
		select {
		case result := <-results:
			received++
			completed[result.kind]++
			if result.err != nil {
				failed++
				if failed <= 16 {
					t.Logf("mixed failure id=%d kind=%d error=%v", result.id, result.kind, result.err)
				}
			}
		case <-ticker.C:
			memory.mu.Lock()
			peakUsed = max(peakUsed, memory.used)
			maxClaims = max(maxClaims, len(memory.workingWaits))
			if len(memory.workingWaits) != 0 {
				maxHeadDonors = max(maxHeadDonors, len(memory.workingWaits[0].donors))
			}
			pressure := memory.used >= 450<<20
			memory.mu.Unlock()
			if !gateOpen && pressure {
				logOwners("gated-initial-Native-pressure")
				close(releaseShort)
				gateOpen = true
			}
		case <-gateDeadline.C:
			if !gateOpen {
				logOwners("bounded-initial-Native-gate-release")
				close(releaseShort)
				gateOpen = true
			}
		case <-ctx.Done():
			t.Fatal("bounded mixed witness did not join calls", received, memory.Snapshot())
		}
	}
	workers.Wait()
	logOwners("mixed-completed-before-slow-consumer-Ack")
	trace.mu.Lock()
	t.Logf("results read/Lua/Native=%v failed=%d peak_used=%d max_working_claims=%d max_head_donors=%d real_short_requests=%d sites=%v errors=%v", completed, failed, peakUsed, maxClaims, maxHeadDonors, shortEntered.Load(), trace.calls, trace.errors)
	trace.mu.Unlock()
	if failed != 0 || memory.Snapshot().BudgetRejections != 0 {
		t.Errorf("mixed normal operations rejected or failed before slow-consumer Ack: failures=%d ledger=%+v", failed, memory.Snapshot())
	}
}
