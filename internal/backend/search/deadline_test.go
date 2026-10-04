package search

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
)

const deadlineWriteReply = `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"write","status":200,"result":"updated","_seq_no":2,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`

type deadlineBackend struct {
	qualificationDelay, readDelay, writeDelay time.Duration
	bodyDelay, stall                          bool
	writes                                    atomic.Int32
}

func (b *deadlineBackend) serve(w http.ResponseWriter, r *http.Request) {
	_, _ = io.ReadAll(r.Body)
	var delay time.Duration
	var reply string
	switch r.URL.Path {
	case "/":
		reply = `{"version":{"build_flavor":"default"}}`
	case "/_cluster/settings":
		reply = `{"defaults":{"action.auto_create_index":"false"}}`
	case "/records":
		delay, reply = b.qualificationDelay, testIndexReply
	case "/records/_mget":
		delay = b.readDelay
		reply = `{"docs":[{"_index":"records","_id":"write","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`
	case "/_bulk":
		b.writes.Add(1)
		delay, reply = b.writeDelay, deadlineWriteReply
		if b.bodyDelay {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
		}
		if b.stall {
			<-r.Context().Done()
			return
		}
	default:
		http.NotFound(w, r)
		return
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.Context().Done():
		return
	}
	_, _ = io.WriteString(w, reply)
}

func deadlineRuntime(t *testing.T, backend *deadlineBackend, timeout time.Duration) (*Adapter, *store.Runtime) {
	t.Helper()
	handler := http.HandlerFunc(backend.serve)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	config := Config{Store: "search", URL: server.URL, Pool: 1}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.BackendTimeout = timeout
	runtime, err := store.New(adapter, limits)
	if err != nil {
		_ = adapter.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := runtime.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return adapter, runtime
}

func deadlineMutation(t *testing.T, runtime *store.Runtime, action string) *execution.Plan {
	t.Helper()
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":2}`)}
	request := &pb.MutateRequest{Resource: "records/s:write"}
	if action == "replace" {
		request.Action = &pb.MutateRequest_Replace{Replace: document}
	} else {
		request.Action = &pb.MutateRequest_Put{Put: document}
	}
	operation := &pb.Command_Mutate{Mutate: request}
	command := &pb.Command{Operation: operation}
	record, err := execution.NewRecord("search", 1, command)
	if err != nil {
		t.Fatal(err)
	}
	prepared, failure := runtime.PrepareRecord(record)
	if failure != nil {
		t.Fatal(failure)
	}
	return prepared
}

func TestConfiguredBackendDeadlineAcceptsAcknowledgementAfterTwoSeconds(t *testing.T) {
	for _, body := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[body], func(t *testing.T) {
			backend := &deadlineBackend{writeDelay: 2150 * time.Millisecond, bodyDelay: body}
			_, runtime := deadlineRuntime(t, backend, 4*time.Second)
			prepared := deadlineMutation(t, runtime, "put")
			caller, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := time.Now()
			ticket, failure, _ := runtime.Submit(caller, prepared, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			result, err := ticket.Wait(caller)
			if err != nil || result.GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || result.GetMutationResult().GetFailure() != nil {
				t.Fatal("legal acknowledgement was cut off by a second deadline", err, result)
			}
			if elapsed := time.Since(started); elapsed < 2*time.Second || elapsed >= 4*time.Second || backend.writes.Load() != 1 {
				t.Fatal("deadline or no-replay contract changed", elapsed, backend.writes.Load())
			}
			ticket.Ack()
			assertDeadlineReleased(t, runtime)
		})
	}
}

func TestConnectedBackendDeadlineIsUnknownAndReleasesOwnership(t *testing.T) {
	for _, body := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[body], func(t *testing.T) {
			backend := &deadlineBackend{stall: true, bodyDelay: body}
			adapter, runtime := deadlineRuntime(t, backend, 80*time.Millisecond)
			prepared := deadlineMutation(t, runtime, "put")
			ticket, failure, _ := runtime.Submit(t.Context(), prepared, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			wait, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			result, err := ticket.Wait(wait)
			if err != nil || result == nil {
				t.Fatal(err, result)
			}
			mutation := result.GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || mutation.GetFailure().GetMessage() != "backend write deadline exceeded; acknowledgement unavailable" || backend.writes.Load() != 1 {
				t.Fatal("deadline lost the sent-write uncertainty or cause", mutation, backend.writes.Load())
			}
			ticket.Ack()
			assertDeadlineReleased(t, runtime)
			until := time.Now().Add(time.Second)
			for len(adapter.dialer.slots) != 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if len(adapter.dialer.slots) != 0 {
				t.Fatal("aborted request retained its connection")
			}
		})
	}
}

func TestBackendDeadlineIsCumulativeAcrossQualificationReadAndWrite(t *testing.T) {
	backend := &deadlineBackend{qualificationDelay: 60 * time.Millisecond, readDelay: 60 * time.Millisecond, writeDelay: 60 * time.Millisecond}
	_, runtime := deadlineRuntime(t, backend, 150*time.Millisecond)
	prepared := deadlineMutation(t, runtime, "replace")
	started := time.Now()
	ticket, failure, _ := runtime.Submit(t.Context(), prepared, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	wait, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	result, err := ticket.Wait(wait)
	if err != nil || result == nil {
		t.Fatal(err, result)
	}
	mutation := result.GetMutationResult()
	if mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || backend.writes.Load() != 1 || time.Since(started) > 500*time.Millisecond {
		t.Fatal("a later backend stage received a fresh deadline", mutation, backend.writes.Load(), time.Since(started))
	}
	ticket.Ack()
	assertDeadlineReleased(t, runtime)
}

func assertDeadlineReleased(t *testing.T, runtime *store.Runtime) {
	t.Helper()
	snapshot := runtime.Snapshot()
	if snapshot.Active != 0 || snapshot.PendingBytes != 0 || snapshot.ResultBytes != 0 || snapshot.WorkingBytes != 0 || snapshot.Retained != 0 {
		t.Fatal("terminal RPC retained ownership", snapshot)
	}
}

func TestBulkUnknownRetainsSanitizedFailureCause(t *testing.T) {
	cases := []struct {
		name, message string
		err           error
		status        int
		raw           string
		code          pb.FailureCode
	}{
		{name: "deadline", err: errTimeout, code: pb.FailureCode_DEADLINE_EXCEEDED, message: "backend write deadline exceeded; acknowledgement unavailable"},
		{name: "canceled", err: errCanceled, code: pb.FailureCode_CANCELLED, message: "backend write canceled; acknowledgement unavailable"},
		{name: "transport", err: errTransport, code: pb.FailureCode_UNAVAILABLE, message: "backend write transport failed; acknowledgement unavailable"},
		{name: "body_limit", err: errResponseLimit, code: pb.FailureCode_RESOURCE_EXHAUSTED, message: "backend write response exceeds byte limit; acknowledgement unavailable"},
		{name: "invalid_response", err: errResponse, code: pb.FailureCode_UNAVAILABLE, message: "backend write response invalid or incomplete; acknowledgement unavailable"},
		{name: "http_status", status: 502, raw: `{"error":{"type":"unknown-secret-sentinel"},"status":502}`, code: pb.FailureCode_UNAVAILABLE, message: "backend write HTTP status 502; acknowledgement unavailable"},
		{name: "invalid_ack", status: 200, raw: `{"errors":false,"took":1,"items":[]}`, code: pb.FailureCode_UNAVAILABLE, message: "backend write acknowledgement invalid or incomplete"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapter := &Adapter{}
			work := unsentWritePlan()
			works := []*execution.Plan{work}
			results, _ := adapter.bulkResults(works, test.status, []byte(test.raw), test.err)
			mutation := results[0].GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != test.code || mutation.GetFailure().GetMessage() != test.message {
				t.Fatal("uncertainty or sanitized cause changed", mutation)
			}
		})
	}
}
