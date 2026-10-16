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
	writeDelay       time.Duration
	bodyDelay, stall bool
	writeStarted     chan struct{}
	writes           atomic.Int32
}

func (b *deadlineBackend) serve(w http.ResponseWriter, r *http.Request) {
	if _, err := io.ReadAll(r.Body); err != nil {
		return
	}
	var delay time.Duration
	var reply string
	switch r.URL.Path {
	case "/":
		reply = `{"version":{"build_flavor":"default"}}`
	case "/_cluster/settings":
		reply = `{"defaults":{"action.auto_create_index":"false"}}`
	case "/records":
		reply = testIndexReply
	case "/records/_mget":
		reply = `{"docs":[{"_index":"records","_id":"write","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`
	case "/_bulk":
		writes := b.writes.Add(1)
		delay, reply = b.writeDelay, deadlineWriteReply
		if b.bodyDelay {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
		}
		if b.writeStarted != nil && writes == 1 {
			close(b.writeStarted)
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

func deadlineRuntime(t *testing.T, backend *deadlineBackend) (*Adapter, *store.Runtime) {
	t.Helper()
	handler := http.HandlerFunc(backend.serve)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	config := Config{Store: "search", URL: server.URL}
	adapter, err := Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	limits := store.DefaultLimits()

	runtime, err := store.New(adapter, limits, nil)
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
	prepared, failure := runtime.PrepareRecord(context.Background(), record)
	if failure != nil {
		t.Fatal(failure)
	}
	return prepared
}

func TestCallerDeadlineAcceptsAcknowledgementAfterTwoSeconds(t *testing.T) {
	for _, body := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[body], func(t *testing.T) {
			backend := &deadlineBackend{writeDelay: 2150 * time.Millisecond, bodyDelay: body}
			_, runtime := deadlineRuntime(t, backend)
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
			writeStarted := make(chan struct{})
			backend := &deadlineBackend{stall: true, bodyDelay: body, writeStarted: writeStarted}
			adapter, runtime := deadlineRuntime(t, backend)
			prepared := deadlineMutation(t, runtime, "put")
			caller, stopCaller := context.WithTimeout(t.Context(), time.Second)
			defer stopCaller()
			ticket, failure, _ := runtime.Submit(caller, prepared, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case <-writeStarted:
			case <-caller.Done():
			}
			// Both can be ready if this goroutine resumes after the deadline.
			select {
			case <-writeStarted:
			default:
				t.Fatal("bulk did not reach the backend before its deadline", caller.Err())
			}
			wait, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			result, err := ticket.Wait(wait)
			if err != nil || result == nil {
				t.Fatal(err, result)
			}
			mutation := result.GetMutationResult()
			if caller.Err() != context.DeadlineExceeded || mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || mutation.GetFailure().GetMessage() != "backend write deadline exceeded; acknowledgement unavailable" || backend.writes.Load() != 1 {
				t.Fatal("deadline lost the sent-write uncertainty or cause", mutation, backend.writes.Load())
			}
			ticket.Ack()
			assertDeadlineReleased(t, runtime)
			until := time.Now().Add(time.Second)
			for searchOwned(adapter.dialer) != 0 && time.Now().Before(until) {
				time.Sleep(time.Millisecond)
			}
			if searchOwned(adapter.dialer) != 0 {
				t.Fatal("aborted request retained its connection")
			}
		})
	}
}

type deadlineObservation struct {
	path        string
	deadline    time.Time
	hasDeadline bool
}

type deadlineObservingTransport struct {
	transport    http.RoundTripper
	observations chan deadlineObservation
}

func (t *deadlineObservingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	deadline, bounded := request.Context().Deadline()
	observation := deadlineObservation{path: request.URL.Path, deadline: deadline, hasDeadline: bounded}
	select {
	case t.observations <- observation:
	case <-request.Context().Done():
		return nil, request.Context().Err()
	}
	return t.transport.RoundTrip(request)
}

func TestCallerDeadlineCoversQualificationReadAndWrite(t *testing.T) {
	backend := &deadlineBackend{}
	adapter, runtime := deadlineRuntime(t, backend)
	observations := make(chan deadlineObservation, 3)
	transport := &deadlineObservingTransport{transport: adapter.client.Transport, observations: observations}
	adapter.client.Transport = transport
	prepared := deadlineMutation(t, runtime, "replace")
	caller, stopCaller := context.WithTimeout(t.Context(), 5*time.Second)
	defer stopCaller()
	ticket, failure, _ := runtime.Submit(caller, prepared, nil)
	if failure != nil {
		t.Fatal(failure)
	}
	result, err := ticket.Wait(caller)
	if err != nil || result == nil {
		t.Fatal(err, result)
	}
	mutation := result.GetMutationResult()
	if mutation.GetOutcome() != pb.MutationOutcome_APPLIED || mutation.GetFailure() != nil || backend.writes.Load() != 1 {
		t.Fatal("shared deadline lost acknowledgement or replayed a write", mutation, backend.writes.Load())
	}
	deadline, _ := caller.Deadline()
	for _, path := range []string{"/records", "/records/_mget", "/_bulk"} {
		select {
		case observation := <-observations:
			if observation.path != path || !observation.hasDeadline || !observation.deadline.Equal(deadline) {
				t.Fatal("backend stage did not retain the original caller deadline", observation, deadline)
			}
		default:
			t.Fatal("missing backend stage", path)
		}
	}
	if len(observations) != 0 {
		t.Fatal("unexpected repeated backend stage", len(observations))
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
		{name: "invalid_response", err: errResponse, code: pb.FailureCode_UNAVAILABLE, message: "backend write response invalid or incomplete; acknowledgement unavailable"},
		{name: "http_status", status: 502, raw: `{"error":{"type":"unknown-secret-sentinel"},"status":502}`, code: pb.FailureCode_UNAVAILABLE, message: "backend write HTTP status 502; acknowledgement unavailable"},
		{name: "invalid_ack", status: 200, raw: `{"errors":false,"took":1,"items":[]}`, code: pb.FailureCode_UNAVAILABLE, message: "backend write acknowledgement invalid or incomplete"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			adapter := &Adapter{}
			work := unsentWritePlan()
			works := []*execution.Plan{work}
			replyInput228 := bulkResponse{works: works, status: test.status, raw: []byte(test.raw), err: test.err}
			results := adapter.bulkResults(context.Background(), replyInput228)
			mutation := results[0].GetMutationResult()
			if mutation.GetOutcome() != pb.MutationOutcome_UNKNOWN || mutation.GetFailure().GetCode() != test.code || mutation.GetFailure().GetMessage() != test.message {
				t.Fatal("uncertainty or sanitized cause changed", mutation)
			}
		})
	}
}
