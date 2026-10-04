//go:build integration

package search

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
)

func TestRealSearchBackendDeadlineAcknowledgements(t *testing.T) {
	cases := []struct {
		name    string
		body    bool
		timeout time.Duration
		outcome pb.MutationOutcome
	}{
		{name: "headers_after_two_seconds", timeout: 4 * time.Second, outcome: pb.MutationOutcome_APPLIED},
		{name: "body_after_two_seconds", body: true, timeout: 4 * time.Second, outcome: pb.MutationOutcome_APPLIED},
		{name: "headers_past_deadline", timeout: time.Second, outcome: pb.MutationOutcome_UNKNOWN},
		{name: "body_past_deadline", body: true, timeout: time.Second, outcome: pb.MutationOutcome_UNKNOWN},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			base, backend := setupSearch(t)
			var writes atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				request.Header = r.Header.Clone()
				response, err := backend.Client.Do(request)
				if err != nil {
					if r.Context().Err() == nil {
						t.Error(err)
					}
					return
				}
				defer response.Body.Close()
				raw, err := io.ReadAll(io.LimitReader(response.Body, responseLimit+1))
				if err != nil || len(raw) > responseLimit {
					t.Error("real backend acknowledgement unavailable", err)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/_bulk" {
					writes.Add(1)
					if test.body {
						w.WriteHeader(response.StatusCode)
						w.(http.Flusher).Flush()
					}
					timer := time.NewTimer(2150 * time.Millisecond)
					defer timer.Stop()
					select {
					case <-timer.C:
					case <-r.Context().Done():
						return
					}
				}
				if r.URL.Path != "/_bulk" || !test.body {
					w.WriteHeader(response.StatusCode)
				}
				_, _ = w.Write(raw)
			})
			proxy := httptest.NewServer(handler)
			t.Cleanup(proxy.Close)
			config := base.config
			config.URL = proxy.URL
			config.Pool = 1
			adapter, err := Open(t.Context(), config)
			if err != nil {
				t.Fatal(err)
			}
			limits := store.DefaultLimits()
			limits.Concurrency = 1
			limits.BackendTimeout = test.timeout
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
			document := &pb.Document{MediaType: "application/json", Data: []byte(`{"n":7}`)}
			action := &pb.MutateRequest_Put{Put: document}
			request := &pb.MutateRequest{Resource: backend.Index + "/s:write", Action: action}
			batch := &pb.MutateBatchRequest{StoreName: "search", Requests: []*pb.MutateRequest{request}}
			records, failure := execution.NewMutationRecords(batch, runtime.PendingByteLimit())
			if failure != nil {
				t.Fatal(failure)
			}
			prepared, failure := runtime.PrepareBatch(records)
			if failure != nil {
				t.Fatal(failure)
			}
			caller, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			started := time.Now()
			ticket, failure, _ := runtime.SubmitBatch(caller, prepared)
			if failure != nil {
				t.Fatal(failure)
			}
			results, err := ticket.WaitBatch(caller)
			if err != nil || len(results) != 1 || results[0].Mutation.GetOutcome() != test.outcome {
				t.Fatal("real acknowledgement certainty changed", err, results)
			}
			failure = results[0].Mutation.GetFailure()
			elapsed := time.Since(started)
			if test.outcome == pb.MutationOutcome_APPLIED {
				if failure != nil || elapsed < 2*time.Second || elapsed >= limits.BackendTimeout {
					t.Fatal("real acknowledgement was cut off before the configured deadline", failure, elapsed)
				}
			} else if failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || failure.GetMessage() != "backend write deadline exceeded; acknowledgement unavailable" || elapsed < test.timeout || elapsed > test.timeout+time.Second {
				t.Fatal("lost real acknowledgement discarded its deadline cause", failure, elapsed)
			}
			if writes.Load() != 1 {
				t.Fatal("deadline or no-replay contract changed", elapsed, writes.Load())
			}
			ticket.Ack()
			assertDeadlineReleased(t, runtime)
			status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/write", "")
			if status != http.StatusOK || !strings.Contains(string(raw), `"n":7`) {
				t.Fatal("independent persisted result disagrees with acknowledgement", status, string(raw))
			}
		})
	}
}
