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
	"github.com/batchstream/weir/internal/store"
)

func TestSearchSharedDeadlineCancellationAndDrain(t *testing.T) {
	testSearchCancellationAndDrain(t, false)
}
func TestSearchExpressionCancellationAndDrain(t *testing.T) { testSearchCancellationAndDrain(t, true) }
func testSearchCancellationAndDrain(t *testing.T, expression bool) {
	for _, mode := range []string{"one_caller_expires", "drain", "cancel_all"} {
		t.Run(mode, func(t *testing.T) {
			base, b := setupSearch(t)
			if expression {
				for _, id := range []string{"short", "long"} {
					b.Do(t, "PUT", "/"+b.Index+"/_doc/"+id, `{"n":0}`)
				}
			}
			acknowledged := make(chan struct{}, 1)
			release := make(chan struct{})
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					t.Error(err)
					return
				}
				request.Header = r.Header.Clone()
				response, err := b.Client.Do(request)
				if err != nil {
					if r.Context().Err() == nil {
						t.Error(err)
					}
					return
				}
				defer response.Body.Close()
				body, err := io.ReadAll(io.LimitReader(response.Body, responseLimit))
				if err != nil {
					t.Error(err)
					return
				}
				if r.URL.Path == "/_bulk" || strings.Contains(r.URL.Path, "/_update/") {
					calls.Add(1)
					acknowledged <- struct{}{}
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(response.StatusCode)
				_, _ = w.Write(body)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			cfg := base.config
			cfg.URL = proxy.URL
			adapter, err := Open(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			limits := store.DefaultLimits()
			limits.BatchOperations = 2
			runtime, err := store.New(adapter, limits)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			}()
			short, stopShort := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer stopShort()
			long, stopLong := context.WithTimeout(context.Background(), time.Second)
			defer stopLong()
			first := searchPlan(t, adapter, "put", searchResource(b.Index, "short"))
			second := searchPlan(t, adapter, "put", searchResource(b.Index, "long"))
			if expression {
				var failure *pb.Failure
				first, failure = prepareTestRecord(adapter, expressionOperation(b.Index+"/s:short", `{"doc":{"n":1}}`))
				if failure != nil {
					t.Fatal(failure)
				}
				second, failure = prepareTestRecord(adapter, expressionOperation(b.Index+"/s:long", `{"doc":{"n":1}}`))
				if failure != nil {
					t.Fatal(failure)
				}
			}
			second.ID = 1
			a, failure, _ := runtime.Submit(short, first, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			other, failure, _ := runtime.Submit(long, second, nil)
			if failure != nil {
				t.Fatal(failure)
			}
			select {
			case <-acknowledged:
			case <-time.After(time.Second):
				t.Fatal("native batch never acknowledged")
			}
			if mode == "one_caller_expires" {
				if _, err := a.Wait(short); err == nil {
					t.Fatal("short caller didn't expire")
				}
				close(release)
				result, err := other.Wait(long)
				if err != nil {
					t.Fatal(err)
				}
				assertOutcome(t, result, pb.MutationOutcome_APPLIED, 0)
				other.Ack()
			} else {
				if mode == "cancel_all" {
					stopShort()
					stopLong()
					_, _ = a.Wait(short)
					_, _ = other.Wait(long)
				}
				drain, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				if err := runtime.Close(drain); err != nil {
					t.Fatal(err)
				}
				cancel()
				a.Ack()
				other.Ack()
				close(release)
			}
			expectedCalls := int32(1)
			if calls.Load() != expectedCalls {
				t.Fatal("shared batch split/replayed", calls.Load())
			}
			for _, id := range []string{"short", "long"} {
				status, raw := b.Do(t, "GET", "/"+b.Index+"/_doc/"+id, "")
				if expression {
					want := `"n":1`
					if !strings.Contains(string(raw), want) {
						t.Fatal("unexpected effect", id, string(raw))
					}
				}
				if status != 200 {
					t.Fatal("real effect missing", id, status)
				}
			}
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) && runtime.Snapshot().Retained != 0 {
				time.Sleep(time.Millisecond)
			}
			snapshot := runtime.Snapshot()
			if snapshot.Active != 0 || snapshot.Pending != 0 || snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
				t.Fatal("resources retained", snapshot)
			}
		})
	}
}
