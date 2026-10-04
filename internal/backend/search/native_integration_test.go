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
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testsearch"
)

func TestSearchNativeRealMixedAndReplyLoss(t *testing.T) {
	for _, mode := range []string{"mixed", "multichunk", "drop", "later_invalid", "pipeline"} {
		t.Run(mode, func(t *testing.T) {
			backend := testsearch.Open(t)
			var calls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request, err := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), r.Body)
				if err != nil {
					return
				}
				request.Header = r.Header.Clone()
				request.GetBody = nil
				response, err := backend.Client.Do(request)
				if err != nil {
					w.WriteHeader(502)
					return
				}
				defer response.Body.Close()
				if strings.HasSuffix(r.URL.Path, "/_bulk") {
					calls.Add(1)
					if mode == "drop" {
						io.Copy(io.Discard, response.Body)
						conn, _, _ := w.(http.Hijacker).Hijack()
						conn.Close()
						return
					}
				}
				for key, values := range response.Header {
					for _, v := range values {
						w.Header().Add(key, v)
					}
				}
				w.WriteHeader(response.StatusCode)
				io.Copy(w, response.Body)
			})
			proxy := httptest.NewServer(handler)
			defer proxy.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := Config{Store: "search", URL: proxy.URL, Pool: 1}
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			body := "{\"create\":{\"_id\":\"x\"}}\n{\"n\":1}\n"
			if mode == "mixed" {
				body += body + "{\"index\":{\"_id\":\"bad\"}}\n{\"n\":{\"bad\":1}}\n"
			}
			if mode == "multichunk" {
				body = "{\"index\":{\"_id\":\"x\"}}\n{\"pad\":\"" + strings.Repeat("x", 150<<10) + "\"}\n"
			}
			if mode == "later_invalid" {
				body = strings.Repeat("{\"index\":{\"_id\":\"x\"}}\n{\"n\":1}\n", 100) + "{\"delete\":{\"_id\":\"x\",\"_index\":\"other\"}}\n"
			}
			if mode == "pipeline" {
				status, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_settings", `{"index.default_pipeline":"missing"}`)
				if status != 200 {
					t.Fatal(status)
				}
			}
			open := nativeOpen(t, backend.Index, "POST", "/_bulk")
			end, capture := runNative(t, a, open, []byte(body))
			expected := pb.NativeCompletion_RESPONSE_COMPLETE
			if mode == "drop" || mode == "later_invalid" {
				expected = pb.NativeCompletion_RESPONSE_INCOMPLETE
			}
			if mode == "pipeline" {
				expected = pb.NativeCompletion_NATIVE_NOT_STARTED
			}
			if end.Completion != expected {
				t.Fatal(end, capture.body.String())
			}
			if calls.Load() > 1 || mode == "drop" && calls.Load() != 1 {
				t.Fatal("replay", calls.Load())
			}
			if mode == "mixed" && (!strings.Contains(capture.body.String(), `"errors":true`) || !strings.Contains(capture.body.String(), `version_conflict_engine_exception`)) {
				t.Fatal(capture.body.String())
			}
			if mode == "drop" || mode == "multichunk" || mode == "later_invalid" {
				status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/x", "")
				if mode != "later_invalid" && status != 200 {
					t.Fatal(status, string(raw))
				}
				t.Logf("%s native bulk calls=%d independent GET status=%d", mode, calls.Load(), status)
			}
			if mode == "multichunk" {
				open = nativeOpen(t, backend.Index, "GET", "/_doc/x")
				end, capture = runNative(t, a, open, nil)
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.chunks < 2 {
					t.Fatal(end, capture.chunks)
				}
			}
		})
	}
}

// A request can lose its reply after a validated prefix reaches the backend.
// A later invalid target must never escape the Store and must not trigger replay.
func TestSearchNativeLaterInvalidNeverEscapesOrReplays(t *testing.T) {
	backend := testsearch.Open(t)
	first := "{\"create\":{\"_id\":\"prefix\"}}\n{\"n\":1,\"pad\":\"" + strings.Repeat("x", 70<<10) + "\"}\n"
	var calls atomic.Int32
	var applied atomic.Bool
	finished := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasSuffix(r.URL.Path, "/_bulk") {
			request, _ := http.NewRequestWithContext(r.Context(), r.Method, backend.URL+r.URL.RequestURI(), nil)
			response, err := backend.Client.Do(request)
			if err != nil {
				w.WriteHeader(502)
				return
			}
			defer response.Body.Close()
			w.WriteHeader(response.StatusCode)
			io.Copy(w, response.Body)
			return
		}
		calls.Add(1)
		defer close(finished)
		prefix := make([]byte, len(first))
		if _, err := io.ReadFull(r.Body, prefix); err != nil {
			return
		}
		// Model a backend that commits a received prefix despite a disconnected
		// caller; forwarding uses its own bounded context rather than replay.
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		request, _ := http.NewRequestWithContext(ctx, "POST", backend.URL+r.URL.Path, strings.NewReader(string(prefix)))
		request.GetBody = nil
		request.Header.Set("Content-Type", "application/x-ndjson")
		response, err := backend.Client.Do(request)
		if err != nil {
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		applied.Store(response.StatusCode == 200 && strings.Contains(string(raw), `"errors":false`))
		// The scoped reader must reject the foreign target before sending it.
		remaining, _ := io.ReadAll(io.LimitReader(r.Body, 4096))
		if len(remaining) != 0 {
			t.Error("invalid item escaped scope validation", string(remaining))
		}
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	cfg := Config{Store: "search", URL: proxy.URL, Pool: 1}
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	open := nativeOpen(t, backend.Index, "POST", "/_bulk")
	p, f := a.prepareNative(open)
	if f != nil {
		t.Fatal(f)
	}
	body := first + "{\"delete\":{\"_id\":\"prefix\",\"_index\":\"outside\"}}\n"
	p.Command = testutil.NativeCommand(open, []byte(body))
	capture := &nativeCapture{}
	end, _ := a.executeNative(ctx, p, capture.Emit)
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE {
		t.Fatal(end)
	}
	// Transport cancellation can prevent even the validated prefix from
	// reaching the proxy. If it arrived, wait for the independently committed
	// write and verify the result without claiming cancellation undoes effects.
	proxy.Close()
	if calls.Load() > 1 {
		t.Fatal("Native upload replayed", calls.Load())
	}
	if calls.Load() == 1 {
		select {
		case <-finished:
		case <-ctx.Done():
			t.Fatal("Native proxy did not join")
		}
	}
	status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/prefix", "")
	if applied.Load() && (status != 200 || !strings.Contains(string(raw), `"n":1`)) {
		t.Fatal("confirmed prefix did not persist", status, string(raw))
	}
	status, raw = backend.Do(t, "GET", "/outside/_doc/prefix", "")
	if status != 404 {
		t.Fatal("foreign Native target escaped", status, string(raw))
	}
	t.Logf("Native calls=%d committed prefix=%t; foreign target rejected, no replay", calls.Load(), applied.Load())
}
