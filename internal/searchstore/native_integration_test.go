//go:build integration

package searchstore

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testsearch"
)

func TestSearchNativeRealMixedAndReplyLoss(t *testing.T) {
	for _, mode := range []string{"mixed", "multiframe", "drop", "later_invalid", "pipeline"} {
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
			cfg := Config{Store: "search", URL: proxy.URL, Index: backend.Index, Profile: backend.Profile, Pool: 1}
			a, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer a.Close()
			body := "{\"create\":{\"_id\":\"x\"}}\n{\"n\":1}\n"
			if mode == "mixed" {
				body += body + "{\"index\":{\"_id\":\"bad\"}}\n{\"n\":{\"bad\":1}}\n"
			}
			if mode == "multiframe" {
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
			end, capture := runNative(t, a, open, io.NopCloser(strings.NewReader(body)))
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
			if mode == "drop" || mode == "multiframe" || mode == "later_invalid" {
				status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/x", "")
				if mode != "later_invalid" && status != 200 {
					t.Fatal(status, string(raw))
				}
				t.Logf("%s native bulk calls=%d independent GET status=%d", mode, calls.Load(), status)
			}
			if mode == "multiframe" {
				open = nativeOpen(t, backend.Index, "GET", "/_doc/x")
				end, capture = runNative(t, a, open, io.NopCloser(strings.NewReader("")))
				if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.chunks < 2 {
					t.Fatal(end, capture.chunks)
				}
			}
		})
	}
}

// This fault proxy deliberately finalizes the first validated item as a complete
// native upload. It is a request-framing fault experiment, not a claim that the
// normal ES/OS bulk endpoint processes an unfinished chunked request.
func TestSearchNativeLaterInvalidAfterRealPrefixApplied(t *testing.T) {
	backend := testsearch.Open(t)
	first := "{\"create\":{\"_id\":\"prefix\"}}\n{\"n\":1,\"pad\":\"" + strings.Repeat("x", 70<<10) + "\"}\n"
	applied := make(chan struct{})
	var calls atomic.Int32
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
		// Forward exactly one real command before asking the sender for a later item.
		prefix := make([]byte, len(first))
		if _, err := io.ReadFull(r.Body, prefix); err != nil {
			return
		}
		request, _ := http.NewRequestWithContext(r.Context(), "POST", backend.URL+r.URL.Path, strings.NewReader(string(prefix)))
		request.GetBody = nil
		request.Header.Set("Content-Type", "application/x-ndjson")
		calls.Add(1)
		response, err := backend.Client.Do(request)
		if err != nil {
			return
		}
		raw, _ := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if response.StatusCode != 200 || !strings.Contains(string(raw), `"errors":false`) {
			return
		}
		close(applied)
		// Discard the real successful reply and await the later invalid client item.
		io.Copy(io.Discard, r.Body)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := Config{Store: "search", URL: proxy.URL, Index: backend.Index, Profile: backend.Profile, Pool: 1}
	a, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	source, writer := io.Pipe()
	defer source.Close()
	defer writer.Close()
	sent := make(chan error, 1)
	go func() {
		if _, err := io.WriteString(writer, first); err != nil {
			sent <- err
			return
		}
		select {
		case <-applied:
		case <-ctx.Done():
			writer.CloseWithError(ctx.Err())
			sent <- ctx.Err()
			return
		}
		_, err := io.WriteString(writer, "{\"delete\":{\"_id\":\"prefix\",\"_index\":\"outside\"}}\n")
		writer.Close()
		sent <- err
	}()
	open := nativeOpen(t, backend.Index, "POST", "/_bulk")
	p, f := a.PrepareNative(open)
	if f != nil {
		t.Fatal(f)
	}
	capture := &nativeCapture{}
	exchange := &execution.NativeExchange{Source: source, Sink: capture}
	end, _ := a.ExecuteNative(ctx, p, exchange)
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || calls.Load() != 1 {
		t.Fatal(end, calls.Load())
	}
	status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/prefix", "")
	if status != 200 || !strings.Contains(string(raw), `"n":1`) {
		t.Fatal(status, string(raw))
	}
	t.Log("fault proxy finalized one validated prefix; native write persisted; later foreign target rejected; no replay")
}
