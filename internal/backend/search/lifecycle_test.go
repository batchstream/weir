package search

import (
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil"
)

func TestSearchTLSOrdinaryNativeSocketCloseBound(t *testing.T) {
	for _, mode := range []string{"header", "body"} {
		t.Run(mode, func(t *testing.T) {
			started := make(chan struct{}, 3)
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if qualification(w, r) {
					return
				}
				if mode == "body" {
					w.Header().Set("Content-Length", "100")
					io.WriteString(w, "{")
					w.(http.Flusher).Flush()
				}
				started <- struct{}{}
				<-r.Context().Done()
			})
			endpoint, c := tlsEndpoint(t, handler, false)
			a := openTestTLS(t, endpoint.URL, c)
			var calls sync.WaitGroup
			for i := 0; i < 3; i++ {
				calls.Go(func() {
					call := exchange{path: "/slow", native: i == 2}
					ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
					_, _, err := a.request(ctx, call)
					cancel()
					if err == nil {
						t.Error("accepted incomplete response")
					}
				})
			}
			for i := 0; i < 3; i++ {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("did not fill normal plus native pools")
				}
			}
			start := time.Now()
			_ = a.Close(context.Background())
			calls.Wait()
			elapsed := time.Since(start)
			if elapsed > time.Second {
				t.Fatal("Close left work running", elapsed)
			}
			_ = a.Close(context.Background())
			t.Logf("normal=2 Native=1 total=3; %s stall Close and call join=%s", mode, elapsed)
		})
	}
}

func TestSearchTLSNativeSlowConsumerCloseJoins(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !qualification(w, r) {
			io.WriteString(w, `{"data":"native"}`)
		}
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	a := openTestTLS(t, endpoint.URL, c)
	started := make(chan struct{})
	emit := func(event *pb.Event) error {
		if event.GetChunk() == nil {
			return nil
		}
		close(started)
		<-a.ctx.Done()
		return io.ErrClosedPipe
	}
	open := nativeRequest(t, "records", "GET", "/_doc/x")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	work, failure := a.prepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	work.Command = testutil.NativeCommand(open)
	done := make(chan *pb.NativeEnd, 1)
	go func() { end := a.executeNative(ctx, work, emit); done <- end }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("consumer not reached")
	}
	start := time.Now()
	_ = a.Close(context.Background())
	select {
	case end := <-done:
		if end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE {
			t.Fatal("stalled Native claimed complete")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join Native publication")
	}
	t.Logf("slow consumer: Close+join=%s", time.Since(start))
}

func TestSearchTLSResponseUsesCallerDeadline(t *testing.T) {
	for _, body := range []bool{false, true} {
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if qualification(w, r) {
				return
			}
			if body {
				w.Header().Set("Content-Length", "100")
				io.WriteString(w, "{")
				w.(http.Flusher).Flush()
			}
			<-r.Context().Done()
		})
		endpoint, c := tlsEndpoint(t, handler, false)
		a := openTestTLS(t, endpoint.URL, c)
		call := exchange{path: "/hard-deadline"}
		start := time.Now()
		ctx, cancel := context.WithTimeout(t.Context(), 80*time.Millisecond)
		_, _, err := a.request(ctx, call)
		cancel()
		elapsed := time.Since(start)
		if err == nil || elapsed > time.Second || elapsed < 60*time.Millisecond {
			t.Fatal("caller response deadline", elapsed, err)
		}
		_ = a.Close(context.Background())
		t.Logf("TLS body=%v caller deadline: call terminated in %s", body, elapsed)
	}
}
