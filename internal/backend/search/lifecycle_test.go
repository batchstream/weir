package search

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testdns"
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
					call := exchange{path: "/slow", limit: 256, native: i == 2}
					_, _, err := a.request(context.Background(), call)
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
			if len(a.dialer.slots) != 3 {
				t.Fatal("expected ordinary 2 + native 1 sockets")
			}
			start := time.Now()
			_ = a.Close()
			calls.Wait()
			elapsed := time.Since(start)
			if elapsed > time.Second || len(a.dialer.slots) != 0 {
				t.Fatal("Close left sockets/work", elapsed)
			}
			_ = a.Close()
			t.Logf("normal=2 Native=1 total=3; %s stall Close and call join=%s, retained sockets=0", mode, elapsed)
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
	emit := func(_ *execution.Plan, event *pb.Event) error {
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
	open.GetSearchHttp().Body = nil
	work.Command = testutil.NativeCommand(open)
	done := make(chan *pb.NativeEnd, 1)
	go func() { end, _ := a.executeNative(ctx, work, emit); done <- end }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("consumer not reached")
	}
	start := time.Now()
	_ = a.Close()
	select {
	case end := <-done:
		if end.Completion == pb.NativeCompletion_RESPONSE_COMPLETE {
			t.Fatal("stalled Native claimed complete")
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not join Native publication")
	}
	if len(a.dialer.slots) != 0 {
		t.Fatal("Native retained sockets")
	}
	t.Logf("slow consumer: Close+join=%s; retained sockets=0", time.Since(start))
}

func TestSearchDNSRepeatedCancelledOpenJoins(t *testing.T) {
	dns := testdns.Start(t)
	answer := testdns.Answer{Drop: true}
	dns.Set("search.test", answer)
	cfg := Config{Store: "search", URL: "http://search.test:9200", Pool: 2, Resolver: dns.Resolver()}
	before := runtime.NumGoroutine()
	for i := 0; i < 24; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		start := time.Now()
		a, err := Open(ctx, cfg)
		cancel()
		if a != nil || err == nil || time.Since(start) > time.Second {
			t.Fatal("cancelled Open retained DNS work")
		}
	}
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > before+6 {
		t.Fatal("DNS goroutines grew after repeated cancellations")
	}
	t.Logf("24 cancelled Open attempts: each <1s; goroutines before=%d after=%d", before, runtime.NumGoroutine())
}

func TestSearchTLSResponseHardDeadline(t *testing.T) {
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
		call := exchange{path: "/hard-deadline", limit: 256}
		start := time.Now()
		_, _, err := a.request(context.Background(), call)
		elapsed := time.Since(start)
		if err == nil || elapsed > 3*time.Second || elapsed < 1500*time.Millisecond {
			t.Fatal("2s hard response deadline", elapsed, err)
		}
		_ = a.Close()
		t.Logf("TLS body=%v no caller deadline: call terminated in %s", body, elapsed)
	}
}

func TestSearchDNSRequestCancellationEndsDetachedDial(t *testing.T) {
	dns := testdns.Start(t)
	good := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("search.test", good)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { qualification(w, r) })
	endpoint, c := tlsEndpoint(t, handler, false)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(endpoint.URL, "https://"))
	cfg := Config{Store: "search", URL: "https://search.test:" + port, Pool: 2, Connection: c, Resolver: dns.Resolver()}
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.transport.CloseIdleConnections()
	drop := testdns.Answer{Drop: true}
	dns.Set("search.test", drop)
	for i := 0; i < 16; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		call := exchange{path: "/", limit: metadataLimit}
		_, _, err := a.request(ctx, call)
		cancel()
		if err == nil {
			t.Fatal("dropped DNS succeeded")
		}
		deadline := time.Now().Add(300 * time.Millisecond)
		for len(a.dialer.slots) != 0 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if len(a.dialer.slots) != 0 {
			t.Fatal("request cancellation left a detached resolver until connect timeout")
		}
	}
	t.Log("16 request cancellations: detached HTTP dial/DNS slots returned to zero within 300ms without Adapter.Close")
}

func TestSearchDNSPinsActiveNativeStream(t *testing.T) {
	dns := testdns.Start(t)
	good := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}}
	dns.Set("search.test", good)
	started, release := make(chan struct{}), make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if qualification(w, r) {
			return
		}
		io.WriteString(w, "first")
		w.(http.Flusher).Flush()
		close(started)
		select {
		case <-release:
			io.WriteString(w, "last")
		case <-r.Context().Done():
		}
	})
	endpoint, c := tlsEndpoint(t, handler, false)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(endpoint.URL, "https://"))
	cfg := Config{Store: "search", URL: "https://search.test:" + port, Pool: 2, Connection: c, Resolver: dns.Resolver()}
	a, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	open := nativeRequest(t, "records", "GET", "/_doc/x")
	plan, failure := a.prepareNative(open)
	if failure != nil {
		t.Fatal(failure)
	}
	capture := &nativeCapture{}
	open.GetSearchHttp().Body = nil
	plan.Command = testutil.NativeCommand(open)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan *pb.NativeEnd, 1)
	go func() { end, _ := a.executeNative(ctx, plan, capture.Emit); done <- end }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("stream did not open")
	}
	bad := testdns.Answer{Addresses: []netip.Addr{netip.MustParseAddr("127.0.0.2")}}
	dns.Set("search.test", bad)
	a.transport.CloseIdleConnections()
	requestCtx, stop := context.WithTimeout(context.Background(), 30*time.Millisecond)
	call := exchange{path: "/", limit: metadataLimit}
	_, _, err = a.request(requestCtx, call)
	stop()
	if err == nil {
		t.Fatal("new connection ignored changed DNS")
	}
	close(release)
	select {
	case end := <-done:
		if end.Completion != pb.NativeCompletion_RESPONSE_COMPLETE || capture.body.String() != "firstlast" {
			t.Fatal("active stream migrated", end)
		}
	case <-time.After(time.Second):
		t.Fatal("active stream did not finish")
	}
	dns.Set("search.test", good)
	if _, _, err := a.request(context.Background(), call); err != nil {
		t.Fatal("new independent call failed after DNS restoration")
	}
	t.Log("active Native HTTP/1 stream stayed on original socket across address change; new calls resolved the update")
}
