package app

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func diagnosticNode(t *testing.T) *Node {
	t.Helper()
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	transport := server.DefaultLimits()
	transport.Stall = 500 * time.Millisecond
	node, err := open(context.Background(), cfg, transport)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := node.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	return node
}

func health(t *testing.T, node *Node, path string) int {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	response, err := client.Get("http://" + node.DiagnosticAddress() + path)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	return response.StatusCode
}

func TestDiagnosticsLifecycleIsolationAndNoSyntheticExecutions(t *testing.T) {
	n := diagnosticNode(t)
	second := diagnosticNode(t)
	request := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	recorder := httptest.NewRecorder()
	n.diagnostics.http.Handler.ServeHTTP(recorder, request)
	if recorder.Code != 503 || n.ready() {
		t.Fatal("ready before Start")
	}
	n.Start(context.Background())
	second.Start(context.Background())
	if health(t, n, "/readyz") != 200 || health(t, n, "/livez") != 200 {
		t.Fatal("not serving")
	}
	conn, err := grpc.NewClient("passthrough:///"+n.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewStoreServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	read := &pb.ReadRequest{Resource: "db/records/s:missing"}
	_, _ = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", read)) // observed failed dial, no synthetic backend work
	if health(t, n, "/readyz") != 200 {
		t.Fatal("unknown Store changed readiness")
	}
	n.admission.SetOverloaded(true)
	_, _ = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", read))
	if health(t, n, "/readyz") != 200 {
		t.Fatal("overload changed readiness")
	}
	before := testmetrics.Scrape(t, n.DiagnosticAddress())
	for range 5 {
		after := testmetrics.Scrape(t, n.DiagnosticAddress())
		if testmetrics.Sum(after, "weir_admission_rejections_total") != testmetrics.Sum(before, "weir_admission_rejections_total") {
			t.Fatal("scrape increased business counters")
		}
		if after["weir_store_executions_total"] != nil {
			t.Fatal("a node without Stores created Runtime metrics")
		}
	}
	other := testmetrics.Scrape(t, second.DiagnosticAddress())
	if testmetrics.Sum(other, "weir_admission_rejections_total") != 0 {
		t.Fatal("registries shared counters")
	}
	if len(n.stores) != 0 || n.guard.Snapshot().Used == 0 {
		t.Fatal("fake Runtime")
	}
	n.admission.SetOverloaded(false)
	// Hold one real decoded-input slot while drain starts; diagnostics retain
	// independent request lifetimes and must remain available after data readiness falls.
	desc := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
	blocked, err := conn.NewStream(ctx, desc, pb.StoreService_Execute_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	_ = blocked
	deadline := time.Now().Add(time.Second)
	for testmetrics.Sum(testmetrics.Registry(t, n.registry), "weir_ingress_sessions") != 1 {
		if time.Now().After(deadline) {
			t.Fatal("slot not held")
		}
		time.Sleep(time.Millisecond)
	}
	_, _ = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", read))
	if health(t, n, "/readyz") != 200 {
		t.Fatal("session capacity changed readiness")
	}
	closed := make(chan error, 1)
	go func() { closed <- n.Close(context.Background()) }()
	for n.ready() {
		time.Sleep(time.Millisecond)
	}
	if health(t, n, "/readyz") != 503 || health(t, n, "/livez") != 200 {
		t.Fatal("drain health")
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	for range 3 {
		n.Start(context.Background())
		if err := n.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	after := testmetrics.Registry(t, n.registry)
	if testmetrics.Sum(after, "weir_node_ready") != 0 || n.state != "closed" {
		t.Fatal("non-monotonic close")
	}
	if testmetrics.Sum(after, "weir_ingress_sessions") != 0 {
		t.Fatal("drain leaked session")
	}
	if conn, err := net.DialTimeout("tcp", n.DiagnosticAddress(), 50*time.Millisecond); err == nil {
		_ = conn.Close()
		t.Fatal("diagnostics remained open")
	}
}

func TestDiagnosticsInputCardinalityAndNoSecrets(t *testing.T) {
	n := diagnosticNode(t)
	n.Start(context.Background())
	before := testmetrics.Scrape(t, n.DiagnosticAddress())
	conn, err := grpc.NewClient("passthrough:///"+n.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewStoreServiceClient(conn)
	random := rand.New(rand.NewPCG(6, 1))
	for i := 0; i < 100; i++ {
		nonce := fmt.Sprintf("%x", random.Uint64())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "weir-request-id", "secret-request-"+nonce)
		req := &pb.ReadRequest{Resource: "private/s:document-secret"}
		_, _ = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("unknown"+nonce, req))
		var output pb.ReadResult
		_ = conn.Invoke(ctx, "/unknown"+nonce+"/Method", req, &output)
		cancel()
		if health(t, n, fmt.Sprintf("/unknown%d?credential=secret", i)) != http.StatusNotFound {
			t.Fatal("unknown diagnostic path served")
		}
	}
	after := testmetrics.Scrape(t, n.DiagnosticAddress())
	if testmetrics.Series(before) != testmetrics.Series(after) {
		t.Fatal("dynamic series", testmetrics.Series(before), testmetrics.Series(after))
	}
	for _, family := range after {
		text := family.String()
		for _, secret := range []string{"weir://", "127.0.0.1:", "private", "secret", "node.weir.test", "unknown99"} {
			if strings.Contains(text, secret) {
				t.Fatal("diagnostic leak", family.GetName(), secret)
			}
		}
	}
	if testmetrics.Sample(after, "weir_admission_rejections_total", map[string]string{"reason": "execute"}).GetCounter().GetValue() != 100 {
		t.Fatal("unknown Store execution counter")
	}
}

func TestDiagnosticsConnectionLimitsDeadlinesAndStartupFailure(t *testing.T) {
	for _, address := range []string{"localhost:1", ":0", "127.0.0.1"} {
		cfg := emptyConfig(t)
		cfg.Listeners.Diagnostics = address
		if cfg.Validate() == nil {
			t.Fatal("diagnostics listener without an explicit IP and port accepted", address)
		}
	}
	n := diagnosticNode(t)
	n.Start(context.Background())
	var sockets []net.Conn
	defer func() {
		for _, conn := range sockets {
			_ = conn.Close()
		}
	}()
	for range 16 {
		conn, err := net.Dial("tcp", n.DiagnosticAddress())
		if err != nil {
			t.Fatal(err)
		}
		sockets = append(sockets, conn)
		_, _ = io.WriteString(conn, "GET /metrics HTTP/1.1\r\nHost: local\r\nX-Slow:")
	}
	if health(t, n, "/livez") != http.StatusOK {
		t.Fatal("slow connections blocked an independent diagnostic request")
	}
	// The header deadline closes every slow connection.
	for _, conn := range sockets {
		requireServerClosed(t, conn)
	}
	if health(t, n, "/livez") != http.StatusOK {
		t.Fatal("diagnostics did not recover from slow headers")
	}
	// Oversized headers and body-bearing requests are bounded by the same path.
	for _, raw := range []string{
		"GET /metrics HTTP/1.1\r\nHost: local\r\nBig: " + strings.Repeat("x", 16<<10) + "\r\n\r\n",
		"POST /metrics HTTP/1.1\r\nHost: local\r\nContent-Length: 1000000000\r\n\r\n",
	} {
		conn, err := net.Dial("tcp", n.DiagnosticAddress())
		if err != nil {
			t.Fatal(err)
		}
		_ = conn.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = io.WriteString(conn, raw)
		response, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge && response.StatusCode != http.StatusMethodNotAllowed {
			t.Fatal(response.StatusCode)
		}
		_ = response.Body.Close()
		_ = conn.Close()
	}
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupied.Close()
	first, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	cfg := emptyConfig(t)
	cfg.Listeners.Application = first.Addr().String()
	cfg.Listeners.Diagnostics = occupied.Addr().String()
	_ = first.Close()
	for range 3 {
		if node, err := Open(context.Background(), cfg); err == nil || node != nil {
			t.Fatal("diagnostic port conflict ignored")
		}
	}
	first, err = net.Listen("tcp", cfg.Listeners.Application)
	if err != nil {
		t.Fatal("partial startup listener leaked", err)
	}
	_ = first.Close()
}

func TestWildcardDiagnosticsListener(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "0.0.0.0:0"
	node, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = node.Close(context.Background()) })
	if err := node.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(node.DiagnosticAddress())
	if err != nil {
		t.Fatal(err)
	}
	metrics := testmetrics.Scrape(t, "127.0.0.1:"+port)
	if metrics["weir_node_ready"] == nil {
		t.Fatal("wildcard diagnostic listener did not serve metrics")
	}
}

func TestDiagnosticsScrapeCloseRace(t *testing.T) {
	n := diagnosticNode(t)
	n.Start(context.Background())
	var group sync.WaitGroup
	for range 2 {
		group.Go(func() {
			for range 20 {
				testmetrics.Registry(t, n.registry)
			}
		})
	}
	for range 3 {
		group.Go(func() {
			if err := n.Close(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	if testmetrics.Sum(testmetrics.Registry(t, n.registry), "weir_node_ready") != 0 || n.state != "closed" {
		t.Fatal("concurrent Close left the node open")
	}
}

// requireServerClosed drains conn until the server closes it, failing if that
// takes longer than the diagnostic deadline allows.
func requireServerClosed(t *testing.T, conn net.Conn) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(diagnosticTimeout + 2*time.Second))
	_, err := io.Copy(io.Discard, conn)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatal("diagnostics retained a stalled connection")
	}
}

// smallSendListener shrinks each accepted connection's send buffer and counts
// the connections the server has not closed yet.
type smallSendListener struct {
	net.Listener
	open atomic.Int64
}

func (l *smallSendListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(1024)
	}
	l.open.Add(1)
	tracked := &trackedConn{Conn: conn, owner: l}
	return tracked, nil
}

type trackedConn struct {
	net.Conn
	owner *smallSendListener
	once  sync.Once
}

func (c *trackedConn) Close() error {
	c.once.Do(func() { c.owner.open.Add(-1) })
	return c.Conn.Close()
}

func TestDiagnosticsStoppedScrapesAndConcurrentHandlersBounded(t *testing.T) {
	cfg := emptyConfig(t)
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	n, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	// A bounded test collector produces enough output to exercise blocked HTTP sends.
	opts := prometheus.GaugeOpts{Name: "weir_test_scrape_padding", Help: "Test-only bounded scrape payload."}
	padding := prometheus.NewGaugeVec(opts, []string{"entry"})
	for i := 0; i < 128; i++ {
		padding.WithLabelValues(fmt.Sprintf("%s%d", strings.Repeat("x", 512), i)).Set(1)
	}
	if err := n.registry.Register(padding); err != nil {
		t.Fatal(err)
	}
	listener := &smallSendListener{Listener: n.diagnostics.listener}
	n.diagnostics.listener = listener
	n.Start(context.Background())
	for round := 0; round < 3; round++ {
		var sockets []net.Conn
		for range 8 {
			conn, err := net.Dial("tcp", n.DiagnosticAddress())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			if tcp, ok := conn.(*net.TCPConn); ok {
				_ = tcp.SetReadBuffer(1024)
			}
			_, _ = io.WriteString(conn, "GET /metrics HTTP/1.1\r\nHost: local\r\n\r\n")
			sockets = append(sockets, conn)
		}
		time.Sleep(80 * time.Millisecond)
		started := time.Now()
		if health(t, n, "/livez") != 200 || time.Since(started) > 200*time.Millisecond {
			t.Fatal("blocked scrapes prevented health requests")
		}
		// The write deadline ends every scrape its reader stopped consuming.
		// Watch the server's side: the kernel can take seconds to drain a
		// closed socket's buffered bytes through these tiny buffers.
		until := time.Now().Add(diagnosticTimeout + time.Second)
		for listener.open.Load() != 0 {
			if time.Now().After(until) {
				t.Fatal("diagnostics retained a stalled scrape", listener.open.Load())
			}
			time.Sleep(5 * time.Millisecond)
		}
		for _, conn := range sockets {
			_ = conn.Close()
		}
		if health(t, n, "/livez") != 200 {
			t.Fatal("diagnostics failed to recover")
		}
	}

}

func TestDiagnosticsDisabledAndFatalListenerReadiness(t *testing.T) {
	cfg := emptyConfig(t)
	disabled, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.diagnostics != nil || disabled.DiagnosticAddress() != "" {
		t.Fatal("disabled diagnostics created listener")
	}
	disabled.Start(context.Background())
	_ = disabled.Close(context.Background())
	n := diagnosticNode(t)
	n.Start(context.Background())
	_ = n.listener.Close()
	select {
	case <-n.Errors:
	case <-time.After(time.Second):
		t.Fatal("fatal listener error not reported")
	}
	if health(t, n, "/readyz") != 503 || health(t, n, "/livez") != 200 {
		t.Fatal("fatal listener readiness")
	}
}
