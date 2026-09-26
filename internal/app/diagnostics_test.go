package app

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/testmetrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func diagnosticNode(t *testing.T) *Node {
	t.Helper()
	cfg := remoteConfig(t)
	cfg.Diagnostics = "127.0.0.1:0"
	cfg.Limits.Sessions = 1
	cfg.Limits.StallMS = 500
	node, err := Open(context.Background(), cfg)
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
	n.Start()
	second.Start()
	if health(t, n, "/readyz") != 200 || health(t, n, "/livez") != 200 {
		t.Fatal("not serving")
	}
	conn, err := grpc.NewClient("passthrough:///"+n.Addresses()[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy(), grpc.WithDisableRetry())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	read := &pb.ReadRequest{Resource: "weir://records/db/records/s:missing"}
	_, _ = client.Read(ctx, read) // observed failed dial, no synthetic backend work
	if health(t, n, "/readyz") != 200 {
		t.Fatal("remote failure changed readiness")
	}
	n.admission.SetOverloaded(true)
	_, _ = client.Read(ctx, read)
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
			t.Fatal("forward-only created Runtime metrics")
		}
	}
	other := testmetrics.Scrape(t, second.DiagnosticAddress())
	if testmetrics.Sum(other, "weir_admission_rejections_total") != 0 || testmetrics.Sum(other, "weir_relay_terminations_total") != 0 {
		t.Fatal("registries shared counters")
	}
	if len(n.runtimes) != 0 || len(n.targets) != 1 {
		t.Fatal("fake Runtime")
	}
	n.admission.SetOverloaded(false)
	// Hold one real decoded-input slot while drain starts; diagnostics retain
	// their own slots and must remain available after data readiness falls.
	desc := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
	blocked, err := conn.NewStream(ctx, desc, pb.Weir_Read_FullMethodName)
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
	_, _ = client.Read(ctx, read)
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
		n.Start()
		if err := n.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	after := testmetrics.Registry(t, n.registry)
	if testmetrics.Sum(after, "weir_node_drains_total") != 1 || testmetrics.Sample(after, "weir_node_state", map[string]string{"state": "closed"}).GetGauge().GetValue() != 1 {
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
	n.Start()
	before := testmetrics.Scrape(t, n.DiagnosticAddress())
	conn, err := grpc.NewClient("passthrough:///"+n.Addresses()[0], grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := pb.NewWeirClient(conn)
	random := rand.New(rand.NewPCG(6, 1))
	for i := 0; i < 100; i++ {
		nonce := fmt.Sprintf("%x", random.Uint64())
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "weir-request-id", "secret-request-"+nonce)
		req := &pb.ReadRequest{Resource: "weir://unknown" + nonce + "/private/s:document-secret"}
		_, _ = client.Read(ctx, req)
		var output pb.ReadResult
		_ = conn.Invoke(ctx, "/unknown"+nonce+"/Method", req, &output)
		cancel()
		if health(t, n, fmt.Sprintf("/unknown%d?credential=secret", i)) != 400 {
			t.Fatal("query accepted")
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
	if testmetrics.Sample(after, "weir_admission_rejections_total", map[string]string{"reason": "route"}).GetCounter().GetValue() != 100 {
		t.Fatal("unknown route counter")
	}
}
func TestDiagnosticsConnectionLimitsDeadlinesAndStartupFailure(t *testing.T) {
	for _, address := range []string{"localhost:1", ":0", "0.0.0.0:0", "[::]:0", "192.0.2.1:1"} {
		cfg := remoteConfig(t)
		cfg.Diagnostics = address
		if cfg.Validate() == nil {
			t.Fatal("non-loopback", address)
		}
	}
	n := diagnosticNode(t)
	n.Start()
	var sockets []net.Conn
	defer func() {
		for _, conn := range sockets {
			_ = conn.Close()
		}
	}()
	for range diagnosticConnections {
		conn, err := net.Dial("tcp", n.DiagnosticAddress())
		if err != nil {
			t.Fatal(err)
		}
		sockets = append(sockets, conn)
		_, _ = io.WriteString(conn, "GET /metrics HTTP/1.1\r\nHost: local\r\nX-Slow:")
	}
	until := time.Now().Add(time.Second)
	for len(n.diagnostics.connections) != diagnosticConnections {
		if time.Now().After(until) {
			t.Fatal("connections not occupied")
		}
		time.Sleep(time.Millisecond)
	}
	extra, err := net.Dial("tcp", n.DiagnosticAddress())
	if err != nil {
		t.Fatal(err)
	}
	defer extra.Close()
	_ = extra.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	var b [1]byte
	if _, err := extra.Read(b[:]); err == nil {
		t.Fatal("connection limit")
	} else if e, ok := err.(net.Error); ok && e.Timeout() {
		t.Fatal("queued instead of rejecting")
	}
	time.Sleep(diagnosticTimeout + 100*time.Millisecond)
	if len(n.diagnostics.connections) != 0 || health(t, n, "/livez") != 200 {
		t.Fatal("slow headers retained connections")
	}
	// Oversized headers and body-bearing requests are bounded by the same path.
	for _, raw := range []string{"GET /metrics HTTP/1.1\r\nHost: local\r\nBig: " + strings.Repeat("x", 16<<10) + "\r\n\r\n", "POST /metrics HTTP/1.1\r\nHost: local\r\nContent-Length: 1000000000\r\n\r\n"} {
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
		if response.StatusCode != 431 && response.StatusCode != 400 {
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
	cfg := remoteConfig(t)
	cfg.Application = first.Addr().String()
	cfg.Diagnostics = occupied.Addr().String()
	_ = first.Close()
	for range 3 {
		if node, err := Open(context.Background(), cfg); err == nil || node != nil {
			t.Fatal("diagnostic port conflict ignored")
		}
	}
	first, err = net.Listen("tcp", cfg.Application)
	if err != nil {
		t.Fatal("partial startup listener leaked", err)
	}
	_ = first.Close()
}
func TestDiagnosticsScrapeCloseRace(t *testing.T) {
	n := diagnosticNode(t)
	n.Start()
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
	if testmetrics.Sum(testmetrics.Registry(t, n.registry), "weir_node_drains_total") != 1 {
		t.Fatal("double drain")
	}
}

type smallSendListener struct{ net.Listener }

func (l *smallSendListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetWriteBuffer(1024)
	}
	return conn, nil
}
func TestDiagnosticsStoppedScrapesAndConcurrentHandlersBounded(t *testing.T) {
	cfg := remoteConfig(t)
	cfg.Diagnostics = "127.0.0.1:0"
	definition := cfg.Services[0]
	cfg.Services = nil
	cfg.Routes = nil
	for i := 0; i < 16; i++ {
		definition.Name = fmt.Sprintf("%s%d", strings.Repeat("r", 58), i)
		cfg.Services = append(cfg.Services, definition)
		route := Route{Store: fmt.Sprintf("store%d", i), Service: definition.Name}
		cfg.Routes = append(cfg.Routes, route)
	}
	n, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer n.Close(context.Background())
	listener := &smallSendListener{Listener: n.diagnostics.listener}
	n.diagnostics.listener = listener
	n.Start()
	for round := 0; round < 3; round++ {
		var sockets []net.Conn
		for range diagnosticHandlers {
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
		until := time.Now().Add(500 * time.Millisecond)
		for len(n.diagnostics.slots) != diagnosticHandlers {
			if time.Now().After(until) {
				t.Fatal("scrapes did not block at output")
			}
			time.Sleep(time.Millisecond)
		}
		time.Sleep(80 * time.Millisecond)
		if len(n.diagnostics.slots) != diagnosticHandlers {
			t.Fatal("scrapes were not held by stopped reader")
		}
		started := time.Now()
		if health(t, n, "/livez") != 503 || time.Since(started) > 200*time.Millisecond {
			t.Fatal("handler capacity queued")
		}
		until = time.Now().Add(2 * time.Second)
		for len(n.diagnostics.slots) != 0 || len(n.diagnostics.connections) != 0 {
			if time.Now().After(until) {
				t.Fatal("stopped reader retained response/handler")
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
	families := testmetrics.Scrape(t, n.DiagnosticAddress())
	if testmetrics.Sample(families, "weir_diagnostic_rejections_total", map[string]string{"reason": "handlers"}).GetCounter().GetValue() != 3 {
		t.Fatal("handler rejection count")
	}
}

func TestDiagnosticsDisabledAndFatalListenerReadiness(t *testing.T) {
	cfg := remoteConfig(t)
	disabled, err := Open(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if disabled.diagnostics != nil || disabled.DiagnosticAddress() != "" {
		t.Fatal("disabled diagnostics created listener")
	}
	disabled.Start()
	_ = disabled.Close(context.Background())
	n := diagnosticNode(t)
	n.Start()
	_ = n.listeners[0].Close()
	select {
	case <-n.Errors:
	case <-time.After(time.Second):
		t.Fatal("fatal listener error not reported")
	}
	if health(t, n, "/readyz") != 503 || health(t, n, "/livez") != 200 {
		t.Fatal("fatal listener readiness")
	}
}
