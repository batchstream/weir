//go:build darwin || linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"go.yaml.in/yaml/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// This fixture speaks only enough synthetic HTTP to reach lifecycle boundaries.
// It is not a database and does not test database correctness.
type startupBackend struct {
	server                     *httptest.Server
	hold                       string
	entered, canceled, release chan struct{}
	mu                         sync.Mutex
	requests                   []string
	connections, peak          int
	changed                    chan struct{}
}

func newStartupBackend(t *testing.T, hold string) *startupBackend {
	t.Helper()

	b := &startupBackend{
		hold:     hold,
		entered:  make(chan struct{}, 1),
		canceled: make(chan struct{}, 1),
		release:  make(chan struct{}),
		changed:  make(chan struct{}),
	}
	handler := http.HandlerFunc(b.handle)
	b.server = httptest.NewUnstartedServer(handler)
	b.server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		b.mu.Lock()
		defer b.mu.Unlock()
		if state == http.StateNew {
			b.connections++
			b.peak = max(b.peak, b.connections)
		} else if state == http.StateClosed || state == http.StateHijacked {
			b.connections--
		}
		close(b.changed)
		b.changed = make(chan struct{})
	}
	b.server.Start()
	t.Cleanup(func() {
		close(b.release)
		b.server.Close()
	})
	return b
}

func (b *startupBackend) handle(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10))
	b.mu.Lock()
	b.requests = append(b.requests, r.Method+" "+r.URL.Path)
	b.mu.Unlock()

	if r.URL.Path == b.hold {
		select {
		case b.entered <- struct{}{}:
		default:
			http.Error(w, "duplicate held request", http.StatusInternalServerError)
			return
		}
		select {
		case <-r.Context().Done():
			select {
			case b.canceled <- struct{}{}:
			default:
			}
		case <-b.release:
		}
		return
	}

	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/":
		if b.hold == "invalid-product" {
			http.Error(w, "backend-error-sentinel", http.StatusBadGateway)
			return
		}
		fmt.Fprintf(w, `{"version":{"number":%q,"build_flavor":"default"}}`, "8.19.22")
	case "/_cluster/settings":
		io.WriteString(w, `{"defaults":{"action.auto_create_index":"false"}}`)
	case "/records":
		io.WriteString(w, `{"records":{"settings":{"index.uuid":"synthetic","index.number_of_shards":"1"},"mappings":{}}}`)
	default:
		http.Error(w, "backend-error-sentinel", http.StatusNotFound)
	}
}

func (b *startupBackend) config() app.Config {
	backend := &app.Search{
		URL: b.server.URL,
	}
	service := app.StoreConfig{Name: "records", Search: backend, Batching: app.Batching{MaxOperations: 1}}

	cfg := app.Config{}
	cfg.Listeners.Application, cfg.Listeners.Diagnostics = "127.0.0.1:0", "127.0.0.1:0"
	cfg.Stores = []app.StoreConfig{service}
	return cfg
}

func (b *startupBackend) idle(t *testing.T) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()

	for {
		b.mu.Lock()
		count, peak, changed := b.connections, b.peak, b.changed
		requests := append([]string(nil), b.requests...)
		b.mu.Unlock()
		if count == 0 {
			if peak > 4 {
				t.Fatal("two C=1 local owners exceeded total C+1 socket bound", peak)
			}
			t.Logf("synthetic backend connections=0 peak=%d requests=%q", peak, requests)
			return
		}
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("backend connection retained", count)
		}
	}
}

func event(t *testing.T, arrived <-chan struct{}) {
	t.Helper()
	select {
	case <-arrived:
	case <-time.After(8 * time.Second):
		t.Fatal("boundary event timed out")
	}
}

type processOutput struct {
	mu      sync.Mutex
	data    bytes.Buffer
	pending string
	lines   chan string
}

func (o *processOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.data.Len()+len(p) > 64<<10 {
		return 0, errors.New("child output limit")
	}
	o.data.Write(p)
	o.pending += string(p)
	for {
		line, rest, ok := strings.Cut(o.pending, "\n")
		if !ok {
			break
		}
		o.pending = rest
		select {
		case o.lines <- line:
		default:
			return 0, errors.New("child line limit")
		}
	}
	return len(p), nil
}

func (o *processOutput) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.data.String()
}

type cliProcess struct {
	cmd            *exec.Cmd
	stdout, stderr *processOutput
	done           chan struct{}
	waitErr        error
	started, sent  time.Time
	trigger        string
	input          io.WriteCloser
}

func startCLI(t *testing.T, cfg app.Config, mode string) *cliProcess {
	t.Helper()
	t.Logf("native test runtime=%s/%s go=%s euid=%d", runtime.GOOS, runtime.GOARCH, runtime.Version(), os.Geteuid())
	raw, err := yaml.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	config := filepath.Join(t.TempDir(), "weir.yaml")
	if err := os.WriteFile(config, raw, 0600); err != nil {
		t.Fatal(err)
	}

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	args := []string{"-test.run=^TestCLIProcessChild$"}

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.WaitDelay = time.Second
	cmd.Env = append(
		os.Environ(),
		"WEIR_PROCESS_CHILD="+mode,
		"WEIR_PROCESS_CONFIG="+config,
	)
	input, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close() })

	stdout := &processOutput{lines: make(chan string, 16)}
	stderr := &processOutput{lines: make(chan string, 16)}
	p := &cliProcess{
		cmd:     cmd,
		stdout:  stdout,
		stderr:  stderr,
		done:    make(chan struct{}),
		started: time.Now(),
		input:   input,
	}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}

	go func() {
		p.waitErr = cmd.Wait()
		close(p.done)
	}()

	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			// Only the exact child started above may be killed, and always Wait.
			_ = cmd.Process.Kill()
			<-p.done
			t.Errorf("child PID=%d required emergency cleanup", cmd.Process.Pid)
		}
		afterSignal := time.Duration(0)
		if !p.sent.IsZero() {
			afterSignal = time.Since(p.sent)
		}
		t.Logf(
			"child PID=%d binary=%q mode=%s trigger=%q SIGTERM=%v exit=%d waited=true elapsed=%s after_signal=%s stdout=%q stderr=%q wait=%v",
			cmd.Process.Pid,
			binary,
			mode,
			p.trigger,
			!p.sent.IsZero(),
			cmd.ProcessState.ExitCode(),
			time.Since(p.started),
			afterSignal,
			stdout.text(),
			stderr.text(),
			p.waitErr,
		)
	})
	return p
}

func (p *cliProcess) line(t *testing.T) string {
	t.Helper()
	select {
	case line := <-p.stdout.lines:
		return line
	case <-p.done:
		// Wait joins the output writer; a completed child can still have a
		// buffered boundary line that must precede its exit notification.
		select {
		case line := <-p.stdout.lines:
			return line
		default:
		}
		t.Fatal("child exited before boundary", p.waitErr, p.stdout.text(), p.stderr.text())
	case <-time.After(8 * time.Second):
		t.Fatal("child output boundary timed out")
	}
	return ""
}

func (p *cliProcess) terminate(t *testing.T, trigger string) {
	t.Helper()
	p.trigger, p.sent = trigger, time.Now()
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
}

func (p *cliProcess) wait(t *testing.T, exit int) {
	t.Helper()
	select {
	case <-p.done:
	case <-time.After(8 * time.Second):
		t.Fatal("child exceeded bounded exit")
	}
	if p.cmd.ProcessState.ExitCode() != exit {
		t.Fatalf("exit=%d want=%d: %v %s", p.cmd.ProcessState.ExitCode(), exit, p.waitErr, p.stderr.text())
	}
	if !p.sent.IsZero() && time.Since(p.sent) > 8*time.Second {
		t.Fatal("signal exit exceeded outer bound")
	}
}

func reserveReleasedAddresses(t *testing.T, addresses []string) {
	t.Helper()
	// Rebind during shutdown and keep the reservations through Wait. Race
	// instrumentation delays process exit, allowing other test packages to
	// acquire an ephemeral port if it is checked only after the child exits.
	deadline := time.Now().Add(8 * time.Second)
	for _, address := range addresses {
		for {
			listener, err := net.Listen("tcp", address)
			if err == nil {
				t.Cleanup(func() { _ = listener.Close() })
				break
			}
			if !errors.Is(err, syscall.EADDRINUSE) || time.Now().After(deadline) {
				t.Fatal("owned listener not released within shutdown bound", address, err)
			}
			time.Sleep(time.Millisecond)
		}
	}
	t.Logf("listeners released, rebound and reserved through Wait: %v", addresses)
}

// healthy requires a 200 response from one diagnostics health endpoint.
func healthy(t *testing.T, diagnostic, path string) {
	t.Helper()
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	response, err := client.Get("http://" + diagnostic + path)
	if err != nil {
		t.Fatal(path, err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatal(path, response.StatusCode)
	}
}

func listenerAddresses(t *testing.T, line string) []string {
	t.Helper()
	address, found := strings.CutPrefix(line, "Weir listening on ")
	if !found || address == "" || strings.ContainsAny(address, " []") {
		t.Fatal("invalid listener line", line)
	}
	addresses := []string{address}
	return addresses
}

func TestCLISignalDuringHandshake(t *testing.T) {
	for _, partial := range []bool{false, true} {
		t.Run(fmt.Sprint("partial=", partial), func(t *testing.T) {
			hold := "/"
			if partial {
				hold = ""
			}
			b := newStartupBackend(t, hold)
			held := b
			cfg := b.config()
			if partial {
				held = newStartupBackend(t, "/")
				backend := &app.Search{
					URL: held.server.URL,
				}
				second := app.StoreConfig{Name: "second", Search: backend}

				cfg.Stores = append(cfg.Stores, second)
			}
			p := startCLI(t, cfg, "cli")
			event(t, held.entered)
			p.terminate(t, "backend received /; response withheld")
			event(t, held.canceled)
			p.wait(t, 1)
			expected := "local Store \"records\" startup qualification failed: search product identification failed\n"
			if partial {
				expected = "local Store \"second\" startup qualification failed: search product identification failed\n"
			}
			if p.stdout.text() != "" ||
				!strings.HasSuffix(p.stderr.text(), expected) ||
				strings.Contains(p.stderr.text(), "sentinel") ||
				strings.Contains(p.stderr.text(), b.server.URL) ||
				strings.Contains(p.stderr.text(), held.server.URL) {
				t.Fatal("canceled startup announced ready or lost safe error", p.stdout.text(), p.stderr.text())
			}
			b.idle(t)
			if held != b {
				held.idle(t)
			}
		})
	}
}

func TestCLISignalAtFirstListener(t *testing.T) {
	b := newStartupBackend(t, "")
	p := startCLI(t, b.config(), "cli")
	line := p.line(t)
	// The first complete observable listener line is the barrier. No readiness
	// probe, second output line, or scheduling delay precedes the signal.
	p.terminate(t, "first listener output")
	addresses := listenerAddresses(t, line)
	diagnostic := strings.TrimPrefix(p.line(t), "Diagnostics listening on ")
	reserveReleasedAddresses(t, append(addresses, diagnostic))
	p.wait(t, 0)
	if !strings.Contains(p.stdout.text(), "Diagnostics listening on ") || !strings.Contains(p.stderr.text(), "owned=0") {
		t.Fatal("output contract", p.stdout.text(), p.stderr.text())
	}
	b.idle(t)
}

func TestCLIStorelessServe(t *testing.T) {
	cfg := app.Config{}
	cfg.Listeners.Application = "127.0.0.1:0"
	cfg.Listeners.Diagnostics = "127.0.0.1:0"
	p := startCLI(t, cfg, "cli")
	addresses := listenerAddresses(t, p.line(t))
	diagnostic := strings.TrimPrefix(p.line(t), "Diagnostics listening on ")
	for _, path := range []string{"/livez", "/readyz"} {
		healthy(t, diagnostic, path)
	}

	p.terminate(t, "Store-less node reached readiness")
	reserveReleasedAddresses(t, append(addresses, diagnostic))
	p.wait(t, 0)
	if p.stderr.text() != "" {
		t.Fatal("Store-less shutdown output", p.stderr.text())
	}
}

func TestCLISignalBeforeStart(t *testing.T) {
	b := newStartupBackend(t, "")
	p := startCLI(t, b.config(), "before-start")
	line := p.line(t)
	if !strings.HasPrefix(line, "opened ") {
		t.Fatal(line)
	}
	p.terminate(t, "Open returned with the application listener, diagnostics and local runtime; Start not called")
	reserveReleasedAddresses(t, strings.Fields(strings.TrimPrefix(line, "opened ")))
	p.wait(t, 1)
	if !strings.HasSuffix(p.stderr.text(), "context canceled\n") || strings.Contains(p.stdout.text(), "listening") {
		t.Fatal("canceled Start classified incorrectly", p.stderr.text())
	}
	b.idle(t)
}

// TestCLIProcessChild reexecutes the real run/Cobra path by default. The
// before-start mode exercises the app API at its caller-owned boundary. The
// writer-error mode gives the shared production serving executor one explicit
// signal context so its blocked writer can acknowledge actual cancellation.
func TestCLIProcessChild(t *testing.T) {
	mode := os.Getenv("WEIR_PROCESS_CHILD")
	if mode == "" {
		return
	}
	config := os.Getenv("WEIR_PROCESS_CONFIG")

	var err error
	if mode == "before-start" {
		err = beforeStartChild(config)
	} else if mode == "writer-error" {
		err = writerErrorChild(config)
	} else {
		err = run([]string{"serve", "--config", config}, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	os.Exit(0)
}

func beforeStartChild(config string) error {
	cfg, err := app.Load(config)
	if err != nil {
		return err
	}

	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
	defer stop()

	startup, cancel := context.WithTimeout(signals, 5*time.Second)
	defer cancel()

	node, err := app.Open(startup, cfg)
	if err != nil {
		return err
	}
	fmt.Fprintln(os.Stdout, "opened", node.Address(), node.DiagnosticAddress())
	<-signals.Done()
	err = node.Start(startup)
	drain, stopDrain := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopDrain()
	err = errors.Join(err, node.Close(drain), node.Close(drain))
	return err
}

func TestCLIStartupFailure(t *testing.T) {
	b := newStartupBackend(t, "invalid-product")
	cfg := b.config()
	p := startCLI(t, cfg, "cli")
	p.wait(t, 1)
	if p.stdout.text() != "" ||
		!strings.HasSuffix(p.stderr.text(), "local Store \"records\" startup qualification failed: search product identification failed\n") ||
		strings.Contains(p.stderr.text(), "sentinel") ||
		strings.Contains(p.stderr.text(), b.server.URL) {
		t.Fatal("startup error leaked or announced readiness", p.stdout.text(), p.stderr.text())
	}
	b.idle(t)
}

const writerSignalAcknowledged = "serving signal cancellation observed"

func writerErrorChild(config string) error {
	cfg, err := app.Load(config)
	if err != nil {
		return err
	}
	signals, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	writer := &failedOutput{ctx: signals, output: os.Stdout, release: os.Stdin}
	return serveContext(signals, cfg, writer)
}

type failedOutput struct {
	ctx     context.Context
	output  io.Writer
	release io.Reader
}

func (w *failedOutput) Write(p []byte) (int, error) {
	if _, err := w.output.Write(p); err != nil {
		return 0, err
	}
	select {
	case <-w.ctx.Done():
	case <-time.After(3 * time.Second):
		return 0, errors.New("signal cancellation acknowledgment timed out")
	}
	if _, err := fmt.Fprintln(w.output, writerSignalAcknowledged); err != nil {
		return 0, err
	}
	var release [1]byte
	if _, err := io.ReadFull(w.release, release[:]); err != nil {
		return 0, err
	}
	return 0, errors.New("writer-error-sentinel")
}

type failedOutputContext struct {
	context.Context
	waiting chan struct{}
}

func (c *failedOutputContext) Done() <-chan struct{} {
	close(c.waiting)
	return c.Context.Done()
}

type failedOutputRelease struct {
	entered chan struct{}
	release chan struct{}
}

func (r *failedOutputRelease) Read(p []byte) (int, error) {
	close(r.entered)
	<-r.release
	return copy(p, "x"), nil
}

func TestFailedOutputRequiresCancellationAndIndependentRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	signalContext := &failedOutputContext{Context: ctx, waiting: make(chan struct{})}
	output := &processOutput{lines: make(chan string, 2)}
	input := &failedOutputRelease{entered: make(chan struct{}), release: make(chan struct{})}
	writer := &failedOutput{ctx: signalContext, output: output, release: input}
	release := sync.OnceFunc(func() { close(input.release) })
	done := make(chan struct{})
	var writeErr error
	go func() {
		_, writeErr = writer.Write([]byte("listener announcement\n"))
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		release()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Error("failed writer control did not join")
		}
	})
	select {
	case line := <-output.lines:
		if line != "listener announcement" {
			t.Fatal("unexpected readiness cue", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("listener announcement missing")
	}
	select {
	case <-signalContext.waiting:
	case <-time.After(3 * time.Second):
		t.Fatal("writer did not wait on the actual cancellation context")
	}
	// Readiness, or a caller returning from Signal without this context having
	// observed it, must not acknowledge handling or read the error release.
	select {
	case line := <-output.lines:
		t.Fatal("acknowledged before cancellation", line)
	case <-input.entered:
		t.Fatal("read error release before cancellation")
	case <-done:
		t.Fatal("writer returned before cancellation", writeErr)
	default:
	}
	cancel()
	select {
	case line := <-output.lines:
		if line != writerSignalAcknowledged {
			t.Fatal("actual cancellation acknowledgment missing", line)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation was not acknowledged")
	}
	event(t, input.entered)
	select {
	case <-done:
		t.Fatal("cancellation swallowed independent writer failure", writeErr)
	default:
	}
	release()
	event(t, done)
	if writeErr == nil || writeErr.Error() != "writer-error-sentinel" {
		t.Fatal("independent output error was lost after cancellation", writeErr)
	}
}

func TestCLISignalWithOutputFailure(t *testing.T) {
	b := newStartupBackend(t, "")
	p := startCLI(t, b.config(), "writer-error")
	line := p.line(t)
	p.terminate(t, "shared serving writer blocked in first Write; awaiting actual SIGTERM cancellation")
	// Signal returning does not prove child handling. Keep the independent
	// writer failure blocked until this exact serving context acknowledges it.
	select {
	case acknowledgment := <-p.stdout.lines:
		if acknowledgment != writerSignalAcknowledged {
			t.Fatal("unexpected signal acknowledgment", acknowledgment)
		}
	case <-p.done:
		t.Fatal("child exited before signal acknowledgment", p.waitErr)
	case <-time.After(3 * time.Second):
		t.Fatal("actual signal cancellation was not acknowledged")
	}
	if _, err := io.WriteString(p.input, "x"); err != nil {
		t.Fatal(err)
	}
	reserveReleasedAddresses(t, listenerAddresses(t, line))
	p.wait(t, 1)
	if !strings.HasSuffix(p.stderr.text(), "listener output failed\n") ||
		strings.Contains(p.stderr.text(), "sentinel") ||
		strings.Contains(p.stdout.text(), "Diagnostics listening") {
		t.Fatal("signal swallowed independent output error or writer bypassed", p.stdout.text(), p.stderr.text())
	}
	b.idle(t)
}

func TestCLISignalDrainDeadline(t *testing.T) {
	b := newStartupBackend(t, "")
	p := startCLI(t, b.config(), "cli")
	addresses := listenerAddresses(t, p.line(t))
	diagnostic := strings.TrimPrefix(p.line(t), "Diagnostics listening on ")
	conn, err := grpc.NewClient(
		"passthrough:///"+addresses[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableRetry(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	desc := &grpc.StreamDesc{ClientStreams: true, ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, pb.StoreService_Execute_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	until := time.Now().Add(time.Second)
	for testmetrics.Sum(testmetrics.Scrape(t, diagnostic), "weir_ingress_sessions") != 1 {
		if time.Now().After(until) {
			t.Fatal("input stream was not admitted")
		}
	}
	p.terminate(t, "metrics confirmed real input slot; no request body; default stall=30s")
	reserveReleasedAddresses(t, append(addresses, diagnostic))
	p.wait(t, 0)
	var result pb.ReadResult
	if err := stream.RecvMsg(&result); err == nil {
		t.Fatal("incomplete request unexpectedly completed")
	}
	if time.Since(p.sent) < 5*time.Second {
		t.Fatal("independent five-second drain was prematurely canceled")
	}
	b.idle(t)
	t.Log("forced bounded drain; incomplete input, no admitted mutation, UNKNOWN mutations=0")
}

func TestStartupCancellationReleasesOwners(t *testing.T) {
	b := newStartupBackend(t, "")
	held := newStartupBackend(t, "/")
	cfg := b.config()
	backend := &app.Search{
		URL: held.server.URL,
	}
	service := app.StoreConfig{Name: "second", Search: backend}

	cfg.Stores = append(cfg.Stores, service)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	var node *app.Node
	var err error
	go func() {
		node, err = app.Open(ctx, cfg)
		close(done)
	}()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
			if node != nil {
				_ = node.Close(context.Background())
			}
		case <-time.After(2 * time.Second):
			t.Error("Open goroutine did not stop")
		}
	})
	event(t, held.entered)
	started := time.Now()
	cancel()
	event(t, done)
	if node != nil ||
		err == nil ||
		err.Error() != "local Store \"second\" startup qualification failed: search product identification failed" ||
		time.Since(started) > 2*time.Second {
		t.Fatal("partial Open cancellation", node, err, time.Since(started))
	}
	event(t, held.canceled)
	b.idle(t)
	held.idle(t)
	t.Log("partial Open releases first Runtime/Adapter and canceled second Adapter without process exit")
}

func TestCLISignalDrainsInflight(t *testing.T) {
	b := newStartupBackend(t, "/_bulk")
	p := startCLI(t, b.config(), "cli")
	addresses := listenerAddresses(t, p.line(t))
	diagnostic := strings.TrimPrefix(p.line(t), "Diagnostics listening on ")
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	healthy(t, diagnostic, "/readyz")
	conn, err := grpc.NewClient(
		"passthrough:///"+addresses[0],
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithNoProxy(),
		grpc.WithDisableRetry(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	client := pb.NewStoreServiceClient(conn)
	document := &pb.Document{ContentType: "application/json", Data: []byte(`{"n":1}`)}
	put := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: "records/s:one", Action: put}
	done := make(chan struct{})
	var result *pb.MutationResult
	var callErr error
	go func() {
		var recordResult *pb.Event
		recordResult, callErr = testutil.ExecuteRecord(ctx, client, testutil.RecordRequest("records", request))
		if recordResult != nil {
			result = recordResult.GetMutationResult()
		}
		close(done)
	}()

	event(t, b.entered)
	p.terminate(t, "backend received full mutation; reply withheld")
	// Observe state transitions through real HTTP, never sleep to assume a state.
	transport := &http.Transport{Proxy: nil, DisableKeepAlives: true}
	defer transport.CloseIdleConnections()
	health := &http.Client{Transport: transport, Timeout: time.Second}
	until := time.Now().Add(time.Second)
	for {
		response, err := health.Get("http://" + diagnostic + "/readyz")
		if err != nil {
			t.Fatal("diagnostics vanished before data drain", err)
		}
		response.Body.Close()
		if response.StatusCode == http.StatusServiceUnavailable {
			break
		}
		if time.Now().After(until) {
			t.Fatal("readiness did not enter draining")
		}
	}
	healthy(t, diagnostic, "/livez")
	select {
	case <-b.canceled:
		t.Fatal("signal canceled the backend before its independent deadline")
	default:
	}
	event(t, done)
	if callErr == nil && result.GetOutcome() != pb.MutationOutcome_UNKNOWN ||
		callErr != nil && status.Code(callErr) != codes.Unavailable && status.Code(callErr) != codes.DeadlineExceeded && status.Code(callErr) != codes.Canceled {
		t.Fatal("forced shutdown must preserve uncertainty", result, callErr)
	}
	event(t, b.canceled)
	reserveReleasedAddresses(t, append(addresses, diagnostic))
	p.wait(t, 0)
	b.idle(t)
	b.mu.Lock()
	mutations := 0
	for _, request := range b.requests {
		if request == "POST /_bulk" {
			mutations++
		}
	}
	b.mu.Unlock()
	if mutations != 1 {
		t.Fatal("mutation replay", mutations)
	}
	t.Log("ready -> draining (readyz=503, livez=200) -> closed; UNKNOWN=1, backend sends=1, replay=0")
}
