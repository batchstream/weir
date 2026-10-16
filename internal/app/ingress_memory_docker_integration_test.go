//go:build integration

package app

import (
	"context"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func ingressMemoryDockerFixture(t *testing.T, handler http.Handler) string {
	t.Helper()
	// Keep the Unix socket path below macOS's sockaddr_un limit.
	directory, err := os.MkdirTemp("/tmp", "weir-inspect-")
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(directory, "s")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		_ = os.Remove(directory)
		t.Fatal(err)
	}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		_ = server.Close()
		_ = listener.Close()
		_ = os.Remove(directory)
	})
	return "unix://" + socket
}

func TestIngressMemoryDockerInspectMetadata(t *testing.T) {
	const validMetadata = `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":true,"OOMKilled":false,"ExitCode":0}}`
	hiddenTail := validMetadata + strings.Repeat(" ", (2<<20)-len(validMetadata)) + "{}"
	cases := []struct {
		name, payload, want string
		status              int
	}{
		{name: "healthy", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":true,"OOMKilled":false,"ExitCode":0}}`, want: "owned true false 0", status: 200},
		{name: "unhealthy_preserved", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":false,"OOMKilled":true,"ExitCode":137}}`, want: "owned false true 137", status: 200},
		{name: "missing_running", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"OOMKilled":false,"ExitCode":0}}`, status: 200},
		{name: "missing_oom", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":true,"ExitCode":0}}`, status: 200},
		{name: "missing_exit", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":true,"OOMKilled":false}}`, status: 200},
		{name: "missing_owner", payload: `{"State":{"Running":true,"OOMKilled":false,"ExitCode":0}}`, status: 200},
		{name: "malformed", payload: `{`, status: 200},
		{name: "multiple_json_values", payload: `{"Config":{"Labels":{"weir.owner":"owned"}},"State":{"Running":true,"OOMKilled":false,"ExitCode":0}}{}`, status: 200},
		{name: "bounded_whitespace_cannot_hide_tail", payload: hiddenTail, status: 200},
		{name: "http_error", payload: `{}`, status: 404},
		{name: "redirect_not_followed", payload: `{}`, status: 302},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var calls atomic.Int64
			handler := http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				if request.Method != http.MethodGet || request.URL.Path != "/containers/owned/json" {
					t.Error("observer changed inspect method or owned container", request.Method, request.URL.Path)
				}
				writer.Header().Set("Location", "/must-not-follow")
				writer.WriteHeader(test.status)
				_, _ = writer.Write([]byte(test.payload))
			})
			t.Setenv("DOCKER_HOST", ingressMemoryDockerFixture(t, handler))
			ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
			defer cancel()
			actual, err := ingressMemoryDockerInspect(ctx, "owned")
			if actual != test.want || (err == nil) != (test.want != "") || calls.Load() != 1 {
				t.Error("inspect changed known state, accepted unknown data, or retried", actual, err, calls.Load())
			}
		})
	}
}

func TestIngressMemoryDockerInspectRejectsRemoteHost(t *testing.T) {
	for _, host := range []string{"tcp://127.0.0.1:2375", "https://docker.example", "unix://remote/socket", "unix:///tmp/socket?credentials=unused"} {
		t.Setenv("DOCKER_HOST", host)
		state, err := ingressMemoryDockerInspect(t.Context(), "owned")
		if state != "" || err == nil {
			t.Error("nonlocal or ambiguous Docker host was accepted", host)
		}
	}
}

func TestIngressMemoryDockerInspectCancellationDoesNotRetry(t *testing.T) {
	var calls atomic.Int64
	handler := http.HandlerFunc(func(_ http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		<-request.Context().Done()
	})
	t.Setenv("DOCKER_HOST", ingressMemoryDockerFixture(t, handler))
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	state, err := ingressMemoryDockerInspect(ctx, "owned")
	if state != "" || !errors.Is(err, context.DeadlineExceeded) || calls.Load() != 1 {
		t.Error("unknown timed-out observation did not fail with its original deadline or retried", state, err, calls.Load())
	}
}
