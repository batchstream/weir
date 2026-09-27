package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestProbeCLI(t *testing.T) {
	t.Setenv("HTTP_PROXY", "http://127.0.0.1:1")
	t.Setenv("NO_PROXY", "")
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" || r.Header.Get("Accept-Encoding") != "" {
			t.Error("unexpected request")
		}
		switch r.URL.Path {
		case "/livez":
			io.WriteString(w, "ok\n")
		case "/readyz":
			io.WriteString(w, "ready\n")
		default:
			t.Error("unexpected path")
		}
	}))
	defer listener.Close()
	address := strings.TrimPrefix(listener.URL, "http://")
	for _, mode := range []string{"live", "ready"} {
		var output bytes.Buffer
		if err := run([]string{"-probe", mode, "-probe-address", address}, &output); err != nil {
			t.Fatal(err)
		}
		if output.Len() != 0 {
			t.Fatal("healthy probe should be quiet")
		}
	}
	cases := [][]string{
		{"-probe", ""}, {"-probe", "metrics"}, {"-probe-address", address},
		{"-probe", "live", "extra"}, {"-probe", "live", "-version"},
		{"-probe", "live", "-version=false"}, {"-probe", "live", "-config", "missing"},
		{"-probe", "ready", "-mongo-uri", "must-not-connect"}, {"-probe", "live", "-diagnostics", address},
		{"-probe", "live", "-memory-mib", "64"},
	}
	for _, args := range cases {
		var output bytes.Buffer
		if err := run(args, &output); err == nil || strings.Contains(err.Error(), "configuration unavailable") {
			t.Fatalf("arguments must fail before config access: %v %v", args, err)
		}
	}
}

func TestProbeRejectsTargets(t *testing.T) {
	for _, address := range []string{"", "localhost:7449", "0.0.0.0:7449", "192.0.2.1:80", "[::]:80", "127.0.0.1:0", "127.0.0.1:65536", "[::1%lo0]:80", "http://127.0.0.1:80", "127.0.0.1:80/metrics"} {
		if err := runProbe(context.Background(), "live", address); err == nil {
			t.Fatal("accepted", address)
		}
	}
}

func TestProbeBoundedResponses(t *testing.T) {
	cases := []struct {
		name, response string
		healthy        bool
	}{
		{"ok", "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nok\n", true},
		{"wrong-status", "HTTP/1.1 503 Unavailable\r\nContent-Length: 3\r\n\r\nok\n", false},
		{"redirect", "HTTP/1.1 302 Found\r\nLocation: http://192.0.2.1/\r\nContent-Length: 0\r\n\r\n", false},
		{"oversize", "HTTP/1.1 200 OK\r\nContent-Length: 999999\r\n\r\n" + strings.Repeat("x", 65), false},
		{"header-limit", "HTTP/1.1 200 OK\r\nX-Large: " + strings.Repeat("x", 2048) + "\r\n\r\n", false},
		{"partial-body", "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\no", false},
		{"partial-header", "HTTP/1.1 200", false},
		{"hang", "", false},
		{"unexpected-body", "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nno\n", false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			done := make(chan struct{})
			go func() {
				defer close(done)
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				defer conn.Close()
				conn.SetDeadline(time.Now().Add(2 * time.Second))
				buf := make([]byte, 1024)
				conn.Read(buf)
				io.WriteString(conn, test.response)
				// Keep the peer open until the probe closes it, including timeout/error paths.
				conn.Read(buf)
			}()
			started := time.Now()
			err = runProbe(context.Background(), "live", listener.Addr().String())
			if (err == nil) != test.healthy {
				t.Fatalf("healthy=%v err=%v", test.healthy, err)
			}
			if time.Since(started) > 1500*time.Millisecond {
				t.Fatal("probe exceeded bound")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("probe retained connection")
			}
		})
	}
}

func TestProbeCancellationAndAbsentListener(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	if err := runProbe(context.Background(), "live", address); err == nil {
		t.Fatal("absent listener healthy")
	}
	arrived := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(arrived); <-r.Context().Done() }))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runProbe(ctx, "live", strings.TrimPrefix(server.URL, "http://")) }()
	<-arrived
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled probe healthy")
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation stuck")
	}
}
