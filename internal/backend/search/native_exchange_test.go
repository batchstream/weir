package search

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"

	"github.com/batchstream/weir/internal/testutil"
)

// nativeAdapter forwards Natives to url with the production Native transport
// settings and the given connect timeout.
func nativeAdapter(t *testing.T, url string, timeout time.Duration) *Adapter {
	t.Helper()
	transport := newTransport(timeout, time.Second)
	transport.DisableKeepAlives = true
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{URL: url}
	adapter := &Adapter{config: config, nativeClient: client, ctx: t.Context()}
	return adapter
}

func requireExchangeFailure(t *testing.T, adapter *Adapter, completion pb.NativeCompletion, want string) {
	t.Helper()
	request := nativeRequest(t, "records", "GET", "/_search")
	end, capture := runNative(t, adapter, request, nil)
	if end.Completion != completion || capture.head != nil {
		t.Fatalf("completion %s, want %s without output", end.Completion, completion)
	}
	failure := end.GetFailure()
	if failure.GetCode() != pb.FailureCode_UNAVAILABLE || failure.GetMessage() != want {
		t.Fatalf("failure %s %q, want UNAVAILABLE %q", failure.GetCode(), failure.GetMessage(), want)
	}
}

func TestNativeExchangeFailureNamesARefusedConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	adapter := nativeAdapter(t, "http://"+address, time.Second)
	requireExchangeFailure(t, adapter, pb.NativeCompletion_NATIVE_NOT_STARTED, "Native HTTP exchange not started while connecting (refused)")
}

// silentListener accepts connections and never answers, so a client's TLS
// handshake stalls, and returns its address.
func silentListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		var held []net.Conn
		for {
			conn, err := listener.Accept()
			if err != nil {
				for _, conn := range held {
					conn.Close()
				}
				return
			}
			held = append(held, conn)
		}
	}()
	return listener.Addr().String()
}

func TestNativeExchangeFailureNamesATLSHandshakeTimeout(t *testing.T) {
	adapter := nativeAdapter(t, "https://"+silentListener(t), 200*time.Millisecond)
	requireExchangeFailure(t, adapter, pb.NativeCompletion_NATIVE_NOT_STARTED, "Native HTTP exchange not started during the TLS handshake (timeout)")
}

func TestNativeExchangeFailureNamesAnUntrustedCertificate(t *testing.T) {
	backend := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(backend.Close)
	adapter := nativeAdapter(t, backend.URL, time.Second)
	requireExchangeFailure(t, adapter, pb.NativeCompletion_NATIVE_NOT_STARTED, "Native HTTP exchange not started during the TLS handshake")
}

func TestNativeExchangeFailureNamesAnUnresolvableBackend(t *testing.T) {
	adapter := nativeAdapter(t, "http://weir-native-test.invalid", time.Second)
	request := nativeRequest(t, "records", "GET", "/_search")
	end, capture := runNative(t, adapter, request, nil)
	failure := end.GetFailure()
	want := "Native HTTP exchange not started while connecting"
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || capture.head != nil ||
		failure.GetCode() != pb.FailureCode_UNAVAILABLE || !strings.HasPrefix(failure.GetMessage(), want) {
		t.Fatalf("completion %s failure %s %q, want NATIVE_NOT_STARTED UNAVAILABLE %q", end.Completion, failure.GetCode(), failure.GetMessage(), want)
	}
}

// A deadline before the exchange had a connection also ends it not started,
// with the deadline as its failure, since the backend never saw it.
func TestNativeExchangeDeadlineBeforeAConnectionEndsNotStarted(t *testing.T) {
	adapter := nativeAdapter(t, "https://"+silentListener(t), 2*time.Second)
	request := nativeRequest(t, "records", "GET", "/_search")
	setNativeBody(t, request, nil)
	capture := &nativeCapture{}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	end := adapter.ExecuteNative(ctx, testutil.NativeCommand(request), capture.Emit)
	if end.Completion != pb.NativeCompletion_NATIVE_NOT_STARTED || end.GetFailure().GetCode() != pb.FailureCode_DEADLINE_EXCEEDED || capture.head != nil {
		t.Fatalf("completion %s failure %s, want NATIVE_NOT_STARTED DEADLINE_EXCEEDED", end.Completion, end.GetFailure().GetCode())
	}
}

// A handshake reported after the exchange got its connection, as from a dial
// net/http abandoned, must not make a sent request look unsent.
func TestExchangePhaseNeverMovesBack(t *testing.T) {
	phase := &exchangePhase{}
	trace := phase.trace()
	connection := httptrace.GotConnInfo{}
	trace.GotConn(connection)
	written := httptrace.WroteRequestInfo{}
	trace.WroteRequest(written)
	trace.TLSHandshakeStart()
	if reached := phase.reached.Load(); reached != phaseWaiting {
		t.Fatalf("phase %s after a late handshake, want %s", exchangePhases[reached], exchangePhases[phaseWaiting])
	}
}

func TestNativeExchangeFailureNamesAConnectionClosedBeforeTheResponse(t *testing.T) {
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		conn.Close()
	})
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	adapter := nativeAdapter(t, backend.URL, time.Second)
	requireExchangeFailure(t, adapter, pb.NativeCompletion_RESPONSE_INCOMPLETE, "Native HTTP exchange incomplete while waiting for the response head (closed)")
}

func TestNativeExchangeFailureNamesARequestCutOffWhileSending(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	// Read the start of the request, then drop the connection mid-upload.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			buffer := make([]byte, 4096)
			_, _ = io.ReadFull(conn, buffer)
			conn.Close()
		}
	}()
	adapter := nativeAdapter(t, "http://"+listener.Addr().String(), time.Second)
	request := nativeRequest(t, "records", "POST", "/_search")
	body := bytes.Repeat([]byte("x"), 32<<20)
	end, capture := runNative(t, adapter, request, body)
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || capture.head != nil {
		t.Fatal("failed exchange published output", end)
	}
	failure := end.GetFailure()
	want := "Native HTTP exchange incomplete while sending the request"
	if failure.GetCode() != pb.FailureCode_UNAVAILABLE || !strings.HasPrefix(failure.GetMessage(), want) {
		t.Fatalf("failure %s %q, want UNAVAILABLE %q", failure.GetCode(), failure.GetMessage(), want)
	}
}
