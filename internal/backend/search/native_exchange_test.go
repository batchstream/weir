package search

import (
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
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

func requireExchangeFailure(t *testing.T, adapter *Adapter, want string) {
	t.Helper()
	request := nativeRequest(t, "records", "GET", "/_search")
	end, capture := runNative(t, adapter, request, nil)
	if end.Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE || capture.head != nil {
		t.Fatal("failed exchange published output", end)
	}
	failure := end.GetFailure()
	if failure.GetCode() != pb.FailureCode_UNAVAILABLE || failure.GetMessage() != want {
		t.Fatalf("failure %s %q, want UNAVAILABLE %q", failure.GetCode(), failure.GetMessage(), want)
	}
	if strings.Contains(failure.GetMessage(), "127.0.0.1") {
		t.Fatal("failure quotes the backend address", failure.GetMessage())
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
	requireExchangeFailure(t, adapter, "Native HTTP exchange incomplete while connecting (refused)")
}

func TestNativeExchangeFailureNamesATLSHandshakeTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	// Accept and never answer, so the client's TLS handshake times out.
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { conn.Close() })
		}
	}()
	adapter := nativeAdapter(t, "https://"+listener.Addr().String(), 200*time.Millisecond)
	requireExchangeFailure(t, adapter, "Native HTTP exchange incomplete during the TLS handshake (timeout)")
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
	requireExchangeFailure(t, adapter, "Native HTTP exchange incomplete while waiting for the response head (closed)")
}
