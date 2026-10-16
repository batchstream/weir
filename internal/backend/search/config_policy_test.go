package search

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestConfiguredIdleTimeoutControlsConnectionRetention(t *testing.T) {
	for _, idle := range []time.Duration{10 * time.Millisecond, 0} {
		t.Run(idle.String(), func(t *testing.T) {
			var connections atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = io.WriteString(w, "ok")
			})
			server := httptest.NewUnstartedServer(handler)
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.Start()
			defer server.Close()
			transport := newTransport(time.Second, idle)
			defer transport.CloseIdleConnections()
			client := &http.Client{Transport: transport}
			for request := 0; request < 2; request++ {
				response, err := client.Get(server.URL)
				if err != nil {
					t.Fatal(err)
				}
				_, err = io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if err != nil || closeErr != nil {
					t.Fatal(err, closeErr)
				}
				if request == 0 {
					time.Sleep(40 * time.Millisecond)
				}
			}
			want := int32(1)
			if idle != 0 {
				want = 2
			}
			if connections.Load() != want {
				t.Fatal("idle retention policy did not apply", connections.Load(), want)
			}
		})
	}
}
