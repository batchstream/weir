package search

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
)

func TestConfiguredPITRetentionReachesOpenAndFetch(t *testing.T) {
	for _, dialect := range []string{ElasticsearchProduct, OpenSearchProduct} {
		t.Run(dialect, func(t *testing.T) {
			var opens, fetches atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/records":
					_, _ = io.WriteString(w, testIndexReply)
				case "/records/_pit", "/records/_search/point_in_time":
					opens.Add(1)
					if r.URL.Query().Get("keep_alive") != "17500ms" {
						t.Error("PIT open ignored configured retention", r.URL.Query())
					}
					reply := `{"id":"initial","_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`
					if dialect == OpenSearchProduct {
						reply = `{"pit_id":"initial","creation_time":1,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0}}`
					}
					_, _ = io.WriteString(w, reply)
				case "/_search":
					fetches.Add(1)
					var request struct {
						PIT struct {
							KeepAlive string `json:"keep_alive"`
						}
					}
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.PIT.KeepAlive != "17500ms" {
						t.Error("PIT fetch ignored configured retention", request, err)
					}
					_, _ = w.Write(scanBatchReply(nil))
				default:
					t.Error("unexpected scan request", r.URL.Path)
				}
			})
			adapter := scanBatchAdapter(t, handler)
			settings := backend.DefaultOptions()
			settings.PITKeepAlive = 17500 * time.Millisecond
			adapter.config.Options = &settings
			adapter.dialect = dialect
			request := &pb.ScanRequest{Resource: "records", PageSize: 1}
			work, failure := adapter.prepareScan(t.Context(), request)
			if failure != nil {
				t.Fatal(failure)
			}
			page := adapter.fetchScan(t.Context(), work)
			if page.Failure != nil || !page.Exhausted || opens.Load() != 1 || fetches.Load() != 1 {
				t.Fatal("configured PIT flow failed", page, opens.Load(), fetches.Load())
			}
		})
	}
}

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
