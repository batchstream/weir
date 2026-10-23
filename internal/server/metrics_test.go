package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestNativeCompletionMetricsAreIndependentOfRPCAndHTTPStatus(t *testing.T) {
	cases := []struct {
		name       string
		truncated  bool
		completion pb.NativeCompletion
		label      string
	}{
		{name: "truncated_http_200", truncated: true, completion: pb.NativeCompletion_RESPONSE_INCOMPLETE, label: "response_incomplete"},
		{name: "complete_http_500", completion: pb.NativeCompletion_RESPONSE_COMPLETE, label: "response_complete"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/":
					_, _ = io.WriteString(w, `{"version":{"build_flavor":"default"}}`)
				case "/_native":
					w.Header().Set("Content-Type", "application/octet-stream")
					if test.truncated {
						w.Header().Set("Content-Length", "32")
					} else {
						w.WriteHeader(http.StatusInternalServerError)
					}
					_, _ = io.WriteString(w, "opaque body")
				default:
					t.Error("unexpected backend request", r.URL.Path)
					http.Error(w, "unexpected request", http.StatusNotFound)
				}
			})
			backend := httptest.NewServer(handler)
			t.Cleanup(backend.Close)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			config := search.Config{Store: "records", URL: backend.URL}
			adapter, err := search.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			local, err := store.New(adapter, store.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				ctx, stop := context.WithTimeout(context.Background(), time.Second)
				defer stop()
				if err := local.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
			server, address := startTestServer(t, opts)
			_, client := testClient(t, address)
			httpRequest, err := http.NewRequest(http.MethodGet, "http://ignored.invalid/_native", nil)
			if err != nil {
				t.Fatal(err)
			}
			native, err := weirclient.NewHTTPNativeRequest("", httpRequest)
			if err != nil {
				t.Fatal(err)
			}
			operation := &pb.Command_Native{Native: native}
			command := &pb.Command{Operation: operation}
			request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
			stream, err := client.Execute(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			var head *pb.NativeHead
			var end *pb.NativeEnd
			var body []byte
			for {
				response, err := stream.Recv()
				if err == io.EOF {
					break // io.EOF confirms the final gRPC status is OK.
				}
				if err != nil {
					t.Fatal("Native forwarding unexpectedly failed the RPC", err)
				}
				if response.Index != 1 || end != nil {
					t.Fatal("unexpected Native event after terminal or wrong index", response)
				}
				event := response.Event
				if event.GetHead() != nil {
					head = event.GetHead()
				}
				body = append(body, event.GetChunk()...)
				end = event.GetNativeEnd()
			}
			if head == nil || string(body) != "opaque body" || end == nil || end.Completion != test.completion || (end.Failure != nil) != test.truncated {
				t.Fatal("Native response evidence changed", head, string(body), end)
			}
			if !test.truncated && !bytes.HasPrefix(head.Metadata.GetData(), []byte("HTTP/1.1 500 ")) {
				t.Fatal("opaque HTTP 500 status was lost", head)
			}
			waitIdle(t, server)
			for range 3 {
				families := testmetrics.Gather(t, local)
				family := families["weir_store_native_completions_total"]
				if len(family.GetMetric()) != 3 {
					t.Fatal("Native completion added dynamic series", family)
				}
				labels := map[string]string{"completion": test.label}
				if testmetrics.Sample(families, "weir_store_native_completions_total", labels).GetCounter().GetValue() != 1 || testmetrics.Sum(families, "weir_store_native_completions_total") != 1 {
					t.Fatal("Native terminal completion was not counted exactly once", family)
				}
				if testmetrics.Sum(families, "weir_store_records_total") != 0 || testmetrics.Sum(families, "weir_store_record_failures_total") != 0 {
					t.Fatal("opaque Native response polluted typed business metrics")
				}
				labels = map[string]string{"method": "execute", "status": "ok"}
				rpc := testmetrics.Gather(t, server)
				if testmetrics.Sample(rpc, "weir_rpc_completions_total", labels).GetCounter().GetValue() != 1 {
					t.Fatal("Native terminal evidence was confused with RPC status")
				}
			}
		})
	}
}

func TestRPCMetricsReportInvalidStream(t *testing.T) {
	_, local := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := readFrame(1, testRequest())
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	invalid := readFrame(3, testRequest())
	if err := stream.Send(invalid); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("invalid ordinal did not terminate the native RPC", err)
	}
	waitIdle(t, server)
	registry := prometheus.NewPedanticRegistry()
	if err := registry.Register(server); err != nil {
		t.Fatal(err)
	}
	metrics := testmetrics.Registry(t, registry)
	labels := map[string]string{"method": "execute", "status": "non_ok"}
	if testmetrics.Sample(metrics, "weir_rpc_completions_total", labels).GetCounter().GetValue() != 1 {
		t.Fatal("native RPC failure missing from completion status metrics")
	}
	if metrics["weir_watchdog_expirations_total"] == nil {
		t.Fatal("collector lost the watchdog metric")
	}
}
