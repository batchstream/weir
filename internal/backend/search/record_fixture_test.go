package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testrecords"
)

func prepareTestRecord(adapter *Adapter, operation *pb.ExecuteRequest) (*execution.Plan, *pb.Failure) {
	return testrecords.Prepare(context.Background(), adapter, adapter.config.Store, operation)
}

func testHTTPAdapter(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := newTransport(connectionTimeout, backend.DefaultOptions().IdleTimeout)
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{dialect: ElasticsearchProduct, config: config, ctx: context.Background(), client: client}
	return adapter
}
