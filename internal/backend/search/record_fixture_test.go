package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testrecords"
)

func prepareTestRecord(adapter *Adapter, operation *pb.ExecuteRequest) (*execution.Plan, *pb.Failure) {
	work, failure := testrecords.Prepare(context.Background(), adapter, adapter.config.Store, operation)
	if failure == nil {
		failure = adapter.validateRecord(context.Background(), work)
	}
	if failure != nil {
		return nil, failure
	}
	return work, nil
}

func testHTTPAdapter(t *testing.T, handler http.HandlerFunc) *Adapter {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := newTransport(time.Second, backend.DefaultOptions().IdleTimeout)
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport, CheckRedirect: noRedirect}
	config := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{dialect: ElasticsearchProduct, config: config, ctx: context.Background(), client: client}
	return adapter
}
