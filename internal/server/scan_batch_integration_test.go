//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type scanRPCObservation struct {
	mu    sync.Mutex
	pages []int
}

func (o *scanRPCObservation) record(size int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.pages = append(o.pages, size)
}

func (o *scanRPCObservation) snapshot() []int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]int(nil), o.pages...)
}

func startScanRPCNode(t *testing.T, adapter execution.Adapter) *routeAcceptanceNode {
	t.Helper()
	limits := store.DefaultLimits()
	limits.Concurrency = 1
	limits.BackendTimeout = 8 * time.Second
	ingress := DefaultLimits()
	ingress.Stall = 10 * time.Second
	opts := routeAcceptanceNodeOptions{adapter: adapter, store: limits, limits: ingress}
	return startRouteAcceptanceNode(t, opts)
}

func scanRPCCommand(request *pb.ScanRequest) *pb.Command {
	variant := &pb.Command_Scan{Scan: request}
	command := &pb.Command{Operation: variant}
	return command
}

func scanRPCPage(t *testing.T, client pb.StoreServiceClient, request *pb.ScanRequest) ([]*pb.Document, *pb.ScanEnd) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	command := scanRPCCommand(request)
	stream, err := testutil.ExecuteEvents(ctx, client, "records", command)
	if err != nil {
		t.Fatal(err)
	}
	var documents []*pb.Document
	var end *pb.ScanEnd
	bytes := 0
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("Scan RPC did not finish", err)
		}
		bytes += proto.Size(event)
		if bytes > 32<<20 || len(documents) > int(protocol.MaxScanPageSize) {
			t.Fatal("Scan test capture exceeded its bounds")
		}
		if end != nil {
			t.Fatal("Scan emitted an event after its terminal result")
		}
		if document := event.GetDocument(); document != nil {
			documents = append(documents, document)
			continue
		}
		end = event.GetScanEnd()
		if end == nil {
			t.Fatal("Scan emitted a non-document event", event)
		}
	}
	if end == nil || end.Failure != nil || end.DocumentCount != uint64(len(documents)) || end.DocumentCount > protocol.ScanPageSize(request) || end.Exhausted == (len(end.NextContinuationToken) != 0) {
		t.Fatal("Scan terminal result is invalid", end)
	}
	return documents, end
}

type scanRPCRestartOptions struct {
	resource     string
	pageSize     uint32
	ids          []string
	open         func(*testing.T) execution.Adapter
	documentID   func(*testing.T, *pb.Document) string
	beforeResume func(*testing.T)
	observation  *scanRPCObservation
}

func assertScanRPCBatchesAcrossRestart(t *testing.T, opts scanRPCRestartOptions) {
	t.Helper()
	seen := make(map[string]bool, len(opts.ids))
	var token []byte
	read := func(t *testing.T, client pb.StoreServiceClient) *pb.ScanEnd {
		t.Helper()
		request := &pb.ScanRequest{Resource: opts.resource, PageSize: opts.pageSize, ContinuationToken: bytes.Clone(token)}
		documents, end := scanRPCPage(t, client, request)
		for _, document := range documents {
			id := opts.documentID(t, document)
			if seen[id] {
				t.Fatal("Scan repeated a document across batches or instances", id)
			}
			seen[id] = true
		}
		token = bytes.Clone(end.NextContinuationToken)
		return end
	}
	if !t.Run("origin", func(t *testing.T) {
		adapter := opts.open(t)
		node := startScanRPCNode(t, adapter)
		client := routeAcceptanceClient(t, node.address)
		end := read(t, client)
		request := &pb.ScanRequest{PageSize: opts.pageSize}
		if end.Exhausted || end.DocumentCount != protocol.ScanPageSize(request) {
			t.Fatal("first Scan did not return the requested complete page", end)
		}
		assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
	}) {
		return
	}
	// Subtest cleanup closes the origin runtime, adapter and gRPC listener.
	if opts.beforeResume != nil {
		opts.beforeResume(t)
	}
	if !t.Run("replacement", func(t *testing.T) {
		adapter := opts.open(t)
		node := startScanRPCNode(t, adapter)
		client := routeAcceptanceClient(t, node.address)
		for page := 0; page < 10; page++ {
			end := read(t, client)
			assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
			if end.Exhausted {
				return
			}
			if end.DocumentCount == 0 {
				t.Fatal("Scan continuation stopped advancing")
			}
		}
		t.Fatal("Scan continuation did not exhaust")
	}) {
		return
	}
	if len(seen) != len(opts.ids) {
		t.Fatal("Scan did not preserve the complete fixture", len(seen), len(opts.ids))
	}
	for _, id := range opts.ids {
		if !seen[id] {
			t.Fatal("Scan omitted a document", id)
		}
	}
	pages := opts.observation.snapshot()
	if len(pages) < 5 || len(pages) > 8 {
		t.Fatal("Scan reverted to per-document backend requests", pages)
	}
	for _, size := range pages {
		if size < 1 || size > execution.ScanBatchDocuments {
			t.Fatal("Scan requested an unbounded native batch", pages)
		}
	}
	t.Logf("public Scan documents=%d page_size=%d backend_fetches=%d requested_batches=%v", len(seen), opts.pageSize, len(pages), pages)
}

func scanRPCMongoDocumentID(t *testing.T, document *pb.Document) string {
	t.Helper()
	id, ok := bson.Raw(document.Data).Lookup("_id").StringValueOK()
	if document.MediaType != "application/bson" || !ok {
		t.Fatal("Scan changed native BSON identity or encoding")
	}
	return id
}

func scanRPCSearchDocumentID(t *testing.T, document *pb.Document) string {
	t.Helper()
	var hit struct {
		ID string `json:"_id"`
	}
	if document.MediaType != "application/json" || json.Unmarshal(document.Data, &hit) != nil || hit.ID == "" {
		t.Fatal("Scan changed native Search hit identity or encoding")
	}
	return hit.ID
}

func seedScanRPCMongo(t *testing.T, backend *testmongo.Fixture, count, padding int) []string {
	t.Helper()
	ids := make([]string, count)
	documents := make([]any, count)
	for i := range ids {
		ids[i] = fmt.Sprintf("record_%04d", i)
		document := bson.D{{Key: "_id", Value: ids[i]}, {Key: "n", Value: int32(i)}, {Key: "pad", Value: strings.Repeat("x", padding)}}
		documents[i] = document
	}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertMany(t.Context(), documents); err != nil {
		t.Fatal(err)
	}
	return ids
}

func seedScanRPCSearch(t *testing.T, backend *testsearch.Backend, count, padding int) []string {
	t.Helper()
	ids := make([]string, count)
	var body strings.Builder
	for i := range ids {
		ids[i] = fmt.Sprintf("record_%04d", i)
		fmt.Fprintf(&body, "{\"index\":{\"_index\":%q,\"_id\":%q}}\n{\"n\":%d,\"pad\":%q}\n", backend.Index, ids[i], i, strings.Repeat("x", padding))
	}
	code, raw := backend.Do(t, "POST", "/_bulk?refresh=true", body.String())
	var reply struct {
		Errors bool `json:"errors"`
	}
	if code != http.StatusOK || json.Unmarshal(raw, &reply) != nil || reply.Errors {
		t.Fatal("Scan fixture bulk seeding failed", code)
	}
	return ids
}

func TestRouteMongoScanBatches513DocumentsAcrossInstances(t *testing.T) {
	for _, pageSize := range []uint32{0, 256} {
		t.Run(strconv.Itoa(int(pageSize)), func(t *testing.T) {
			backend := testmongo.Open(t)
			ids := seedScanRPCMongo(t, backend, 513, 32)
			proxy := testmongo.StartProxy(t, backend)
			observation := &scanRPCObservation{}
			monitor := &event.CommandMonitor{}
			monitor.Started = func(_ context.Context, event *event.CommandStartedEvent) {
				if event.CommandName == "find" {
					observation.record(int(event.Command.Lookup("limit").AsInt64()))
				}
			}
			proxy.Monitor = monitor
			opts := scanRPCRestartOptions{resource: backend.DB + "/records", pageSize: pageSize, ids: ids, documentID: scanRPCMongoDocumentID, observation: observation}
			opts.open = func(t *testing.T) execution.Adapter {
				config := mongodb.Config{Store: "records", URI: proxy.URI(), Pool: 2, MaxReadSize: protocol.MaxDocument}
				adapter, err := mongodb.Open(t.Context(), config)
				if err != nil {
					t.Fatal(err)
				}
				return adapter
			}
			assertScanRPCBatchesAcrossRestart(t, opts)
		})
	}
}

func scanRPCSearchProxy(t *testing.T, backend *testsearch.Backend, observation *scanRPCObservation) string {
	t.Helper()
	proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, inbound *http.Request) {
		body, err := io.ReadAll(io.LimitReader(inbound.Body, 1<<20))
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		if inbound.URL.Path == "/_search" {
			var query struct {
				Size int `json:"size"`
			}
			if json.Unmarshal(body, &query) != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}
			observation.record(query.Size)
		}
		request, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, backend.URL+inbound.URL.RequestURI(), bytes.NewReader(body))
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		request.Header = inbound.Header.Clone()
		request.GetBody = nil
		response, err := backend.Client.Do(request)
		if err != nil {
			writer.WriteHeader(http.StatusBadGateway)
			return
		}
		defer response.Body.Close()
		for name, values := range response.Header {
			for _, value := range values {
				writer.Header().Add(name, value)
			}
		}
		writer.WriteHeader(response.StatusCode)
		_, _ = io.Copy(writer, response.Body)
	}))
	t.Cleanup(proxy.Close)
	return proxy.URL
}

func TestRouteSearchScanBatches513DocumentsAcrossInstances(t *testing.T) {
	for _, pageSize := range []uint32{0, 256} {
		t.Run(strconv.Itoa(int(pageSize)), func(t *testing.T) {
			backend := testsearch.Open(t)
			ids := seedScanRPCSearch(t, backend, 513, 32)
			observation := &scanRPCObservation{}
			proxyURL := scanRPCSearchProxy(t, backend, observation)
			opts := scanRPCRestartOptions{resource: backend.Index, pageSize: pageSize, ids: ids, documentID: scanRPCSearchDocumentID, observation: observation}
			opts.open = func(t *testing.T) execution.Adapter {
				config := search.Config{Store: "records", URL: proxyURL, Pool: 2, MaxReadSize: protocol.MaxDocument}
				adapter, err := search.Open(t.Context(), config)
				if err != nil {
					t.Fatal(err)
				}
				return adapter
			}
			opts.beforeResume = func(t *testing.T) {
				code, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/after_origin?refresh=true", `{"n":999}`)
				if code != http.StatusCreated {
					t.Fatal("Scan snapshot fixture write failed", code)
				}
			}
			assertScanRPCBatchesAcrossRestart(t, opts)
		})
	}
}

type scanRPCStallOptions struct {
	adapter      execution.Adapter
	resource     string
	readResource string
	selector     *pb.Document
}

func assertScanRPCStallReleasesPermit(t *testing.T, opts scanRPCStallOptions) {
	t.Helper()
	node := startScanRPCNode(t, opts.adapter)
	client := routeAcceptanceClient(t, node.address)
	caller, cancel := context.WithCancel(t.Context())
	defer cancel()
	request := &pb.ScanRequest{Resource: opts.resource, PageSize: 256}
	command := scanRPCCommand(request)
	stream, err := testutil.ExecuteEvents(caller, client, "records", command)
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var stalledSince time.Time
	for {
		snapshot := node.runtime.Snapshot()
		if snapshot.Publishers == 1 && snapshot.Active == 0 && snapshot.WorkingBytes == 0 {
			if snapshot.ResultBytes > execution.ScanResultBytes || snapshot.Retained != 1 {
				t.Fatal("slow Scan retained more than one bounded output batch", snapshot)
			}
			if stalledSince.IsZero() {
				stalledSince = time.Now()
			}
			if time.Since(stalledSince) >= 50*time.Millisecond {
				break
			}
		} else {
			stalledSince = time.Time{}
		}
		if time.Now().After(deadline) {
			t.Fatal("slow Scan consumer retained the only execution permit", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	readContext, stop := context.WithTimeout(t.Context(), 2*time.Second)
	read := &pb.ReadRequest{Resource: opts.readResource}
	batch := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.
		Command{Operation: &pb.Command_Read{
		Read: &pb.ReadBatch{Requests: []*pb.ReadRequest{read}}}}}

	response, err := testutil.ReadRecords(readContext, client, batch.StoreName, batch.Command.GetRead().Requests)
	stop()
	if err != nil || len(response) != 1 || response[0].GetFailure() != nil || response[0].GetDocument() == nil {
		t.Fatal("independent Read was blocked by a slow Scan at concurrency one", response, err)
	}
	// A separate Scan remains live while the first caller cancels. Its context,
	// plan and backend continuation must survive that cancellation.
	survivorContext, survivorCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer survivorCancel()
	survivorRequest := &pb.ScanRequest{Resource: opts.resource, Selector: opts.selector}
	survivorCommand := scanRPCCommand(survivorRequest)
	survivor, err := testutil.ExecuteEvents(survivorContext, client, "records", survivorCommand)
	if err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for node.server.Snapshot().ActiveRPCs < 2 {
		if time.Now().After(deadline) {
			t.Fatal("independent Scan did not start before cancellation")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	// gRPC may already have queued complete document frames before it observes
	// local cancellation. Drain that bounded tail before checking its status.
	for buffered := 0; ; buffered++ {
		_, err := stream.Recv()
		if status.Code(err) == codes.Canceled {
			break
		}
		if err != nil || buffered >= int(protocol.MaxScanPageSize) {
			t.Fatal("canceled Scan did not return caller cancellation", err)
		}
	}
	count := 0
	var end *pb.ScanEnd
	for {
		event, err := survivor.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal("one caller canceled an independent Scan", err)
		}
		if event.GetDocument() != nil {
			count++
		} else {
			end = event.GetScanEnd()
		}
	}
	if count != 1 || end == nil || end.Failure != nil || !end.Exhausted || end.DocumentCount != 1 {
		t.Fatal("independent Scan result changed after another caller canceled", count, end)
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
	complete := &pb.ScanRequest{Resource: opts.resource, PageSize: 256}
	documents, terminal := scanRPCPage(t, client, complete)
	if len(documents) != 40 || !terminal.Exhausted {
		t.Fatal("Scan lost documents while splitting output by its byte bound", len(documents), terminal)
	}
	seen := make(map[string]bool, len(documents))
	for _, document := range documents {
		var id string
		if document.MediaType == "application/bson" {
			id = scanRPCMongoDocumentID(t, document)
		} else {
			id = scanRPCSearchDocumentID(t, document)
		}
		if seen[id] {
			t.Fatal("Scan repeated a document at a bounded output batch boundary", id)
		}
		seen[id] = true
	}
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func TestRouteMongoScanSlowConsumerDoesNotHoldExecutionPermit(t *testing.T) {
	backend := testmongo.Open(t)
	seedScanRPCMongo(t, backend, 40, 128<<10)
	config := mongodb.Config{Store: "records", URI: backend.URI, Pool: 2, MaxReadSize: protocol.MaxDocument}
	adapter, err := mongodb.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "n", Value: int32(0)}}
	selector := bson.D{{Key: "filter", Value: filter}}
	raw, err := bson.Marshal(selector)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: "application/bson", Data: raw}
	opts := scanRPCStallOptions{adapter: adapter, resource: backend.DB + "/records", readResource: backend.DB + "/records/s:record_0000", selector: document}
	assertScanRPCStallReleasesPermit(t, opts)
}

func TestRouteSearchScanSlowConsumerDoesNotHoldExecutionPermit(t *testing.T) {
	backend := testsearch.Open(t)
	seedScanRPCSearch(t, backend, 40, 128<<10)
	config := search.Config{Store: "records", URL: backend.URL, Pool: 2, MaxReadSize: protocol.MaxDocument}
	adapter, err := search.Open(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{MediaType: "application/json", Data: []byte(`{"query":{"term":{"n":0}}}`)}
	opts := scanRPCStallOptions{adapter: adapter, resource: backend.Index, readResource: backend.Index + "/s:record_0000", selector: document}
	assertScanRPCStallReleasesPermit(t, opts)
}
