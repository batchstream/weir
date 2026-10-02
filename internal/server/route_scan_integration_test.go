//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type routeScanAcrossInstancesOptions struct {
	resource     string
	wantIDs      []string
	open         func(*testing.T) execution.Adapter
	documentID   func(*testing.T, *pb.Document) string
	beforeResume func(*testing.T)
}

func assertRouteScanAcrossInstances(t *testing.T, opts routeScanAcrossInstancesOptions) {
	t.Helper()
	const pageSize = 3
	seen := make(map[string]bool)
	var token []byte
	readPage := func(t *testing.T, client pb.StoreServiceClient) *pb.ScanEnd {
		t.Helper()
		request := &pb.ScanRequest{Resource: opts.resource, PageSize: pageSize, ContinuationToken: bytes.Clone(token)}
		variant := &pb.Call_Scan{Scan: request}
		call := &pb.Call{Version: 1, Operation: variant}
		events := routeBackendEvents(t, client, call)
		if len(events) == 0 || len(events) > pageSize+1 {
			t.Fatal("scan did not complete one bounded page", len(events))
		}
		end := events[len(events)-1].GetScanEnd()
		if end == nil || end.Failure != nil || end.DocumentCount != uint64(len(events)-1) || end.Exhausted == (len(end.NextContinuationToken) != 0) {
			t.Fatal("scan page has an invalid terminal result", end)
		}
		for _, event := range events[:len(events)-1] {
			document := event.GetDocument()
			if document == nil {
				t.Fatal("scan page contains a non-document event", event)
			}
			id := opts.documentID(t, document)
			if seen[id] {
				t.Fatal("replacement instance repeated a document", id)
			}
			seen[id] = true
		}
		token = bytes.Clone(end.NextContinuationToken)
		return end
	}

	if !t.Run("origin", func(t *testing.T) {
		adapter := opts.open(t)
		nodes, client := routeBackendServer(t, adapter)
		end := readPage(t, client)
		if end.Exhausted || end.DocumentCount != pageSize || len(token) == 0 {
			t.Fatal("origin did not return a resumable page", end)
		}
		assertRouteAcceptanceIdle(t, nodes)
	}) {
		return
	}
	// The origin subtest's cleanup joins both servers and closes its adapter and
	// Store runtime before any replacement instance is created.
	if opts.beforeResume != nil {
		opts.beforeResume(t)
	}
	if !t.Run("replacement", func(t *testing.T) {
		adapter := opts.open(t)
		nodes, client := routeBackendServer(t, adapter)
		for page := 0; page <= len(opts.wantIDs); page++ {
			end := readPage(t, client)
			assertRouteAcceptanceIdle(t, nodes)
			if end.Exhausted {
				return
			}
			if end.DocumentCount == 0 {
				t.Fatal("continuation did not advance", end)
			}
		}
		t.Fatal("scan continuation did not exhaust")
	}) {
		return
	}
	if len(seen) != len(opts.wantIDs) {
		t.Fatal("scan changed the complete document set across instances", seen, opts.wantIDs)
	}
	for _, id := range opts.wantIDs {
		if !seen[id] {
			t.Fatal("replacement instance skipped a document", id)
		}
	}
}

func TestRouteMongoScanContinuesOnNewInstanceAfterOriginShutdown(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	ids := make([]string, 7)
	documents := make([]any, len(ids))
	for i := range ids {
		ids[i] = fmt.Sprintf("record_%02d", i)
		document := bson.D{{Key: "_id", Value: ids[i]}, {Key: "n", Value: int32(i)}}
		documents[i] = document
	}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertMany(ctx, documents); err != nil {
		t.Fatal(err)
	}
	opts := routeScanAcrossInstancesOptions{
		resource: backend.DB + "/records",
		wantIDs:  ids,
	}
	opts.open = func(t *testing.T) execution.Adapter {
		config := mongodb.Config{Store: "records", URI: backend.URI, Pool: 2}
		adapter, err := mongodb.Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}
	opts.documentID = func(t *testing.T, document *pb.Document) string {
		t.Helper()
		if document.MediaType != "application/bson" {
			t.Fatal("Mongo scan changed native document encoding", document.MediaType)
		}
		id, ok := bson.Raw(document.Data).Lookup("_id").StringValueOK()
		if !ok {
			t.Fatal("Mongo scan omitted the document identifier")
		}
		return id
	}
	assertRouteScanAcrossInstances(t, opts)
}

func TestRouteSearchScanContinuesOnNewInstanceAfterOriginShutdown(t *testing.T) {
	backend := testsearch.Open(t)
	ids := make([]string, 7)
	for i := range ids {
		ids[i] = fmt.Sprintf("record_%02d", i)
		body := fmt.Sprintf(`{"n":%d}`, i)
		status, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/"+ids[i], body)
		if status != 201 {
			t.Fatal("Search scan fixture write failed", status)
		}
	}
	status, _ := backend.Do(t, "POST", "/"+backend.Index+"/_refresh", "")
	if status != 200 {
		t.Fatal("Search scan fixture refresh failed", status)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	opts := routeScanAcrossInstancesOptions{
		resource: backend.Index,
		wantIDs:  ids,
	}
	opts.open = func(t *testing.T) execution.Adapter {
		config := search.Config{Store: "records", URL: backend.URL, Pool: 2}
		adapter, err := search.Open(ctx, config)
		if err != nil {
			t.Fatal(err)
		}
		return adapter
	}
	opts.documentID = func(t *testing.T, document *pb.Document) string {
		t.Helper()
		var hit struct {
			ID string `json:"_id"`
		}
		if document.MediaType != "application/json" || json.Unmarshal(document.Data, &hit) != nil || hit.ID == "" {
			t.Fatal("Search scan changed native hit encoding")
		}
		return hit.ID
	}
	opts.beforeResume = func(t *testing.T) {
		status, _ := backend.Do(t, "PUT", "/"+backend.Index+"/_doc/after_origin_shutdown", `{"n":99}`)
		if status != 201 {
			t.Fatal("Search snapshot fixture write failed", status)
		}
		status, _ = backend.Do(t, "POST", "/"+backend.Index+"/_refresh", "")
		if status != 200 {
			t.Fatal("Search snapshot fixture refresh failed", status)
		}
	}
	assertRouteScanAcrossInstances(t, opts)
}
