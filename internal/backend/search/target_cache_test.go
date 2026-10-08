package search

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/targetcache"
	"github.com/batchstream/weir/internal/execution"
)

func TestSearchHotRecordsDoNotRepeatMetadataIO(t *testing.T) {
	var metadata, reads, writes atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/records":
			metadata.Add(1)
			_, _ = io.WriteString(w, testIndexReply)
		case "/records/_mget":
			reads.Add(1)
			_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"same","found":true,"_seq_no":1,"_primary_term":1,"_source":{"n":1}}]}`)
		case "/_bulk":
			writes.Add(1)
			_, _ = io.WriteString(w, `{"errors":false,"took":1,"items":[{"index":{"_index":"records","_id":"same","status":200,"result":"updated","_seq_no":1,"_primary_term":1,"_shards":{"total":1,"successful":1,"failed":0}}}]}`)
		default:
			t.Error("unexpected target request", r.URL.Path)
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: cfg, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
	for range 2 {
		read := batchTestPlan(t, adapter, "read", "records/s:same")
		put := batchTestPlan(t, adapter, "put", "records/s:same")
		put.ID = 1
		works := []*execution.Plan{read, put}
		results := adapter.executeRecords(context.Background(), works)
		if len(results) != 2 || results[0].GetReadResult().GetDocument() == nil || results[1].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("hot target lost business results", results)
		}
	}
	if metadata.Load() != 1 || reads.Load() != 2 || writes.Load() != 2 {
		t.Fatal("hot requests repeated metadata or lost business I/O", metadata.Load(), reads.Load(), writes.Load())
	}
}

func TestSearchConcurrentTargetInspectionUsesOneRequest(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var inspections atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if inspections.Add(1) == 1 {
			close(started)
			<-release
		}
		_, _ = io.WriteString(w, testIndexReply)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	defer func() {
		select {
		case <-release:
		default:
			close(release)
		}
	}()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, client: server.Client(), ctx: context.Background()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var workers sync.WaitGroup
	workers.Go(func() {
		caps, failure := adapter.inspect(ctx, "records", false)
		if failure != nil || !caps.source || !caps.write {
			t.Error("first target check failed", caps, failure)
		}
	})
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first target inspection did not start")
	}
	for range 16 {
		workers.Go(func() {
			caps, failure := adapter.inspect(ctx, "records", false)
			if failure != nil || !caps.source || !caps.write {
				t.Error("shared target check failed", caps, failure)
			}
		})
	}
	waiter, stop := context.WithTimeout(ctx, 10*time.Millisecond)
	defer stop()
	if _, failure := adapter.inspect(waiter, "records", false); failure.GetCode() != pb.FailureCode_DEADLINE_EXCEEDED {
		t.Fatal("metadata waiter ignored cancellation", failure)
	}
	close(release)
	workers.Wait()
	if inspections.Load() != 1 {
		t.Fatal("concurrent cold requests repeated inspection", inspections.Load())
	}
}

func TestSearchFailedInspectionIsRetriedAndCapabilitiesAreIsolated(t *testing.T) {
	var left, right atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/left":
			if left.Add(1) == 1 {
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = io.WriteString(w, `{}`)
				return
			}
			_, _ = io.WriteString(w, strings.ReplaceAll(testIndexReply, "records", "left"))
		case "/right":
			right.Add(1)
			_, _ = io.WriteString(w, `{"right":{"settings":{"index.uuid":"test","index.number_of_shards":"1"},"mappings":{"_source":{"enabled":false}}}}`)
		default:
			http.NotFound(w, r)
		}
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, client: server.Client(), ctx: context.Background()}
	if _, failure := adapter.inspect(context.Background(), "left", false); failure.GetCode() != pb.FailureCode_UNAVAILABLE {
		t.Fatal("failed target inspection was accepted", failure)
	}
	for range 2 {
		caps, failure := adapter.inspect(context.Background(), "left", false)
		if failure != nil || !caps.source || !caps.write {
			t.Fatal("failed inspection poisoned retry", caps, failure)
		}
		caps, failure = adapter.inspect(context.Background(), "right", false)
		if failure != nil || caps.source || caps.write {
			t.Fatal("capabilities crossed index cache keys", caps, failure)
		}
	}
	if left.Load() != 2 || right.Load() != 1 {
		t.Fatal("failure cached or capabilities reloaded", left.Load(), right.Load())
	}
}

func TestSearchCanceledInspectionDoesNotPoisonRetry(t *testing.T) {
	var inspections atomic.Int32
	started := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if inspections.Add(1) == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		_, _ = fmt.Fprint(w, testIndexReply)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, client: server.Client(), ctx: context.Background()}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan *pb.Failure, 1)
	go func() {
		_, failure := adapter.inspect(ctx, "records", false)
		done <- failure
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("inspection did not start")
	}
	cancel()
	if failure := <-done; failure.GetCode() != pb.FailureCode_CANCELLED {
		t.Fatal("canceled inspection did not preserve cancellation", failure)
	}
	caps, failure := adapter.inspect(context.Background(), "records", false)
	if failure != nil || !caps.source || inspections.Load() != 2 {
		t.Fatal("canceled inspection poisoned retry", caps, failure, inspections.Load())
	}
	if _, failure := adapter.inspect(ctx, "records", false); failure.GetCode() != pb.FailureCode_CANCELLED || inspections.Load() != 2 {
		t.Fatal("hot cache ignored cancellation", failure, inspections.Load())
	}
}

func TestSearchMetadataCacheRechecksEvictedTargets(t *testing.T) {
	var inspections atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		inspections.Add(1)
		index := strings.TrimPrefix(r.URL.Path, "/")
		_, _ = io.WriteString(w, strings.ReplaceAll(testIndexReply, "records", index))
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{URL: server.URL}
	adapter := &Adapter{config: cfg, client: server.Client(), ctx: context.Background()}
	for i := 0; i <= targetcache.DefaultCapacity; i++ {
		index := fmt.Sprintf("target%d", i)
		if _, failure := adapter.inspect(context.Background(), index, false); failure != nil {
			t.Fatal(failure)
		}
	}
	latest := fmt.Sprintf("target%d", targetcache.DefaultCapacity)
	if _, failure := adapter.inspect(context.Background(), latest, false); failure != nil {
		t.Fatal(failure)
	}
	if inspections.Load() != targetcache.DefaultCapacity+1 {
		t.Fatal("recent target was not retained", inspections.Load())
	}
	if _, failure := adapter.inspect(context.Background(), "target0", false); failure != nil {
		t.Fatal(failure)
	}
	if inspections.Load() != targetcache.DefaultCapacity+2 {
		t.Fatal("evicted target did not repeat its metadata check", inspections.Load())
	}
}

func TestSearchCachedTargetStillChecksBusinessPermissions(t *testing.T) {
	var metadata, reads atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/records" {
			metadata.Add(1)
			_, _ = io.WriteString(w, testIndexReply)
			return
		}
		if r.URL.Path != "/records/_mget" {
			t.Error("unexpected business request", r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if reads.Add(1) == 1 {
			_, _ = io.WriteString(w, `{"docs":[{"_index":"records","_id":"same","found":false}]}`)
			return
		}
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"error":{"type":"security_exception"},"status":403}`)
	})
	server := httptest.NewServer(handler)
	defer server.Close()
	cfg := Config{Store: "search", URL: server.URL}
	adapter := &Adapter{config: cfg, dialect: ElasticsearchProduct, client: server.Client(), ctx: context.Background()}
	read := batchTestPlan(t, adapter, "read", "records/s:same")
	works := []*execution.Plan{read}
	results := adapter.executeRecords(context.Background(), works)
	if results[0].GetReadResult().GetMissing() == nil {
		t.Fatal("first authorized read failed", results)
	}
	results = adapter.executeRecords(context.Background(), works)
	if results[0].GetReadResult().GetFailure() == nil || results[0].GetReadResult().GetMissing() != nil || results[0].GetReadResult().GetDocument() != nil {
		t.Fatal("cached metadata became a cached authorization", results)
	}
	if metadata.Load() != 1 || reads.Load() != 2 {
		t.Fatal("cached target skipped business permission enforcement", metadata.Load(), reads.Load())
	}
}
