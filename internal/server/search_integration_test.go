//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/app"
	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/searchstore"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testmongo"
	"github.com/batchstream/weir/internal/testsearch"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type dualFixture struct {
	fixture
	search  *store.Runtime
	backend *testsearch.Backend
}

func dualServer(t *testing.T, backend *testsearch.Backend, endpoint string, limits Limits) dualFixture {
	t.Helper()
	native, db := testmongo.Open(t)
	mongo := mongostore.Config{URI: testmongo.URI, Store: "mongo", Database: db, Collection: "records"}
	search := &searchstore.Config{URL: endpoint, Profile: backend.Profile, Store: "search", Index: backend.Index}
	cfg := app.Config{Mongo: mongo, Search: search, Limits: store.DefaultLimits()}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	routes, err := app.OpenStores(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(routes, limits)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDisableRetry(), grpc.WithStaticStreamWindowSize(65535), grpc.WithStaticConnWindowSize(256<<10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = conn.Close()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	base := fixture{server: server, runtime: routes["mongo"], client: pb.NewWeirClient(conn), conn: conn, native: native, db: db, address: listener.Addr().String()}
	f := dualFixture{fixture: base, search: routes["search"], backend: backend}
	return f
}
func searchResource(f dualFixture, id string) string {
	return "weir://search/" + f.backend.Index + "/s:" + id
}
func searchMutation(f dualFixture, id string, n int) *pb.MutateRequest {
	document := &pb.Document{MediaType: "application/json", Data: []byte(fmt.Sprintf(`{"n":%d}`, n))}
	action := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: searchResource(f, id), Action: action}
	return request
}
func TestDualStoreRoutesAndSearchBulkOrder(t *testing.T) {
	b := testsearch.Open(t)
	limits := DefaultLimits()
	f := dualServer(t, b, b.URL, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mongo := mutation(f.fixture, "mongo", 7)
	if result, err := f.client.Mutate(ctx, mongo); err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result, err)
	}
	stream, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	open := &pb.BulkOpen{Store: "weir://search"}
	opening := &pb.BulkRequestFrame_Open{Open: open}
	frame := &pb.BulkRequestFrame{Frame: opening}
	if err := stream.Send(frame); err != nil {
		t.Fatal(err)
	}
	const count = 24
	sent := make(chan error, 1)
	go func() {
		for i := 0; i < count; i++ {
			op := &pb.BulkOperation{Index: uint64(i)}
			if i%2 == 0 {
				request := searchMutation(f, "ordered", i/2)
				op.Operation = &pb.BulkOperation_Mutate{Mutate: request}
			} else {
				request := &pb.ReadRequest{Resource: searchResource(f, "ordered")}
				op.Operation = &pb.BulkOperation_Read{Read: request}
			}
			variant := &pb.BulkRequestFrame_Operation{Operation: op}
			frame := &pb.BulkRequestFrame{Frame: variant}
			if err := stream.Send(frame); err != nil {
				sent <- err
				return
			}
		}
		// Pinning must reject, not reroute, a later MongoDB operation.
		request := mutation(f.fixture, "must-not-exist", 1)
		variant := &pb.BulkOperation_Mutate{Mutate: request}
		op := &pb.BulkOperation{Index: count, Operation: variant}
		outer := &pb.BulkRequestFrame_Operation{Operation: op}
		frame := &pb.BulkRequestFrame{Frame: outer}
		if err := stream.Send(frame); err != nil {
			sent <- err
			return
		}
		sent <- stream.CloseSend()
	}()
	seen := make(map[uint64]bool)
	for {
		frame, err := stream.Recv()
		if err != nil {
			t.Fatal("missing Bulk End", err)
		}
		if end := frame.GetEnd(); end != nil {
			if len(seen) != count+1 || end.ResultCount != count+1 || end.ReceivedCount != count+1 {
				t.Fatal("bad accounting", end, len(seen))
			}
			break
		}
		result := frame.GetResult()
		if result == nil || seen[result.Index] {
			t.Fatal("invalid/duplicate result", frame)
		}
		seen[result.Index] = true
		if result.Index == count {
			if result.GetMutation().GetOutcome() != pb.MutationOutcome_NOT_STARTED || result.GetMutation().GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
				t.Fatal("cross-store operation not rejected", result)
			}
			continue
		}
		if result.Index%2 == 0 {
			if result.GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
				t.Fatal(result)
			}
		} else {
			var source struct{ N int }
			if json.Unmarshal(result.GetRead().GetDocument().GetData(), &source) != nil || source.N != int(result.Index/2) {
				t.Fatal("same-key order lost", result)
			}
		}
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("missing final gRPC OK", err)
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	request := &pb.ReadRequest{Resource: resource(f.fixture, "must-not-exist")}
	result, err := f.client.Read(ctx, request)
	if err != nil || result.GetMissing() == nil {
		t.Fatal("cross-store write executed", result, err)
	}
	unknown := &pb.ReadRequest{Resource: "weir://unknown/records/s:x"}
	result, err = f.client.Read(ctx, unknown)
	if err != nil || result.GetFailure().GetCode() != pb.FailureCode_INVALID_ARGUMENT {
		t.Fatal("unknown store fallback", result, err)
	}
	if f.search == f.runtime {
		t.Fatal("stores share runtime")
	}
}
func TestDualStoreSlowBackendAndOverloadProgress(t *testing.T) {
	b := testsearch.Open(t)
	reached := make(chan struct{}, 1)
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		response, err := b.Client.Do(request)
		if err != nil {
			if r.Context().Err() == nil {
				t.Error(err)
			}
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(r.URL.Path, "/_doc/slow") {
			reached <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	limits := DefaultLimits()
	f := dualServer(t, b, proxy.URL, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		request := &pb.ReadRequest{Resource: searchResource(f, "slow")}
		result, err := f.client.Read(ctx, request)
		if err == nil && result.GetMissing() == nil {
			err = fmt.Errorf("bad search read: %v", result)
		}
		done <- err
	}()
	select {
	case <-reached:
	case <-ctx.Done():
		t.Fatal("search did not reach backend")
	}
	independent, stop := context.WithTimeout(ctx, 300*time.Millisecond)
	request := mutation(f.fixture, "independent", 9)
	result, err := f.client.Mutate(independent, request)
	stop()
	if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("slow Search blocked MongoDB", result, err)
	}
	if f.search.Snapshot().Active != 1 || f.runtime.Snapshot().Active != 0 {
		t.Fatal("execution permits shared", f.search.Snapshot(), f.runtime.Snapshot())
	}
	f.search.SetOverloaded(true)
	f.runtime.SetOverloaded(true)
	mongoRead := &pb.ReadRequest{Resource: resource(f.fixture, "independent")}
	if _, err := f.client.Read(ctx, mongoRead); status.Code(err) != codes.ResourceExhausted {
		t.Fatal("new admission not stopped", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal("overload blocked admitted delivery", err)
	}
	f.search.SetOverloaded(false)
	f.runtime.SetOverloaded(false)
	if result, err := f.client.Read(ctx, mongoRead); err != nil || result.GetDocument() == nil {
		t.Fatal(result, err)
	}
}
func TestSearchUnaryAndSlowBulkTransport(t *testing.T) {
	b := testsearch.Open(t)
	limits := DefaultLimits()
	limits.UnaryLifetime = 100 * time.Millisecond
	limits.Stall = 100 * time.Millisecond
	f := dualServer(t, b, b.URL, limits)
	statusCode, _ := b.Do(t, "PUT", "/"+b.Index+"/_doc/large", `{"payload":"`+strings.Repeat("x", 200<<10)+`"}`)
	if statusCode != 201 {
		t.Fatal(statusCode)
	}
	conn := acceptanceConnection(t, f.address)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	desc := &grpc.StreamDesc{ServerStreams: true}
	stream, err := conn.NewStream(ctx, desc, pb.Weir_Read_FullMethodName)
	if err != nil {
		t.Fatal(err)
	}
	request := &pb.ReadRequest{Resource: searchResource(f, "large")}
	if err := stream.SendMsg(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Header(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(600 * time.Millisecond)
	var result pb.ReadResult
	firstErr := stream.RecvMsg(&result)
	if firstErr == nil {
		var extra pb.ReadResult
		firstErr = stream.RecvMsg(&extra)
	}
	if firstErr == nil || firstErr == io.EOF {
		t.Fatal("Search unary exceeded server lifetime with OK")
	}
	// Continuous producer, no result reads: result reservations must cap execution.
	bulk, err := f.client.Bulk(ctx)
	if err != nil {
		t.Fatal(err)
	}
	opening := &pb.BulkOpen{Store: "weir://search"}
	ov := &pb.BulkRequestFrame_Open{Open: opening}
	of := &pb.BulkRequestFrame{Frame: ov}
	if err := bulk.Send(of); err != nil {
		t.Fatal(err)
	}
	var sent atomic.Int32
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		for i := 0; i < 100000; i++ {
			read := &pb.ReadRequest{Resource: searchResource(f, "large")}
			v := &pb.BulkOperation_Read{Read: read}
			op := &pb.BulkOperation{Index: uint64(i), Operation: v}
			outer := &pb.BulkRequestFrame_Operation{Operation: op}
			frame := &pb.BulkRequestFrame{Frame: outer}
			if bulk.Send(frame) != nil {
				return
			}
			sent.Add(1)
		}
	}()
	peak := 0
	for i := 0; i < 300; i++ {
		snapshot := f.search.Snapshot()
		peak = max(peak, snapshot.Retained)
		if snapshot.Retained > 8 || snapshot.Active > 4 {
			t.Fatal("Search resource bound", snapshot)
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("unread bulk producer never stopped")
	}
	for {
		frame, err := bulk.Recv()
		if err != nil {
			if err == io.EOF {
				t.Fatal("truncated Bulk returned OK")
			}
			break
		}
		if frame.GetEnd() != nil {
			t.Fatal("stalled producer unexpectedly completed")
		}
	}
	if peak == 0 || sent.Load() == 100000 {
		t.Fatal("did not exercise backpressure", peak, sent.Load())
	}
	deadline := time.Now().Add(time.Second)
	for f.search.Snapshot().Retained != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if f.search.Snapshot().Retained != 0 || f.search.Snapshot().Active != 0 {
		t.Fatal("Search transport cleanup leaked", f.search.Snapshot())
	}
	t.Log("Search retained peak", peak, "producer frames before stop", sent.Load())
}

func TestDualStoreIndependentSearchReadsAndCooldown(t *testing.T) {
	b := testsearch.Open(t)
	arrived := make(chan struct{}, 2)
	release := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request, err := http.NewRequestWithContext(r.Context(), r.Method, b.URL+r.URL.RequestURI(), r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		request.Header = r.Header.Clone()
		response, err := b.Client.Do(request)
		if err != nil {
			if r.Context().Err() == nil {
				t.Error(err)
			}
			return
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		if err != nil {
			t.Error(err)
			return
		}
		if strings.Contains(r.URL.Path, "/_doc/parallel") {
			arrived <- struct{}{}
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
		}
		if strings.Contains(r.URL.Path, "/_doc/timeout") {
			<-r.Context().Done()
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(response.StatusCode)
		_, _ = w.Write(raw)
	})
	proxy := httptest.NewServer(handler)
	defer proxy.Close()
	limits := DefaultLimits()
	f := dualServer(t, b, proxy.URL, limits)
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	var tickets []*store.Ticket
	for i := 0; i < 24; i++ {
		request := &pb.ReadRequest{Resource: searchResource(f, fmt.Sprintf("warm%d", i))}
		variant := &pb.BulkOperation_Read{Read: request}
		op := &pb.BulkOperation{Operation: variant}
		plan, failure := f.search.Prepare(op)
		if failure != nil {
			t.Fatal(failure)
		}
		ticket, failure, _ := f.search.Submit(ctx, plan, nil)
		if failure != nil {
			t.Fatal(failure)
		}
		tickets = append(tickets, ticket)
	}
	for _, ticket := range tickets {
		if _, err := ticket.Wait(ctx); err != nil {
			t.Fatal(err)
		}
		ticket.Ack()
	}
	before := f.search.Snapshot().Window
	if before < 2 {
		t.Fatal("real healthy search work did not grow window", before)
	}
	done := make(chan error, 2)
	for range 2 {
		go func() {
			request := &pb.ReadRequest{Resource: searchResource(f, "parallel")}
			result, err := f.client.Read(ctx, request)
			if err == nil && result.GetMissing() == nil {
				err = fmt.Errorf("bad parallel result")
			}
			done <- err
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(time.Second):
			t.Fatal("independent same-key Search reads serialized")
		}
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	failed := make(chan error, 1)
	go func() {
		request := &pb.ReadRequest{Resource: searchResource(f, "timeout")}
		result, err := f.client.Read(ctx, request)
		if err == nil && result.GetFailure() == nil {
			err = fmt.Errorf("expected failed backend read")
		}
		failed <- err
	}()
	// MongoDB keeps executing while Search occupies a permit and again in cooldown.
	for range 2 {
		request := mutation(f.fixture, "progress", 1)
		limited, stop := context.WithTimeout(ctx, 300*time.Millisecond)
		result, err := f.client.Mutate(limited, request)
		stop()
		if err != nil || result.GetOutcome() != pb.MutationOutcome_APPLIED {
			t.Fatal("Search failure/cooldown blocked MongoDB", result, err)
		}
		if err := <-failed; err != nil {
			t.Fatal(err)
		}
		failed <- nil
	}
	after := f.search.Snapshot().Window
	if after >= before {
		t.Fatal("owned backend timeout supplied no congestion feedback", before, after)
	}
	t.Log("independent reads overlapped; real delayed response timed out; Search window", before, "->", after)
}
