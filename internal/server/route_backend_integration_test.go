//go:build integration

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/api/protocol"
	searchpb "github.com/batchstream/weir/api/weir/search/v1"
	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/protobuf/proto"
)

func routeBackendServer(t *testing.T, adapter execution.Adapter) ([]*routeAcceptanceNode, pb.StoreServiceClient) {
	t.Helper()
	opts := routeAcceptanceNodeOptions{adapter: adapter}
	executor := startRouteAcceptanceNode(t, opts)
	return []*routeAcceptanceNode{executor}, routeAcceptanceClient(t, executor.address)
}

func routeBackendEvents(t *testing.T, client pb.StoreServiceClient, call *pb.Call) []*pb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	produced := false
	var events []*pb.Event
	bytes := 0
	opts := weirclient.Options{StoreName: "records"}
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if produced {
			return nil, io.EOF
		}
		produced = true
		return call, nil
	}
	opts.Consume = func(_ context.Context, _ uint64, event *pb.Event) error {
		bytes += proto.Size(event)
		if len(events) >= 100 || bytes > 9<<20 {
			return errors.New("bounded backend test capture exceeded")
		}
		events = append(events, proto.Clone(event).(*pb.Event))
		return nil
	}
	if err := weirclient.Execute(ctx, client, opts); err != nil {
		t.Fatal(err)
	}
	return events
}

func routeBackendRead(key string) *pb.Call {
	read := &pb.ReadRequest{Resource: key}
	value := &pb.Call_Read{Read: read}
	call := &pb.Call{Version: 1, Operation: value}
	return call
}

func routeBackendMutation(key, kind, media string, raw []byte) *pb.Call {
	document := &pb.Document{MediaType: media, Data: raw}
	mutation := &pb.MutateRequest{Resource: key}
	if kind == "create" {
		value := &pb.MutateRequest_Create{Create: document}
		mutation.Action = value
	} else {
		value := &pb.MutateRequest_Put{Put: document}
		mutation.Action = value
	}
	value := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: value}
	return call
}

func TestRouteMongo2MiBRecordLuaScanAndPartialBatch(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	config := mongodb.Config{Store: "records", URI: backend.URI, Pool: 4, MaxReadSize: protocol.MaxDocument}
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	base := bson.D{{Key: "_id", Value: "large"}, {Key: "blob", Value: []byte{}}}
	raw, err := bson.Marshal(base)
	if err != nil {
		t.Fatal(err)
	}
	base[1].Value = bytes.Repeat([]byte{59}, (2<<20)-len(raw))
	raw, err = bson.Marshal(base)
	if err != nil || len(raw) != 2<<20 {
		t.Fatal("fixture must exercise exactly the legal 2 MiB record boundary", len(raw), err)
	}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, bson.Raw(raw)); err != nil {
		t.Fatal(err)
	}
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	request := routeAcceptanceRead(1, backend.DB+"/records/s:large")
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	_ = stream.CloseSend()
	decoder := newRouteAcceptanceDecoder()
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		event, err := decoder.consume(response)
		if err != nil {
			t.Fatal(err)
		}
		if event != nil && !bytes.Equal(event.GetResult().GetRead().GetDocument().GetData(), raw) {
			t.Fatal("real MongoDB record was rejected, corrupted or truncated", event.GetResult().GetRead().GetFailure())
		}
	}
	if decoder.bytes != 2<<20 || decoder.frames < 33 {
		t.Fatal("real 2 MiB record did not stream directly in legal fragments", decoder.bytes, decoder.frames)
	}

	counter := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, counter); err != nil {
		t.Fatal(err)
	}
	program := &pb.ProgramTransform{Runtime: "lua.v1", Source: []byte(`return weir.replace(weir.set(current, "n", weir.add(weir.get(current, "n"), weir.i32("1"))))`)}
	form := &pb.Transform_Program{Program: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: backend.DB + "/records/s:counter", Action: action}
	value := &pb.Call_Mutate{Mutate: mutation}
	call := &pb.Call{Version: 1, Operation: value}
	produced, applied := 0, 0
	opts := weirclient.Options{StoreName: "records"}
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if produced == 12 {
			return nil, io.EOF
		}
		produced++
		return call, nil
	}
	opts.Consume = func(_ context.Context, _ uint64, event *pb.Event) error {
		result := event.GetResult().GetMutation()
		if result.GetOutcome() != pb.MutationOutcome_APPLIED || result.Failure != nil {
			return fmt.Errorf("Lua did not apply under original transaction boundary: %v", result)
		}
		applied++
		return nil
	}
	if err := weirclient.Execute(ctx, client, opts); err != nil {
		t.Fatal(err)
	}
	var observed struct{ N int32 }
	filter := bson.D{{Key: "_id", Value: "counter"}}
	if err := backend.Admin.Database(backend.DB).Collection("records").FindOne(ctx, filter).Decode(&observed); err != nil || observed.N != 12 || applied != 12 {
		t.Fatal("Lua same-record mutations lost an atomic increment", observed.N, applied, err)
	}
	selector := bson.D{{Key: "filter", Value: bson.D{{Key: "_id", Value: "counter"}}}}
	selectorRaw, _ := bson.Marshal(selector)
	document := &pb.Document{MediaType: "application/bson", Data: selectorRaw}
	scan := &pb.ScanRequest{Resource: backend.DB + "/records", Selector: document}
	scanValue := &pb.Call_Scan{Scan: scan}
	scanCall := &pb.Call{Version: 1, Operation: scanValue}
	events := routeBackendEvents(t, client, scanCall)
	if len(events) != 2 || events[0].GetDocument() == nil || events[1].GetScanEnd().GetDocumentCount() != 1 || events[1].GetScanEnd().Failure != nil || !events[1].GetScanEnd().GetExhausted() || len(events[1].GetScanEnd().GetNextContinuationToken()) != 0 {
		t.Fatal("Mongo scan did not report its exhausted page", events)
	}

	rawCounter, _ := bson.Marshal(counter)
	newDocument := bson.D{{Key: "_id", Value: "new"}, {Key: "n", Value: int32(1)}}
	rawNew, _ := bson.Marshal(newDocument)
	calls := []*pb.Call{routeBackendMutation(backend.DB+"/records/s:counter", "create", "application/bson", rawCounter), routeBackendMutation(backend.DB+"/records/s:new", "put", "application/bson", rawNew)}
	produced = 0
	results := make(map[uint64]*pb.MutationResult)
	opts.Produce = func(context.Context) (*pb.Call, error) {
		if produced == len(calls) {
			return nil, io.EOF
		}
		call := calls[produced]
		produced++
		return call, nil
	}
	opts.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
		results[id] = event.GetResult().GetMutation()
		return nil
	}
	if err := weirclient.Execute(ctx, client, opts); err != nil {
		t.Fatal(err)
	}
	if results[1].GetOutcome() != pb.MutationOutcome_NOT_APPLIED || results[1].GetFailure().GetCode() != pb.FailureCode_PRECONDITION_FAILED || results[2].GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal("partial backend success changed per-item outcomes", results)
	}
	assertRouteAcceptanceIdle(t, nodes)
}

func TestRouteMongoAppliedWriteAndNativeReplyLossAreNotReplayed(t *testing.T) {
	for _, mode := range []string{"record", "native"} {
		t.Run(mode, func(t *testing.T) {
			backend := testmongo.Open(t)
			proxy := testmongo.StartProxy(t, backend)
			command := "bulkWrite"
			if mode == "native" {
				command = "findAndModify"
			}
			proxy.DropCommand = command
			proxy.DropRemaining.Store(1)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			config := mongodb.Config{Store: "records", URI: proxy.URI(), Pool: 1}
			adapter, err := mongodb.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			nodes, client := routeBackendServer(t, adapter)
			document := bson.D{{Key: "_id", Value: "lost"}, {Key: "n", Value: int32(1)}}
			raw, _ := bson.Marshal(document)
			call := routeBackendMutation(backend.DB+"/records/s:lost", "put", "application/bson", raw)
			if mode == "native" {
				initial := bson.D{{Key: "_id", Value: "lost"}, {Key: "n", Value: int32(0)}}
				if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, initial); err != nil {
					t.Fatal(err)
				}
				query := bson.D{{Key: "_id", Value: "lost"}}
				increment := bson.D{{Key: "n", Value: int32(1)}}
				update := bson.D{{Key: "$inc", Value: increment}}
				nativeCommand := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: query}, {Key: "update", Value: update}, {Key: "new", Value: true}}
				body, _ := bson.Marshal(nativeCommand)
				descriptor := &pb.Document{MediaType: mongodb.NativeDescriptor}
				open := &pb.NativeOpen{Resource: backend.DB + "/records", Descriptor_: descriptor, BodyMediaType: "application/bson"}
				native := &pb.NativeCall{Open: open, Body: body}
				value := &pb.Call_Native{Native: native}
				call = &pb.Call{Version: 1, Operation: value}
			}
			events := routeBackendEvents(t, client, call)
			if mode == "record" {
				if len(events) != 1 || events[0].GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
					t.Fatal("lost write ACK was not explicitly indeterminate", events)
				}
			} else if len(events) == 0 || events[len(events)-1].GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_INCOMPLETE {
				t.Fatal("lost native response was reported complete", events)
			}
			var observed struct{ N int32 }
			filter := bson.D{{Key: "_id", Value: "lost"}}
			if err := backend.Admin.Database(backend.DB).Collection("records").FindOne(ctx, filter).Decode(&observed); err != nil || observed.N != 1 {
				t.Fatal("independent backend observation did not confirm one applied write", observed.N, err)
			}
			count := 0
			for _, event := range proxy.Events() {
				if event.Command == command {
					count++
				}
			}
			if count != 1 {
				t.Fatal("possibly applied operation was automatically replayed", mode, count)
			}
			assertRouteAcceptanceIdle(t, nodes)
		})
	}
}

func TestRouteSearch2MiBRecordAndAppliedReplyLoss(t *testing.T) {
	backend := testsearch.Open(t)
	index := backend.Index + "_large"
	backend.Create(t, index, `{"settings":{"number_of_shards":1,"number_of_replicas":0},"mappings":{"properties":{"pad":{"type":"keyword","index":false,"doc_values":false}}}}`)
	body := `{"pad":"` + strings.Repeat("x", (2<<20)-len(`{"pad":""}`)) + `"}`
	status, _ := backend.Do(t, "PUT", "/"+index+"/_doc/large", body)
	if status != 201 || len(body) != 2<<20 {
		t.Fatal("Search legal 2 MiB source fixture failed", status, len(body))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	config := search.Config{Store: "records", URL: backend.URL, Pool: 4, MaxReadSize: protocol.MaxDocument}
	adapter, err := search.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	call := routeBackendRead(index + "/s:large")
	events := routeBackendEvents(t, client, call)
	if len(events) != 1 || !bytes.Equal(events[0].GetResult().GetRead().GetDocument().GetData(), []byte(body)) {
		var failure *pb.Failure
		if len(events) > 0 {
			failure = events[0].GetResult().GetRead().GetFailure()
		}
		t.Fatal("real Search source at record size limit failed Route transport", len(events), failure)
	}
	assertRouteAcceptanceIdle(t, nodes)

	for _, mode := range []string{"record", "native"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			var proxyFailure atomic.Value
			proxy := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, inbound *http.Request) {
				request, err := http.NewRequestWithContext(inbound.Context(), inbound.Method, backend.URL+inbound.URL.RequestURI(), inbound.Body)
				if err != nil {
					writer.WriteHeader(502)
					return
				}
				request.Header = inbound.Header.Clone()
				request.GetBody = nil
				response, err := backend.Client.Do(request)
				if err != nil {
					proxyFailure.Store(err.Error())
					writer.WriteHeader(502)
					return
				}
				defer response.Body.Close()
				if strings.HasSuffix(inbound.URL.Path, "/_bulk") {
					calls.Add(1)
					_, _ = io.Copy(io.Discard, response.Body)
					connection, _, err := writer.(http.Hijacker).Hijack()
					if err == nil {
						_ = connection.Close()
					}
					return
				}
				for key, values := range response.Header {
					for _, value := range values {
						writer.Header().Add(key, value)
					}
				}
				writer.WriteHeader(response.StatusCode)
				_, _ = io.Copy(writer, response.Body)
			}))
			t.Cleanup(proxy.Close)
			config := search.Config{Store: "records", URL: proxy.URL, Pool: 1}
			adapter, err := search.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			nodes, client := routeBackendServer(t, adapter)
			id := "lost_" + mode
			call := routeBackendMutation(backend.Index+"/s:"+id, "put", "application/json", []byte(`{"n":1}`))
			if mode == "native" {
				httpCall := &searchpb.Request{Method: "POST", Path: "/_bulk"}
				rawDescriptor, _ := proto.Marshal(httpCall)
				descriptor := &pb.Document{MediaType: search.NativeDescriptor, Data: rawDescriptor}
				open := &pb.NativeOpen{Resource: backend.Index, Descriptor_: descriptor, BodyMediaType: "application/x-ndjson"}
				body := []byte(fmt.Sprintf("{\"create\":{\"_id\":%q}}\n{\"n\":1}\n", id))
				native := &pb.NativeCall{Open: open, Body: body}
				value := &pb.Call_Native{Native: native}
				call = &pb.Call{Version: 1, Operation: value}
			}
			events := routeBackendEvents(t, client, call)
			if mode == "record" {
				if len(events) != 1 || events[0].GetResult().GetMutation().GetOutcome() != pb.MutationOutcome_UNKNOWN {
					t.Fatal("Search lost ACK was not UNKNOWN", events)
				}
			} else if len(events) == 0 || events[len(events)-1].GetNativeEnd().GetCompletion() != pb.NativeCompletion_RESPONSE_INCOMPLETE {
				t.Fatal("Search lost native response was not incomplete", events, "proxy failure:", proxyFailure.Load())
			}
			status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+id, "")
			if status != 200 || !bytes.Contains(raw, []byte(`"n":1`)) || calls.Load() != 1 {
				t.Fatal("lost reply was replayed or applied write was not independently observed", status, calls.Load())
			}
			assertRouteAcceptanceIdle(t, nodes)
		})
	}
}

func TestRouteMongoPerformance(t *testing.T) {
	if os.Getenv("WEIR_ROUTE_PERFORMANCE") != "1" {
		t.Skip("explicit performance run: WEIR_INTEGRATION=1 WEIR_ROUTE_PERFORMANCE=1 go test -tags integration ./internal/server -run '^TestRouteMongoPerformance$' -count=1 -v")
	}
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for i := 0; i < 256; i++ {
		document := bson.D{{Key: "_id", Value: fmt.Sprintf("%d", i)}, {Key: "n", Value: int32(i)}, {Key: "pad", Value: strings.Repeat("x", 1024)}}
		if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, document); err != nil {
			t.Fatal(err)
		}
	}
	config := mongodb.Config{Store: "records", URI: backend.URI, Pool: 4, MaxReadSize: protocol.MaxDocument}
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	const activeRPCs, recordsPerRPC = 4, 10000
	limits := store.DefaultLimits()
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)
	baselineHeap := mem.HeapAlloc
	process := &routeMemoryProcess{address: routeMemoryAddress{PID: os.Getpid()}}
	var mongoStatus struct {
		PID int `bson:"pid"`
	}
	serverStatus := bson.D{{Key: "serverStatus", Value: int32(1)}}
	if err := backend.Admin.Database("admin").RunCommand(ctx, serverStatus).Decode(&mongoStatus); err != nil || mongoStatus.PID <= 0 {
		t.Fatal("cannot attribute owned MongoDB process CPU and RSS", err)
	}
	mongoProcess := &routeMemoryProcess{address: routeMemoryAddress{PID: mongoStatus.PID}}
	processes := []*routeMemoryProcess{process, mongoProcess}
	baselineOS, err := routeMemoryOSSample(processes)
	if err != nil {
		t.Fatal(err)
	}
	peakHeap, peakRSS := mem.HeapAlloc, uint64(baselineOS[os.Getpid()][0])
	backendPeakRSS := uint64(baselineOS[mongoStatus.PID][0])
	var latenciesMu sync.Mutex
	latencies := make([]float64, 0, activeRPCs*recordsPerRPC)
	var failed atomic.Int64
	var workers sync.WaitGroup
	errors := make(chan error, activeRPCs)
	started := time.Now()
	for worker := 0; worker < activeRPCs; worker++ {
		worker := worker
		workers.Go(func() {
			produced := 0
			var timesMu sync.Mutex
			times := make(map[uint64]time.Time)
			opts := weirclient.Options{StoreName: "records"}
			opts.Produce = func(context.Context) (*pb.Call, error) {
				if produced == recordsPerRPC {
					return nil, io.EOF
				}
				produced++
				timesMu.Lock()
				times[uint64(produced)] = time.Now()
				timesMu.Unlock()
				return routeBackendRead(fmt.Sprintf("%s/records/s:%d", backend.DB, (produced+worker*53)%256)), nil
			}
			opts.Consume = func(_ context.Context, id uint64, event *pb.Event) error {
				result := event.GetResult().GetRead()
				if result.GetDocument() == nil || result.GetFailure() != nil || bson.Raw(result.GetDocument().GetData()).Lookup("n").Int32() != int32((int(id)+worker*53)%256) {
					failed.Add(1)
					return fmt.Errorf("performance backend read failed: %v", result)
				}
				timesMu.Lock()
				begin := times[id]
				delete(times, id)
				timesMu.Unlock()
				latenciesMu.Lock()
				latencies = append(latencies, float64(time.Since(begin))/float64(time.Millisecond))
				latenciesMu.Unlock()
				return nil
			}
			errors <- weirclient.Execute(ctx, client, opts)
		})
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(done) }()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	running := true
	for running {
		select {
		case <-done:
			running = false
		case <-ticker.C:
			runtime.ReadMemStats(&mem)
			peakHeap = max(peakHeap, mem.HeapAlloc)
			values, err := routeMemoryOSSample(processes)
			if err != nil {
				cancel()
				<-done
				t.Fatal(err)
			}
			peakRSS = max(peakRSS, uint64(values[os.Getpid()][0]))
			backendPeakRSS = max(backendPeakRSS, uint64(values[mongoStatus.PID][0]))
		}
	}
	seconds := time.Since(started).Seconds()
	for i := 0; i < activeRPCs; i++ {
		if err := <-errors; err != nil {
			failed.Add(1)
			t.Error(err)
		}
	}
	assertRouteAcceptanceIdle(t, nodes)
	if len(latencies) != activeRPCs*recordsPerRPC || failed.Load() != 0 {
		t.Fatal("performance test did not complete its comparable workload", len(latencies), failed.Load())
	}
	finalOS, err := routeMemoryOSSample(processes)
	if err != nil {
		t.Fatal(err)
	}
	sort.Float64s(latencies)
	result := map[string]any{
		"backend": "MongoDB 8.0.32 loopback replica set, majority write concern, primary reads", "operation": "point reads of 256 preloaded BSON records, each 1 KiB pad", "go": runtime.Version(), "platform": runtime.GOOS + "/" + runtime.GOARCH,
		"active_rpcs": activeRPCs, "records_per_rpc": recordsPerRPC, "total_operations": len(latencies), "seconds": seconds, "operations_per_second": float64(len(latencies)) / seconds,
		"latency_ms_p50": latencies[len(latencies)/2], "latency_ms_p95": latencies[len(latencies)*95/100], "latency_ms_max": latencies[len(latencies)-1], "latency_definition": "Produce callback to complete business Event callback; includes client admission, excludes final RPC drain",
		"failures": failed.Load(), "batch_operations": limits.BatchOperations, "batch_bytes": limits.BatchBytes, "batch_result_bytes": limits.BatchResultBytes, "collect_ns": limits.Collect.Nanoseconds(), "backend_concurrency": limits.Concurrency,
		"cpu_seconds_client_and_router": finalOS[os.Getpid()][1] - baselineOS[os.Getpid()][1], "baseline_heap_alloc": baselineHeap, "peak_heap_alloc": peakHeap, "peak_rss": peakRSS,
		"backend_cpu_seconds": finalOS[mongoStatus.PID][1] - baselineOS[mongoStatus.PID][1], "backend_peak_rss": backendPeakRSS,
		"measurement_scope": "client and local router share one process; MongoDB CPU/RSS measured separately; no speed comparison to old RPC or unbatched direct calls",
	}
	raw, _ := json.MarshalIndent(result, "", "  ")
	t.Log("ROUTE_PERFORMANCE=" + string(raw))
	if artifact := os.Getenv("WEIR_ROUTE_PERFORMANCE_REPORT"); artifact != "" {
		if err := os.WriteFile(artifact, raw, 0644); err != nil {
			t.Fatal(err)
		}
	}
}
