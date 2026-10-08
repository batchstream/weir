//go:build integration

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil"
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

func routeBackendEvents(t *testing.T, client pb.StoreServiceClient, command *pb.Command) []*pb.Event {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := testutil.ExecuteEvents(ctx, client, "records", command)
	if err != nil {
		t.Fatal(err)
	}
	var events []*pb.Event
	total := 0
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatal(err)
		}
		total += proto.Size(event)
		if len(events) >= 100 || total > 32<<20 {
			t.Fatal("test event capture exceeded")
		}
		events = append(events, event)
	}

}

func routeBackendMutation(key, kind, media string, raw []byte) *pb.MutateRequest {
	document := &pb.Document{ContentType: media, Data: raw}
	mutation := &pb.MutateRequest{Resource: key}
	if kind == "create" {
		mutation.Action = &pb.MutateRequest_Create{Create: document}
	} else {
		mutation.Action = &pb.MutateRequest_Put{Put: document}
	}
	return mutation
}

func TestRouteMongo2MiBRecordLuaScanAndPartialBatch(t *testing.T) {
	backend := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	config := mongodb.Config{Store: "records", URI: backend.URI}
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	base := bson.D{{Key: "_id", Value: "large"}, {Key: "blob", Value: []byte{}}}
	raw, _ := bson.Marshal(base)
	base[1].Value = bytes.Repeat([]byte{59}, (2<<20)-len(raw))
	raw, _ = bson.Marshal(base)
	if len(raw) != 2<<20 {
		t.Fatal("fixture size", len(raw))
	}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, bson.Raw(raw)); err != nil {
		t.Fatal(err)
	}
	read := &pb.ReadRequest{Resource: backend.DB + "/records/s:large"}
	batch := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Read{Read: read}}}

	response, err := testutil.ReadRecords(ctx, client, batch.StoreName, []*pb.ReadRequest{batch.Command.GetRead()})
	if err != nil || !bytes.Equal(response[0].GetDocument().GetData(), raw) {
		t.Fatal("large read corrupted", response, err)
	}
	counter := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(0)}}
	if _, err := backend.Admin.Database(backend.DB).Collection("records").InsertOne(ctx, counter); err != nil {
		t.Fatal(err)
	}
	program := &pb.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
	form := &pb.Transform_Lua{Lua: program}
	transform := &pb.Transform{Form: form}
	action := &pb.MutateRequest_AtomicTransform{AtomicTransform: transform}
	mutation := &pb.MutateRequest{Resource: backend.DB + "/records/s:counter", Action: action}
	mutations := make([]*pb.MutateRequest, 12)
	for i := range mutations {
		mutations[i] = mutation
	}

	result, err := testutil.MutateRecords(ctx, client, "records", mutations)
	if err != nil {
		t.Fatal(err)
	}
	for _, reply := range result {
		if reply.Outcome != pb.MutationOutcome_APPLIED || reply.Failure != nil {
			t.Fatal(reply)
		}
	}
	var observed struct{ N int32 }
	filter := bson.D{{Key: "_id", Value: "counter"}}
	if err := backend.Admin.Database(backend.DB).Collection("records").FindOne(ctx, filter).Decode(&observed); err != nil || observed.N != 12 {
		t.Fatal("ordered Lua increment lost", observed.N, err)
	}
	selector := filter
	selectorRaw, _ := bson.Marshal(selector)
	document := &pb.Document{ContentType: "application/bson", Data: selectorRaw}
	scan := &pb.ScanRequest{Resource: backend.DB + "/records", Filter: document}
	scanValue := &pb.Command_Scan{Scan: scan}
	scanCommand := &pb.Command{Operation: scanValue}
	events := routeBackendEvents(t, client, scanCommand)
	if len(events) != 2 || events[1].GetScanEnd().Failure != nil {
		t.Fatal("Scan failed", events)
	}
	newDoc := bson.D{{Key: "_id", Value: "new"}, {Key: "n", Value: 1}}
	rawNew, _ := bson.Marshal(newDoc)
	rawCounter, _ := bson.Marshal(counter)
	requests := []*pb.MutateRequest{routeBackendMutation(backend.DB+"/records/s:counter", "create", "application/bson", rawCounter), routeBackendMutation(backend.DB+"/records/s:new", "put", "application/bson", rawNew)}

	result, err = testutil.MutateRecords(ctx, client, "records", requests)
	if err != nil || result[0].Outcome != pb.MutationOutcome_NOT_APPLIED || result[1].Outcome != pb.MutationOutcome_APPLIED {
		t.Fatal("partial evidence lost", result, err)
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
			config := mongodb.Config{Store: "records", URI: proxy.URI()}
			adapter, err := mongodb.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			nodes, client := routeBackendServer(t, adapter)
			document := bson.D{{Key: "_id", Value: "lost"}, {Key: "n", Value: int32(1)}}
			raw, _ := bson.Marshal(document)
			mutation := routeBackendMutation(backend.DB+"/records/s:lost", "put", "application/bson", raw)
			var call *pb.Command
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
				nativeBody := &pb.Document{ContentType: "application/bson", Data: []byte{5, 0, 0, 0, 0}}
				open := &pb.NativeRequest{Resource: backend.DB + "/records", Request: nativeBody}
				open.Request.Data = body
				native := open
				value := &pb.Command_Native{Native: native}
				call = &pb.Command{Operation: value}
			}
			if mode == "record" {
				batch := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Mutate{Mutate: mutation}}}

				response, err := testutil.MutateRecords(ctx, client, batch.StoreName, []*pb.MutateRequest{batch.Command.GetMutate()})
				if err != nil || response[0].Outcome != pb.MutationOutcome_UNKNOWN {
					t.Fatal("lost write ACK not indeterminate", response, err)
				}
			} else {
				events := routeBackendEvents(t, client, call)
				if len(events) == 0 || events[len(events)-1].GetNativeEnd().Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE {
					t.Fatal("native reply complete", events)
				}
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
	config := search.Config{Store: "records", URL: backend.URL}
	adapter, err := search.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	nodes, client := routeBackendServer(t, adapter)
	read := &pb.ReadRequest{Resource: index + "/s:large"}
	batch := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Read{Read: read}}}

	response, err := testutil.ReadRecords(ctx, client, batch.StoreName, []*pb.ReadRequest{batch.Command.GetRead()})
	if err != nil || !bytes.Equal(response[0].GetDocument().GetData(), []byte(body)) {
		t.Fatal("Search legal source failed", response, err)
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
			config := search.Config{Store: "records", URL: proxy.URL}
			adapter, err := search.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			nodes, client := routeBackendServer(t, adapter)
			id := "lost_" + mode
			mutation := routeBackendMutation(backend.Index+"/s:"+id, "put", "application/json", []byte(`{"n":1}`))
			var call *pb.Command
			if mode == "native" {
				body := []byte(fmt.Sprintf("{\"create\":{\"_id\":%q}}\n{\"n\":1}\n", id))
				httpCall, err := http.NewRequest(http.MethodPost, "http://ignored.invalid/_bulk", bytes.NewReader(body))
				if err != nil {
					t.Fatal(err)
				}
				httpCall.Header.Set("Content-Type", "application/x-ndjson")
				open, err := weirclient.NewHTTPNativeRequest(backend.Index, httpCall)
				if err != nil {
					t.Fatal(err)
				}
				native := open
				value := &pb.Command_Native{Native: native}
				call = &pb.Command{Operation: value}
			}
			if mode == "record" {
				batch := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: &pb.Command{Operation: &pb.Command_Mutate{Mutate: mutation}}}

				response, err := testutil.MutateRecords(ctx, client, batch.StoreName, []*pb.MutateRequest{batch.Command.GetMutate()})
				if err != nil || response[0].Outcome != pb.MutationOutcome_UNKNOWN {
					t.Fatal("lost write ACK not UNKNOWN", response, err)
				}
			} else {
				events := routeBackendEvents(t, client, call)
				if len(events) == 0 || events[len(events)-1].GetNativeEnd().Completion != pb.NativeCompletion_RESPONSE_INCOMPLETE {
					t.Fatal("native reply complete", events, proxyFailure.Load())
				}
			}
			status, raw := backend.Do(t, "GET", "/"+backend.Index+"/_doc/"+id, "")
			if status != 200 || !bytes.Contains(raw, []byte(`"n":1`)) || calls.Load() != 1 {
				t.Fatal("lost reply was replayed or applied write was not independently observed", status, calls.Load())
			}
			assertRouteAcceptanceIdle(t, nodes)
		})
	}
}
