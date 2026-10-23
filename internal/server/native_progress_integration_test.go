//go:build integration

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/backend/search"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"github.com/batchstream/weir/internal/testutil/testsearch"
	"go.mongodb.org/mongo-driver/v2/bson"
	"golang.org/x/net/http2"
)

type nativeProgressFixture struct {
	adapter       execution.Adapter
	resource      string
	native        *pb.NativeRequest
	stalledNative *pb.NativeRequest
}

func TestMongoSlowNativeOutputAllowsHealthyQueries(t *testing.T) {
	fixture := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	document := bson.D{{Key: "_id", Value: "probe"}, {Key: "n", Value: int64(1)}, {Key: "pad", Value: strings.Repeat("x", 2048)}}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(ctx, document); err != nil {
		t.Fatal(err)
	}
	filter := bson.D{{Key: "_id", Value: "probe"}}
	command := bson.D{{Key: "find", Value: "records"}, {Key: "filter", Value: filter}, {Key: "limit", Value: 1}, {Key: "singleBatch", Value: true}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	request := &pb.Document{ContentType: "application/bson", Data: raw}
	native := &pb.NativeRequest{Resource: fixture.DB, Request: request}
	invalidDocument := &pb.Document{ContentType: "application/bson", Data: []byte{1}}
	invalid := &pb.NativeRequest{Resource: fixture.DB, Request: invalidDocument}
	cases := []struct {
		name    string
		stalled *pb.NativeRequest
	}{{name: "valid_response", stalled: native}, {name: "early_invalid_response", stalled: invalid}}
	for _, scenario := range cases {
		t.Run(scenario.name, func(t *testing.T) {
			config := mongodb.Config{Store: "records", URI: fixture.URI}
			adapter, err := mongodb.Open(ctx, config)
			if err != nil {
				t.Fatal(err)
			}
			probe := nativeProgressFixture{adapter: adapter, resource: fixture.DB + "/records/s:probe", native: native, stalledNative: scenario.stalled}
			verifyNativeProgress(t, probe)
		})
	}
}

func TestSearchSlowNativeOutputAllowsHealthyQueries(t *testing.T) {
	fixture := testsearch.Open(t)
	body := `{"n":1,"pad":"` + strings.Repeat("x", 2048) + `"}`
	status, _ := fixture.Do(t, http.MethodPut, "/"+fixture.Index+"/_doc/probe", body)
	if status != http.StatusCreated {
		t.Fatal("failed to seed owned Search record", status)
	}
	request, err := http.NewRequest(http.MethodGet, "http://weir.invalid/"+fixture.Index+"/_doc/probe", nil)
	if err != nil {
		t.Fatal(err)
	}
	native, err := weirclient.NewHTTPNativeRequest("", request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	config := search.Config{Store: "records", URL: fixture.URL}
	adapter, err := search.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	probe := nativeProgressFixture{adapter: adapter, resource: fixture.Index + "/s:probe", native: native}
	verifyNativeProgress(t, probe)
}

func verifyNativeProgress(t *testing.T, fixture nativeProgressFixture) {
	t.Helper()
	options := routeAcceptanceNodeOptions{adapter: fixture.adapter, limits: DefaultLimits()}
	node := startRouteAcceptanceNode(t, options)
	conn, err := net.DialTimeout("tcp", node.address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, http2.ClientPreface); err != nil {
		t.Fatal(err)
	}
	framer := http2.NewFramer(conn, conn)
	setting := http2.Setting{ID: http2.SettingInitialWindowSize, Val: 0}
	if err := framer.WriteSettings(setting); err != nil {
		t.Fatal(err)
	}
	stalledNative := fixture.stalledNative
	if stalledNative == nil {
		stalledNative = fixture.native
	}
	variant := &pb.Command_Native{Native: stalledNative}
	command := &pb.Command{Operation: variant}
	const stalled = 32
	for index := range stalled {
		request := flowControlledRequest{framer: framer, address: node.address, streamID: uint32(index*2 + 1), command: command}
		sendFlowControlledRequest(t, request)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	// Small replies fit in each stream's transport buffer, so the stalled
	// Natives finish and release their execution slots.
	for testmetrics.Sum(testmetrics.Gather(t, node.runtime), "weir_store_native_completions_total") < stalled {
		if ctx.Err() != nil {
			t.Fatal("slow Native replies retained execution capacity", node.runtime.Stats())
		}
		time.Sleep(time.Millisecond)
	}
	client := routeAcceptanceClient(t, node.address)
	read := &weirclient.ReadRequest{Resource: fixture.resource}
	readOptions := weirclient.ReadOneOptions{StoreName: "records", Request: read}
	started := time.Now()
	result, err := weirclient.ReadOne(ctx, client, readOptions)
	if err != nil || result == nil || result.Failure != nil || result.Document == nil {
		t.Fatal("healthy typed query waited behind Native output", result, err)
	}
	if err := verifyProgressDocument(result.Document); err != nil {
		t.Fatal(err)
	}
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 64<<10 {
			return fmt.Errorf("small Native query exceeded probe response bound")
		}
		body = append(body, chunk...)
		return nil
	}
	nativeOptions := weirclient.NativeOptions{StoreName: "records", Request: fixture.native, Consume: consume}
	nativeResult, err := weirclient.Native(ctx, client, nativeOptions)
	if err != nil || nativeResult == nil || nativeResult.Failure != nil || nativeResult.Completion != weirclient.NativeResponseComplete {
		t.Fatal("healthy Native query waited behind Native output", nativeResult, err)
	}
	if fixture.native.Request.ContentType == "application/bson" {
		var reply struct {
			OK     float64 `bson:"ok"`
			Cursor struct {
				FirstBatch []bson.Raw `bson:"firstBatch"`
			} `bson:"cursor"`
		}
		if err := bson.Unmarshal(body, &reply); err != nil || reply.OK != 1 || len(reply.Cursor.FirstBatch) != 1 {
			t.Fatal("Native Mongo query failed", err, reply.OK, len(reply.Cursor.FirstBatch))
		}
		document := &pb.Document{ContentType: "application/bson", Data: reply.Cursor.FirstBatch[0]}
		if err := verifyProgressDocument(document); err != nil {
			t.Fatal(err)
		}
	} else {
		head, err := weirclient.ParseHTTPNativeResponse(nativeResult.Response)
		if err != nil || head.StatusCode != http.StatusOK {
			t.Fatal("Native Search query failed", head, err)
		}
		var reply struct {
			Found  bool            `json:"found"`
			Source json.RawMessage `json:"_source"`
		}
		if err := json.Unmarshal(body, &reply); err != nil || !reply.Found {
			t.Fatal("Native Search query did not find owned record", err)
		}
		document := &pb.Document{ContentType: "application/json", Data: reply.Source}
		if err := verifyProgressDocument(document); err != nil {
			t.Fatal(err)
		}
	}
	watchdogs := testmetrics.Sum(testmetrics.Gather(t, node.server.metrics.watchdogs), "weir_watchdog_expirations_total")
	if watchdogs != 0 {
		t.Fatal("healthy queries depended on closing slow Native streams", watchdogs)
	}
	t.Logf("backend=%s slow_Native=%d healthy_typed_and_Native=%s; default Stall=%s; no watchdog release", fixture.native.Request.ContentType, stalled, time.Since(started), DefaultLimits().Stall)
	_ = conn.Close()
	assertRouteAcceptanceIdle(t, []*routeAcceptanceNode{node})
}

func verifyProgressDocument(document *pb.Document) error {
	var actual struct {
		N   int64  `json:"n" bson:"n"`
		Pad string `json:"pad" bson:"pad"`
	}
	var err error
	if document.ContentType == "application/bson" {
		err = bson.Unmarshal(document.Data, &actual)
	} else {
		err = json.Unmarshal(document.Data, &actual)
	}
	if err != nil {
		return err
	}
	if actual.N != 1 || actual.Pad != strings.Repeat("x", 2048) {
		return fmt.Errorf("point query changed the expected document")
	}
	return nil
}
