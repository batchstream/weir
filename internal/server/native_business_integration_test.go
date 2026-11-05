//go:build integration

package server

import (
	"bytes"
	"context"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/backend"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"google.golang.org/grpc/metadata"
)

func sdkClient(t *testing.T, address string) *weirclient.Client {
	t.Helper()
	client, err := weirclient.New(address)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// ownStream gives a context's calls a gRPC stream of their own instead of a shared
// one, so that a handler returning proves the stream ended normally after half-close
// rather than on the Client's cancellation.
func ownStream(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "weir-test", "own-stream")
}

type sdkMongoCommand struct {
	client   *weirclient.Client
	database string
	command  bson.D
}

func executeSDKMongoCommand(t *testing.T, opts sdkMongoCommand) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(opts.command)
	if err != nil {
		t.Fatal(err)
	}
	document := &weirclient.Document{ContentType: "application/bson", Data: raw}
	request := &weirclient.NativeRequest{Resource: opts.database, Request: document}
	var output bytes.Buffer
	consume := func(_ context.Context, _ *weirclient.NativeResponse, data []byte) error {
		_, err := output.Write(data)
		return err
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	result, err := opts.client.Native(ctx, "records", request, weirclient.WithChunks(consume))
	if err != nil || result == nil || result.Completion != weirclient.NativeResponseComplete || result.Failure != nil || result.Response == nil || result.Response.BodyContentType != "application/bson" {
		t.Fatal("SDK Native did not complete", result, err)
	}
	reply := bson.Raw(output.Bytes())
	if err := reply.Validate(); err != nil {
		t.Fatal("SDK Native lost BSON response bytes", err)
	}
	return reply
}

func TestSDKMongoLargeNativeCursorAndTypedRecords(t *testing.T) {
	fixture := testmongo.Open(t)
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	options := backend.DefaultOptions()
	options.ExchangeBytes = 64 << 10
	config := mongodb.Config{Store: "records", URI: fixture.URI, Options: &options}
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	window := store.DefaultLimits()
	window.QueueBytes, window.BatchBytes = 64<<10, 64<<10
	nodeOptions := routeAcceptanceNodeOptions{adapter: adapter, store: window}
	node := startRouteAcceptanceNode(t, nodeOptions)
	client := sdkClient(t, node.address)
	blob := bytes.Repeat([]byte("x"), 5<<20)
	first := bson.D{{Key: "_id", Value: "first"}, {Key: "blob", Value: blob}}
	second := bson.D{{Key: "_id", Value: "second"}, {Key: "blob", Value: blob}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	insert := bson.D{{Key: "insert", Value: "records"}, {Key: "documents", Value: bson.A{first, second}}, {Key: "writeConcern", Value: concern}}
	commandOptions := sdkMongoCommand{client: client, database: fixture.DB, command: insert}
	reply := executeSDKMongoCommand(t, commandOptions)
	if reply.Lookup("n").Int32() != 2 {
		t.Fatal("large Native insert did not apply", reply.Lookup("n"))
	}
	order := bson.D{{Key: "_id", Value: 1}}
	find := bson.D{{Key: "find", Value: "records"}, {Key: "sort", Value: order}, {Key: "batchSize", Value: 2}}
	commandOptions.command = find
	reply = executeSDKMongoCommand(t, commandOptions)
	values, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || len(values) != 2 || len(reply) <= 9<<20 {
		t.Fatal("large Native page was truncated", len(reply), len(values), err)
	}
	find[len(find)-1].Value = 1
	commandOptions.command = find
	reply = executeSDKMongoCommand(t, commandOptions)
	cursor := reply.Lookup("cursor", "id").Int64()
	seen, err := reply.Lookup("cursor", "firstBatch").Array().Values()
	if err != nil || cursor == 0 {
		t.Fatal("SDK cursor page lost state", cursor, err)
	}
	for cursor != 0 {
		getMore := bson.D{{Key: "getMore", Value: cursor}, {Key: "collection", Value: "records"}, {Key: "batchSize", Value: 1}}
		commandOptions.command = getMore
		reply = executeSDKMongoCommand(t, commandOptions)
		page, err := reply.Lookup("cursor", "nextBatch").Array().Values()
		if err != nil {
			t.Fatal(err)
		}
		seen = append(seen, page...)
		cursor = reply.Lookup("cursor", "id").Int64()
	}
	if len(seen) != 2 || seen[0].Document().Lookup("_id").StringValue() != "first" || seen[1].Document().Lookup("_id").StringValue() != "second" {
		t.Fatal("SDK cursor lost sort order or records", len(seen))
	}
	raw, err := bson.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	document := &weirclient.Document{ContentType: "application/bson", Data: raw}
	write := &weirclient.MutateRequest{Resource: fixture.DB + "/records/s:first", Action: weirclient.MutationPut, Document: document}
	calls := ownStream(ctx)
	results, err := client.Mutate(calls, "records", weirclient.WithRequests(write))
	if err != nil || results[0] == nil || results[0].Outcome != weirclient.MutationApplied || results[0].Failure != nil {
		t.Fatal("large typed write was rejected by processing windows", results[0], err)
	}
	read := &weirclient.ReadRequest{Resource: write.Resource}
	reads, err := client.Read(calls, "records", weirclient.WithRequests(read))
	if err != nil || reads[0] == nil || reads[0].Document == nil || !bytes.Equal(reads[0].Document.Data, raw) {
		t.Fatal("large typed read changed BSON bytes", err)
	}
	nodes := []*routeAcceptanceNode{node}
	assertRouteAcceptanceIdle(t, nodes)
}
