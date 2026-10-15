//go:build integration

package server

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir/internal/backend/mongodb"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type valueReadCommand struct {
	name  string
	items int
}

type valueReadObserver struct {
	mu       sync.Mutex
	commands []valueReadCommand
}

func (observer *valueReadObserver) started(_ context.Context, command *event.CommandStartedEvent) {
	if command.CommandName != "find" && command.CommandName != "getMore" && command.CommandName != "listCollections" {
		return
	}
	items := 1
	if filter, ok := command.Command.Lookup("filter").DocumentOK(); ok {
		if id, ok := filter.Lookup("_id").DocumentOK(); ok {
			if in, ok := id.Lookup("$in").ArrayOK(); ok {
				values, err := in.Values()
				if err == nil {
					items = len(values)
				}
			}
		}
	}
	observed := valueReadCommand{name: command.CommandName, items: items}
	observer.mu.Lock()
	observer.commands = append(observer.commands, observed)
	observer.mu.Unlock()
}

func (observer *valueReadObserver) snapshot() []valueReadCommand {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return slices.Clone(observer.commands)
}

type valueReadFixture struct {
	collection *mongo.Collection
	client     *weirclient.Client
	observer   *valueReadObserver
	resource   string
	documents  []bson.Raw
}

func openValueReadFixture(t *testing.T) *valueReadFixture {
	t.Helper()
	fixture := testmongo.Open(t)
	observer := &valueReadObserver{}
	proxy := testmongo.StartProxy(t, fixture)
	proxy.Monitor = &event.CommandMonitor{Started: observer.started}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	directOptions := options.Client().ApplyURI(proxy.URI()).SetRetryReads(false).SetRetryWrites(false).
		SetMaxAdaptiveRetries(0).SetMaxPoolSize(0).SetMaxConnecting(2)
	direct, err := mongo.Connect(directOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := direct.Disconnect(cleanup); err != nil {
			t.Error(err)
		}
	})
	config := mongodb.Config{Store: "records", URI: proxy.URI()}
	adapter, err := mongodb.Open(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	local, err := store.New(adapter, store.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := local.Close(cleanup); err != nil {
			t.Error(err)
		}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	directoryConfig := directory.Config{Group: "value-proof", Stores: []string{"records"}, Targets: []string{listener.Addr().String()}}
	discovery, err := directory.New(directoryConfig)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = discovery.Close(t.Context()) })
	serverOptions := peerServerOptions{stores: map[string]*store.Runtime{"records": local}, directory: discovery, listener: listener}
	server, address := startPeerServer(t, serverOptions)
	clientOptions := weirclient.OpenOptions{Seed: address, Stores: []string{"records"}}
	client, err := weirclient.Open(ctx, clientOptions)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
		waitPeerIdle(t, server)
	})
	documents := make([]bson.Raw, 512)
	seed := make([]any, len(documents))
	for index := range documents {
		document := bson.D{{Key: "_id", Value: fmt.Sprint(index)}, {Key: "value", Value: int32(index)}, {Key: "payload", Value: strings.Repeat("x", 256)}}
		raw, err := bson.Marshal(document)
		if err != nil {
			t.Fatal(err)
		}
		documents[index] = raw
		seed[index] = raw
	}
	if _, err := fixture.Admin.Database(fixture.DB).Collection("records").InsertMany(ctx, seed); err != nil {
		t.Fatal(err)
	}
	valueFixture := &valueReadFixture{collection: direct.Database(fixture.DB).Collection("records"), client: client, observer: observer, resource: fixture.DB + "/records/s:", documents: documents}
	return valueFixture
}

type valueReadCase struct {
	path        string
	batch       int
	concurrency int
}

func (fixture *valueReadFixture) read(ctx context.Context, scenario valueReadCase, indexes []int) error {
	if scenario.path == "weir" {
		requests := make([]*weirclient.ReadRequest, len(indexes))
		for position, index := range indexes {
			requests[position] = &weirclient.ReadRequest{Resource: fixture.resource + fmt.Sprint(index)}
		}
		readOptions := weirclient.ReadOptions{StoreName: "records", Requests: requests}
		results, err := fixture.client.Read(ctx, readOptions)
		if err != nil {
			return err
		}
		if len(results) != len(indexes) {
			return fmt.Errorf("received %d results for %d records", len(results), len(indexes))
		}
		for position, result := range results {
			if result.Failure != nil || result.Document == nil || result.Document.ContentType != "application/bson" || !bytes.Equal(result.Document.Data, fixture.documents[indexes[position]]) {
				return fmt.Errorf("SDK result changed or lost input order at %d: %v", position, result)
			}
		}
		return nil
	}
	if len(indexes) == 1 {
		index := indexes[0]
		filter := bson.D{{Key: "_id", Value: fmt.Sprint(index)}}
		raw, err := fixture.collection.FindOne(ctx, filter).Raw()
		if err != nil {
			return err
		}
		if !bytes.Equal(raw, fixture.documents[index]) {
			return fmt.Errorf("direct result changed for %d", index)
		}
		return nil
	}
	ids := make([]string, len(indexes))
	want := make(map[string]bson.Raw, len(indexes))
	for position, index := range indexes {
		ids[position] = fmt.Sprint(index)
		want[ids[position]] = fixture.documents[index]
	}
	in := bson.D{{Key: "$in", Value: ids}}
	filter := bson.D{{Key: "_id", Value: in}}
	findOptions := options.Find().SetLimit(int64(len(ids))).SetBatchSize(int32(len(ids)))
	cursor, err := fixture.collection.Find(ctx, filter, findOptions)
	if err != nil {
		return err
	}
	defer cursor.Close(ctx)
	for cursor.Next(ctx) {
		id := cursor.Current.Lookup("_id").StringValue()
		if !bytes.Equal(cursor.Current, want[id]) {
			return fmt.Errorf("direct batch returned an unexpected document %q", id)
		}
		delete(want, id)
	}
	if err := cursor.Err(); err != nil {
		return err
	}
	if len(want) != 0 {
		return fmt.Errorf("direct batch lost %d documents", len(want))
	}
	return nil
}

// TestValueProofMongoReadPaths measures real commands without pausing the
// scheduler, delaying the database, or asserting a performance winner. It is a
// finite local experiment; it does not establish database saturation or capacity.
func TestValueProofMongoReadPaths(t *testing.T) {
	fixture := openValueReadFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()
	cases := []valueReadCase{
		{path: "direct", batch: 1, concurrency: 1},
		{path: "weir", batch: 1, concurrency: 1},
		{path: "direct", batch: 1, concurrency: 32},
		{path: "weir", batch: 1, concurrency: 32},
		{path: "direct", batch: 1, concurrency: 128},
		{path: "weir", batch: 1, concurrency: 128},
		{path: "direct", batch: 32, concurrency: 1},
		{path: "weir", batch: 32, concurrency: 1},
	}
	// Run each complete workload outside samples to warm both independent pools
	// at the measured concurrency and populate the MongoDB target cache.
	for _, scenario := range cases {
		_, err := runValueReadCase(ctx, fixture, scenario)
		if err != nil {
			t.Fatal(err)
		}
	}
	for round := range 3 {
		order := slices.Clone(cases)
		if round%2 != 0 {
			slices.Reverse(order)
		}
		for _, scenario := range order {
			before := len(fixture.observer.snapshot())
			elapsed, err := runValueReadCase(ctx, fixture, scenario)
			if err != nil {
				t.Fatal(err)
			}
			commands := fixture.observer.snapshot()[before:]
			histogram := make(map[int]int)
			other := make(map[string]int)
			for _, command := range commands {
				if command.name == "find" {
					histogram[command.items]++
				} else {
					other[command.name]++
				}
			}
			if len(commands) == 0 || len(commands) > len(fixture.documents) {
				t.Fatal("unexpected physical command count", commands)
			}
			if scenario.path == "direct" && len(commands) != len(fixture.documents)/scenario.batch {
				t.Fatal("direct baseline did not issue its declared native batches", histogram, other)
			}
			if len(other) != 0 {
				t.Fatal("warm sample contains unaccounted metadata or cursor work", other)
			}
			t.Logf("round=%d path=%s request_records=%d concurrency=%d records=%d backend_calls=%d mean_records_per_call=%.3f elapsed=%s find_batch_histogram=%v all_BSON_results_verified=true", round+1, scenario.path, scenario.batch, scenario.concurrency, len(fixture.documents), len(commands), float64(len(fixture.documents))/float64(len(commands)), elapsed, histogram)
		}
	}
}

func runValueReadCase(ctx context.Context, fixture *valueReadFixture, scenario valueReadCase) (time.Duration, error) {
	jobs := make(chan []int, len(fixture.documents)/scenario.batch)
	for start := 0; start < len(fixture.documents); start += scenario.batch {
		indexes := make([]int, scenario.batch)
		for position := range indexes {
			// Reversed request order differs from database insertion order.
			indexes[position] = len(fixture.documents) - 1 - start - position
		}
		jobs <- indexes
	}
	close(jobs)
	failures := make(chan error, scenario.concurrency)
	var workers sync.WaitGroup
	started := time.Now()
	for range scenario.concurrency {
		workers.Go(func() {
			for indexes := range jobs {
				if err := fixture.read(ctx, scenario, indexes); err != nil {
					failures <- err
					return
				}
			}
		})
	}
	workers.Wait()
	elapsed := time.Since(started)
	close(failures)
	for err := range failures {
		return elapsed, err
	}
	return elapsed, nil
}
