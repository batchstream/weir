//go:build integration

package mongodb

import (
	"context"
	"net/url"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestMongoSCRAMTLSProductionOpen(t *testing.T) {
	if os.Getenv("WEIR_M10_INTEGRATION") != "1" {
		t.Skip("secure profile opt-in")
	}
	fixture := testmongo.OpenSecure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := Config{URI: fixture.URI, Store: "mongo", Database: fixture.DB, Collection: "records", Pool: 4}
	cfg = mongoFixtureConfig(t, cfg)
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close()
	doc := bson.D{{Key: "_id", Value: "secure"}, {Key: "n", Value: int64(42)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{MediaType: "application/bson", Data: raw}
	put := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: "weir://mongo/" + fixture.DB + "/records/s:secure", Action: put}
	mutation := &pb.BulkOperation_Mutate{Mutate: request}
	op := &pb.BulkOperation{Operation: mutation}
	plan, failure := adapter.Prepare(op)
	if failure != nil {
		t.Fatal(failure)
	}
	result, _ := adapter.Execute(ctx, []*execution.Plan{plan})
	if result[0].GetMutation().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result)
	}
	read := prepareCounter(t, adapter, "secure")
	result, _ = adapter.Execute(ctx, []*execution.Plan{read})
	if result[0].GetRead().GetDocument() == nil {
		t.Fatal(result)
	}
	for name, uri := range map[string]string{"password": fixture.BadPassURI, "missing": fixture.MissingPassURI, "CA": fixture.BadCAURI, "SAN": fixture.WrongHostURI, "privilege": fixture.DeniedURI} {
		t.Run(name, func(t *testing.T) {
			invalid := cfg
			invalid.URI = uri
			invalid = mongoFixtureConfig(t, invalid)
			bad, err := Open(ctx, invalid)
			if bad != nil {
				bad.Close()
			}
			parsed, _ := url.Parse(uri)
			secret, _ := parsed.User.Password()
			if err == nil || strings.Contains(err.Error(), "weir_app") || strings.Contains(err.Error(), uri) || secret != "" && strings.Contains(err.Error(), secret) {
				t.Fatal("connection was accepted or leaked credentials")
			}
		})
	}
}

func TestMongoSCRAMTLSRepeatedFailureAndClose(t *testing.T) {
	if os.Getenv("WEIR_M10_INTEGRATION") != "1" {
		t.Skip("secure profile opt-in")
	}
	fixture := testmongo.OpenSecure(t)
	baseline := connectionCount(t, fixture.Admin)
	goroutines := runtime.NumGoroutine()
	for i := 0; i < 12; i++ {
		uri := fixture.BadPassURI
		if i%2 == 0 {
			uri = fixture.BadCAURI
		}
		cfg := Config{URI: uri, Store: "mongo", Database: fixture.DB, Collection: "records", Pool: 1}
		cfg = mongoFixtureConfig(t, cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		start := time.Now()
		adapter, err := Open(ctx, cfg)
		cancel()
		if adapter != nil {
			adapter.Close()
		}
		if err == nil || time.Since(start) > time.Second {
			t.Fatal("failure/close exceeded finite deadline")
		}
	}
	cfg := Config{URI: fixture.URI, Store: "mongo", Database: fixture.DB, Collection: "records", Pool: 1}
	cfg = mongoFixtureConfig(t, cfg)
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		adapter, err := Open(ctx, cfg)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.Close(); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && (connectionCount(t, fixture.Admin) > baseline || runtime.NumGoroutine() > goroutines+4) {
		time.Sleep(20 * time.Millisecond)
	}
	after := connectionCount(t, fixture.Admin)
	if after > baseline || runtime.NumGoroutine() > goroutines+4 {
		t.Fatal("retained failed connections/goroutines", baseline, after, goroutines, runtime.NumGoroutine())
	}
	t.Logf("failed opens=12 successful reopen/close=4 sockets before=%d after=%d goroutines before=%d after=%d", baseline, after, goroutines, runtime.NumGoroutine())
}

func TestMongoSCRAMTLS391NoReplay(t *testing.T) {
	if os.Getenv("WEIR_MONGO_PROFILE") != "tls" {
		t.Skip("full TLS suite opt-in")
	}
	for _, kind := range []string{"ordinary", "expression", "write_error"} {
		t.Run(kind, func(t *testing.T) {
			backend := testmongo.Open(t)
			native, db := backend.Admin, backend.DB
			proxy := testmongo.StartProxy(t, backend)
			cfg := Config{URI: proxy.URI(), Store: "mongo", Database: db, Collection: "records", Pool: 1}
			cfg = mongoFixtureConfig(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			adapter, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close()
			document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(1)}}
			_, err = native.Database(db).Collection("records").InsertOne(ctx, document)
			if err != nil {
				t.Fatal(err)
			}
			increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}
			work := mongoExpression(t, adapter, increment)
			if kind != "expression" {
				raw, _ := bson.Marshal(document)
				doc := &pb.Document{MediaType: "application/bson", Data: raw}
				put := &pb.MutateRequest_Put{Put: doc}
				request := &pb.MutateRequest{Resource: "weir://mongo/" + db + "/records/s:counter", Action: put}
				variant := &pb.BulkOperation_Mutate{Mutate: request}
				operation := &pb.BulkOperation{Operation: variant}
				var failure *pb.Failure
				work, failure = adapter.Prepare(operation)
				if failure != nil {
					t.Fatal(failure)
				}
			}
			if kind == "write_error" {
				proxy.AlterCommand = "bulkWrite"
				proxy.AlterMode = "write_error_391"
				proxy.AlterRemaining.Store(1)
			} else {
				data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "errorCode", Value: int32(391)}, {Key: "appName", Value: "weir:" + db}}
				testmongo.FailCommand(t, native, data, 1)
			}
			results, _ := adapter.Execute(ctx, []*execution.Plan{work})
			if results[0].GetMutation().Outcome == pb.MutationOutcome_APPLIED || results[0].GetMutation().Outcome == pb.MutationOutcome_NOT_STARTED {
				t.Fatal("391 produced unjustified outcome", results)
			}
			count := 0
			for _, e := range proxy.Events() {
				if e.Command == "bulkWrite" {
					count++
				}
			}
			if count != 1 {
				t.Fatal("391 replayed business command", count)
			}
			filter := bson.D{{Key: "_id", Value: "counter"}}
			raw, err := native.Database(db).Collection("records").FindOne(ctx, filter).Raw()
			if err != nil || raw.Lookup("n").AsInt64() != 1 {
				t.Fatal("unexpected effect after 391")
			}
			t.Logf("%s 391: business updates=%d n=1 outcome=%s", kind, count, results[0].GetMutation().Outcome)
		})
	}
}

func verifyReconnectRead(t *testing.T, adapter *Adapter, proxy *testmongo.Proxy, key string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := prepareCounter(t, adapter, key)
	results, _ := adapter.Execute(ctx, []*execution.Plan{plan})
	if results[0].GetRead().GetDocument() == nil {
		t.Fatal("new connection did not recover after lost reply", results)
	}
	sasl := 0
	for _, event := range proxy.Events() {
		if event.Command == "saslContinue" {
			sasl++
		}
	}
	if os.Getenv("WEIR_MONGO_PROFILE") == "tls" && sasl < 2 {
		t.Fatal("SCRAM reconnect not observed", sasl)
	}
	t.Logf("post-loss production Read recovered; SASL continuation commands=%d (excluded from business counts)", sasl)
}
