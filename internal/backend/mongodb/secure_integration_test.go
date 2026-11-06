//go:build integration

package mongodb

import (
	"context"
	"net/url"
	"runtime"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestMongoSCRAMTLSProductionOpen(t *testing.T) {
	fixture := testmongo.OpenSecure(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cfg := Config{URI: fixture.URI, Store: "mongo"}
	cfg = mongoFixtureConfig(t, cfg)
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	doc := bson.D{{Key: "_id", Value: "secure"}, {Key: "n", Value: int64(42)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	put := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: fixture.DB + "/records/s:secure", Action: put}
	opOperation := &pb.Command_Mutate{Mutate: request}
	opCommand := &pb.Command{Operation: opOperation}
	op := &pb.ExecuteRequest{Index: 1, Command: opCommand}
	plan, failure := prepareTestRecord(adapter, op)
	if failure != nil {
		t.Fatal(failure)
	}
	result := adapter.ExecuteRecords(ctx, []*execution.Plan{plan})
	if result[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result)
	}
	read := prepareCounter(t, adapter, fixture.DB+"/records/s:secure")
	result = adapter.ExecuteRecords(ctx, []*execution.Plan{read})
	if result[0].GetReadResult().GetDocument() == nil {
		t.Fatal(result)
	}
	for name, uri := range map[string]string{"password": fixture.BadPassURI, "missing": fixture.MissingPassURI, "CA": fixture.BadCAURI, "SAN": fixture.WrongHostURI, "privilege": fixture.DeniedURI} {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			invalid := cfg
			invalid.URI = uri
			invalid = mongoFixtureConfig(t, invalid)
			bad, err := Open(ctx, invalid)
			if name == "privilege" {
				if err != nil || bad == nil {
					t.Fatal("server qualification must not require target privileges", err)
				}
				work := prepareCounter(t, bad, fixture.DB+"/records/s:secure")
				replies := bad.ExecuteRecords(ctx, []*execution.Plan{work})
				if replies[0].GetReadResult().GetFailure() == nil || replies[0].GetReadResult().GetDocument() != nil {
					t.Fatal("request bypassed target privilege check", replies)
				}
				if err = bad.Close(context.Background()); err != nil {
					t.Fatal(err)
				}
				return
			}
			if bad != nil {
				bad.Close(context.Background())
			}
			parsed, _ := url.Parse(uri)
			secret, _ := parsed.User.Password()
			if err == nil || strings.Contains(err.Error(), "weir_app") || strings.Contains(err.Error(), uri) || secret != "" && strings.Contains(err.Error(), secret) {
				t.Fatal("connection was accepted or leaked credentials")
			}
		})
	}
}

// plaintextConfig turns a fixture configuration into SCRAM over tls=false.
func plaintextConfig(t *testing.T, cfg Config) Config {
	t.Helper()
	parsed, err := url.Parse(cfg.URI)
	if err != nil {
		t.Fatal("invalid owned MongoDB fixture URI")
	}
	query := parsed.Query()
	query.Set("tls", "false")
	parsed.RawQuery = query.Encode()
	cfg.URI = parsed.String()
	cfg.CAFile = ""
	return cfg
}

// With an explicit tls=false, SCRAM-SHA-256 authenticates over plaintext:
// writes and reads work without a TLS handshake, and a wrong password still
// fails authentication.
func TestMongoSCRAMWithoutTLSOpen(t *testing.T) {
	fixture := testmongo.OpenSecureAllowingPlaintext(t)
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	cfg := Config{URI: fixture.URI, Store: "mongo"}
	cfg = plaintextConfig(t, mongoFixtureConfig(t, cfg))
	before := settledTransportCounts(t, fixture.Admin)
	adapter, err := Open(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adapter.Close(context.Background())
	doc := bson.D{{Key: "_id", Value: "plaintext"}, {Key: "n", Value: int64(7)}}
	raw, _ := bson.Marshal(doc)
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	put := &pb.MutateRequest_Put{Put: document}
	request := &pb.MutateRequest{Resource: fixture.DB + "/records/s:plaintext", Action: put}
	operation := &pb.Command_Mutate{Mutate: request}
	command := &pb.Command{Operation: operation}
	op := &pb.ExecuteRequest{Index: 1, Command: command}
	plan, failure := prepareTestRecord(adapter, op)
	if failure != nil {
		t.Fatal(failure)
	}
	result := adapter.ExecuteRecords(ctx, []*execution.Plan{plan})
	if result[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED {
		t.Fatal(result)
	}
	read := prepareCounter(t, adapter, fixture.DB+"/records/s:plaintext")
	result = adapter.ExecuteRecords(ctx, []*execution.Plan{read})
	if result[0].GetReadResult().GetDocument() == nil {
		t.Fatal(result)
	}
	after := transportCounts(t, fixture.Admin)
	if after.connections <= before.connections || after.handshakes != before.handshakes {
		t.Fatalf("tls=false must connect without TLS: connections %d->%d, TLS handshakes %d->%d",
			before.connections, after.connections, before.handshakes, after.handshakes)
	}
	badConfig := Config{URI: fixture.BadPassURI, Store: "mongo"}
	badConfig = plaintextConfig(t, mongoFixtureConfig(t, badConfig))
	badContext, badCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer badCancel()
	bad, err := Open(badContext, badConfig)
	if err == nil {
		bad.Close(context.Background())
		t.Fatal("wrong password authenticated over plaintext")
	}
	if !strings.Contains(err.Error(), "MongoDB code 18") {
		t.Fatal("wrong password over plaintext did not fail authentication", err)
	}
}

type transportCount struct {
	connections int64
	handshakes  int64
}

// settledTransportCounts waits until the fixture's own clients stop opening
// monitoring connections, so later changes belong to the code under test.
func settledTransportCounts(t *testing.T, client *mongo.Client) transportCount {
	t.Helper()
	previous := transportCounts(t, client)
	for range 20 {
		time.Sleep(250 * time.Millisecond)
		current := transportCounts(t, client)
		if current == previous {
			return current
		}
		previous = current
	}
	t.Fatal("fixture connections did not settle")
	return previous
}

// transportCounts reads how many connections the server has accepted and how
// many of them negotiated TLS.
func transportCounts(t *testing.T, client *mongo.Client) transportCount {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	command := bson.D{{Key: "serverStatus", Value: 1}}
	var status struct {
		Connections struct {
			TotalCreated int64 `bson:"totalCreated"`
		} `bson:"connections"`
		TransportSecurity map[string]int64 `bson:"transportSecurity"`
	}
	if err := client.Database("admin").RunCommand(ctx, command).Decode(&status); err != nil {
		t.Fatal(err)
	}
	count := transportCount{connections: status.Connections.TotalCreated}
	for _, handshakes := range status.TransportSecurity {
		count.handshakes += handshakes
	}
	return count
}

func TestMongoSCRAMTLSRepeatedFailureAndClose(t *testing.T) {
	fixture := testmongo.OpenSecure(t)
	baseline := connectionCount(t, fixture.Admin)
	goroutines := runtime.NumGoroutine()
	for i := 0; i < 12; i++ {
		uri := fixture.BadPassURI
		if i%2 == 0 {
			uri = fixture.BadCAURI
		}
		cfg := Config{URI: uri, Store: "mongo"}
		cfg = mongoFixtureConfig(t, cfg)
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		start := time.Now()
		adapter, err := Open(ctx, cfg)
		cancel()
		if adapter != nil {
			adapter.Close(context.Background())
		}
		if err == nil || time.Since(start) > time.Second {
			t.Fatal("failure/close exceeded finite deadline")
		}
	}
	cfg := Config{URI: fixture.URI, Store: "mongo"}
	cfg = mongoFixtureConfig(t, cfg)
	for i := 0; i < 4; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		adapter, err := Open(ctx, cfg)
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if err := adapter.Close(context.Background()); err != nil {
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
	for _, kind := range []string{"ordinary", "expression", "write_error"} {
		t.Run(kind, func(t *testing.T) {
			backend := &testmongo.OpenSecure(t).Fixture
			native, db := backend.Admin, backend.DB
			proxy := testmongo.StartProxy(t, backend)
			cfg := Config{URI: proxy.URI(), Store: "mongo"}
			cfg = mongoFixtureConfig(t, cfg)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			adapter, err := Open(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer adapter.Close(context.Background())
			document := bson.D{{Key: "_id", Value: "counter"}, {Key: "n", Value: int32(1)}}
			_, err = native.Database(db).Collection("records").InsertOne(ctx, document)
			if err != nil {
				t.Fatal(err)
			}
			increment := bson.D{{Key: "$inc", Value: bson.D{{Key: "n", Value: 1}}}}
			work := mongoExpression(t, adapter, db, increment)
			if kind != "expression" {
				raw, _ := bson.Marshal(document)
				doc := &pb.Document{ContentType: "application/bson", Data: raw}
				put := &pb.MutateRequest_Put{Put: doc}
				request := &pb.MutateRequest{Resource: db + "/records/s:counter", Action: put}
				operationOperation := &pb.Command_Mutate{Mutate: request}
				operationCommand := &pb.Command{Operation: operationOperation}
				operation := &pb.ExecuteRequest{Index: 1, Command: operationCommand}
				var failure *pb.Failure
				work, failure = prepareTestRecord(adapter, operation)
				if failure != nil {
					t.Fatal(failure)
				}
			}
			if kind == "write_error" {
				proxy.AlterCommand = "bulkWrite"
				proxy.AlterMode = "write_error_391"
				proxy.AlterRemaining.Store(1)
			} else {
				data := bson.D{{Key: "failCommands", Value: bson.A{"bulkWrite"}}, {Key: "errorCode", Value: int32(391)}, {Key: "appName", Value: "weir:mongo"}}
				testmongo.FailCommand(t, native, data, 1)
			}
			results := adapter.ExecuteRecords(ctx, []*execution.Plan{work})
			if results[0].GetMutationResult().Outcome == pb.MutationOutcome_APPLIED || results[0].GetMutationResult().Outcome == pb.MutationOutcome_NOT_STARTED {
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
			t.Logf("%s 391: business updates=%d n=1 outcome=%s", kind, count, results[0].GetMutationResult().Outcome)
		})
	}
}

func verifyReconnectRead(t *testing.T, adapter *Adapter, proxy *testmongo.Proxy, resource string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	plan := prepareCounter(t, adapter, resource)
	results := adapter.ExecuteRecords(ctx, []*execution.Plan{plan})
	if results[0].GetReadResult().GetDocument() == nil {
		t.Fatal("new connection did not recover after lost reply", results)
	}
	sasl := 0
	for _, event := range proxy.Events() {
		if event.Command == "saslContinue" {
			sasl++
		}
	}
	if proxy.Secure() && sasl < 2 {
		t.Fatal("SCRAM reconnect not observed", sasl)
	}
	t.Logf("post-loss production Read recovered; SASL continuation commands=%d (excluded from business counts)", sasl)
}
