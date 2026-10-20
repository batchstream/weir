//go:build integration

package app

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

type replyLossRecord struct {
	id       string
	expected any
	watch    *testmongo.OperationWatch
}
type replyLossLifetime struct {
	fixture      *testmongo.Replica
	proxy        *testmongo.ReplicaProxy
	client       *weirclient.Client
	service      *process
	primary, pid int
	kind         string
	records      []replyLossRecord
}
type replyLossReceipt struct {
	valid       bool
	description string
	err         error
}

func TestMongoReplicaCLIReplyLoss(t *testing.T) {
	if os.Getenv("WEIR_MONGO_REPLICA_INTEGRATION") != "1" {
		t.Skip("explicit owned replica integration opt-in")
	}
	// This is the ordinary CLI, not an in-process adapter or test-only server.
	binary := buildEndpointProcess(t)
	driver, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("reply_loss CLI=%s CLI_SHA256=%s driver=%s driver_SHA256=%s production_qualified=false", binary, replicaExecutableDigest(t, binary), driver, replicaExecutableDigest(t, driver))
	for _, secure := range []bool{false, true} {
		profile := "plain"
		if secure {
			profile = "tls"
		}
		t.Run(profile, func(t *testing.T) {
			for _, kind := range []string{"bulkWrite", "findAndModify"} {
				name := "typed"
				if kind == "findAndModify" {
					name = "native"
				}
				t.Run(name, func(t *testing.T) {
					fixtureOptions := testmongo.ReplicaOptions{TLS: secure}
					fixture := testmongo.OpenReplica(t, fixtureOptions)
					external, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					primary, _, err := fixture.WaitUsable(external, -1)
					cancel()
					if err != nil {
						t.Fatal(err)
					}
					proxy := testmongo.StartReplicaProxy(t, fixture)
					mongo := &Mongo{URI: proxy.SeedURI((primary + 1) % 3)}
					backend := BackendConfig{MongoDB: mongo}
					if secure {
						credential := &Credentials{Username: fixture.Username, Password: fixture.Password}
						trust := &BackendTLS{CAFile: fixture.CAFile}
						backend.Authentication, backend.TLS = credential, trust
					}
					local := &Local{Backend: backend}
					store := StoreConfig{Name: "mongo", Local: local}
					cfg := emptyConfig(t)
					cfg.Basic.Diagnostics.Address = "127.0.0.1:0"
					cfg.Routing.Stores = []StoreConfig{store}
					service := startProcessWithLifetime(t, binary, cfg, 3*time.Minute)
					open, cancel := context.WithTimeout(t.Context(), 3*time.Second)
					openOptions := weirclient.OpenOptions{Seed: service.address, Stores: []string{"mongo"}}
					client, err := weirclient.Open(open, openOptions)
					cancel()
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { _ = client.Close() })
					// Failure cleanup interrupts and joins held relays before normal CLI/fixture
					// cleanup. It never invokes DropReply and cannot manufacture injection proof.
					t.Cleanup(proxy.Close)
					lifetime := &replyLossLifetime{fixture: fixture, proxy: proxy, client: client, service: service, primary: primary, pid: service.command.Process.Pid, kind: kind}
					health := replyLossRecord{id: "health", expected: int32(0)}
					replyLossSeed(t, lifetime, health)
					for _, transition := range []string{"stable", "stepdown", "primary_loss"} {
						replyLossCase(t, lifetime, transition)
					}
					if kind == "findAndModify" {
						replyLossBusinessErrors(t, lifetime)
					}
					replicaCLIIdle(t, service, time.Now().Add(3*time.Second))
					replyLossOracles(t, lifetime, "before_CLI_exit")
					var terminal processStopResult
					var readWatches []*testmongo.ReadWatch
					if kind == "bulkWrite" {
						t.Run("sent_read_retirement", func(t *testing.T) {
							terminal, readWatches = replicaCLISentReadRetirement(t, lifetime)
						})
						if t.Failed() {
							return
						}
					} else {
						terminal = service.stop(t)
					}
					t.Logf("reply_loss final_same_PID=%d SIGTERM_elapsed=%s wait_complete=true exit_valid=%t exit_code=%d timed_out=%t wait_error=%v", lifetime.pid, terminal.Elapsed, terminal.ExitCodeValid, terminal.ExitCode, terminal.TimedOut, terminal.WaitErr)
					if !terminal.successful() {
						t.Fatal("actual CLI exit gate failed")
					}
					if err := client.Close(); err != nil {
						t.Fatal(err)
					}
					replicaClosedOwner(t, service)
					replyLossOracles(t, lifetime, "after_actual_CLI_exit")
					for _, watch := range readWatches {
						replicaCLIReadFacts(t, watch, "parent_actual_Wait_and_product_owner_zero")
						ctx, cancel := context.WithTimeout(t.Context(), time.Second)
						err := watch.CloseHeldRelays(ctx)
						cancel()
						if err != nil {
							t.Fatal(err)
						}
						if s := watch.Snapshot(); s.Joined != 2 || s.Tails != 2 {
							t.Fatal("actual targeted read relay join", s)
						}
					}
					proxy.Close()
					for i, snapshot := range proxy.Snapshots() {
						if snapshot.Active != 0 || snapshot.Rejections != 0 {
							t.Fatal("relay retirement", i, snapshot)
						}
						t.Logf("reply_loss member=%d frontend=%s actual=%s real_relay_join=true active=%d connections=%d", i, snapshot.Frontend, snapshot.Upstream, snapshot.Active, len(snapshot.Connections))
					}
					replyLossOracles(t, lifetime, "after_relay_join")
					ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
					err = fixture.CheckOriginalAddresses(ctx)
					cancel()
					if err != nil {
						t.Fatal(err)
					}
					fixture.Close(t)
				})
			}
		})
	}
}

func replyLossSeed(t *testing.T, lifetime *replyLossLifetime, record replyLossRecord) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	collectionOptions := options.Collection().SetWriteConcern(writeconcern.Majority())
	collection := lifetime.fixture.Admin.Database(lifetime.fixture.DB).Collection("records", collectionOptions)
	seed := bson.D{{Key: "_id", Value: record.id}, {Key: "n", Value: record.expected}}
	_, err := collection.InsertOne(ctx, seed)
	if err != nil {
		t.Fatal("majority seed", err)
	}
	replyLossEffect(t, lifetime, record)
}
func replyLossEffect(t *testing.T, lifetime *replyLossLifetime, record replyLossRecord) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	collectionOptions := options.Collection().SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary())
	collection := lifetime.fixture.Admin.Database(lifetime.fixture.DB).Collection("records", collectionOptions)
	filter := bson.D{{Key: "_id", Value: record.id}}
	raw, err := collection.FindOne(ctx, filter).Raw()
	if err != nil {
		t.Fatal("independent primary-majority read", err)
	}
	expected := bson.D{{Key: "n", Value: record.expected}}
	encoded, err := bson.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	want := bson.Raw(encoded).Lookup("n")
	got := raw.Lookup("n")
	if got.Type != want.Type || !bytes.Equal(got.Value, want.Value) {
		t.Fatal("independent old effect changed", record.id, got.Type, want.Type)
	}
	t.Logf("independent_primary_majority id=%s expected_n=%v observed_n=%v", record.id, record.expected, got)
}
func replyLossOracles(t *testing.T, lifetime *replyLossLifetime, phase string) {
	t.Helper()
	for _, record := range lifetime.records {
		replyLossEffect(t, lifetime, record)
		s := record.watch.Snapshot()
		if s.MatchedRequests != 1 || s.ForwardedItems != 1 || s.Error != "" {
			t.Fatal("persistent no-replay oracle", phase, record.id, s)
		}
		t.Logf("reply_loss phase=%s PID=%d owned_id=%s matched_requests=%d forwarded_mutation_items=%d observation=%+v", phase, lifetime.pid, record.id, s.MatchedRequests, s.ForwardedItems, s)
	}
}
func replyLossCase(t *testing.T, lifetime *replyLossLifetime, transition string) {
	t.Helper()
	replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "idle_baseline", false)
	external, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	primary, _, err := lifetime.fixture.WaitUsable(external, -1)
	cancel()
	if err != nil {
		t.Fatal(err)
	}
	lifetime.primary = primary
	record := replyLossRecord{id: "uncertain-" + transition, expected: int32(0)}
	replyLossSeed(t, lifetime, record)
	spec := testmongo.OwnedOperation{Namespace: lifetime.fixture.DB + ".records", ID: record.id, Kind: lifetime.kind}
	watch, err := lifetime.proxy.WatchOperation(spec)
	if err != nil {
		t.Fatal(err)
	}
	record.watch = watch
	if err := lifetime.proxy.ArmReplyLoss(primary, watch); err != nil {
		t.Fatal(err)
	}
	// The caller remains uncancelled throughout the held-reply/action/drop and
	// actual receipt barriers. Cancellation is cleanup, never the result oracle.
	ctx, stop := context.WithTimeout(t.Context(), 20*time.Second)
	defer stop()
	done := make(chan replyLossReceipt, 1)
	request := replyLossNativeCommand(record.id, false)
	amount := bson.D{{Key: "n", Value: int32(1)}}
	update := bson.D{{Key: "$inc", Value: amount}}
	raw, err := bson.Marshal(update)
	if err != nil {
		t.Fatal(err)
	}
	expression := &pb.Document{ContentType: "application/vnd.weir.mongodb-update.v1+bson", Data: raw}
	transform := &weirclient.AtomicTransformRequest{Resource: lifetime.fixture.DB + "/records/s:" + record.id, BackendExpression: expression}
	typedOptions := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: transform}
	nativeRequest := replyLossNativeRequest(t, lifetime.fixture.DB, request)
	consume := func(context.Context, *weirclient.NativeResponse, []byte) error {
		return fmt.Errorf("lost response unexpectedly delivered body")
	}
	nativeOptions := weirclient.NativeOptions{StoreName: "mongo", Request: nativeRequest, Consume: consume}
	go func() {
		receipt := replyLossReceipt{}
		if lifetime.kind == "bulkWrite" {
			result, err := lifetime.client.AtomicTransform(ctx, typedOptions)
			receipt.err = err
			receipt.valid = err == nil && result != nil && result.Outcome == weirclient.MutationUnknown && result.Failure != nil
			receipt.description = fmt.Sprintf("%v", result)
			if result != nil {
				receipt.description = fmt.Sprintf("actual_outcome=%v actual_failure=%v", result.Outcome, result.Failure)
			}
		} else {
			result, err := lifetime.client.Native(ctx, nativeOptions)
			receipt.err = err
			receipt.valid = err == nil && result != nil && result.Completion == weirclient.NativeResponseIncomplete && result.Failure != nil
			receipt.description = fmt.Sprintf("%v", result)
			if result != nil {
				receipt.description = fmt.Sprintf("actual_completion=%v actual_failure=%v", result.Completion, result.Failure)
			}
		}
		done <- receipt
	}()
	select {
	case <-watch.Held():
	case receipt := <-done:
		t.Fatal("caller returned before real hold", receipt, watch.Snapshot())
	case <-ctx.Done():
		t.Fatal("real successful held reply barrier", watch.Snapshot())
	}
	record.expected = int32(1)
	replyLossGauges(t, lifetime, "successful_reply_held", true)
	replyLossEffect(t, lifetime, record)
	s := watch.Snapshot()
	if s.MatchedRequests != 1 || s.ForwardedItems != 1 || !s.RequestValid || !s.ReplyValid || !s.ReplyReceived || !s.Held || s.DropPerformed || s.ReplyForwarded {
		t.Fatal("actual majority reply hold facts", s)
	}
	select {
	case receipt := <-done:
		t.Fatal("caller returned while reply held", receipt)
	default:
	}
	if ctx.Err() != nil {
		t.Fatal("caller not alive", ctx.Err())
	}
	t.Logf("reply_loss held kind=%s transition=%s PID=%d primary=%d caller_alive=true independent_n=1 observation=%+v", lifetime.kind, transition, lifetime.pid, primary, s)
	started := time.Now()
	if transition == "stepdown" {
		action, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		err := lifetime.fixture.StepDown(action, primary)
		cancel()
		if err != nil {
			t.Fatal("owned stepdown", err)
		}
	} else if transition == "primary_loss" {
		lifetime.fixture.LosePrimary(t, primary)
	}
	actionCompleted := time.Now()
	// Drop promptly after the action, before the separate external election wait.
	if err := watch.DropReply(); err != nil {
		t.Fatal("deliberate drop", err)
	}
	select {
	case <-watch.Dropped():
	case <-ctx.Done():
		t.Fatal("drop was not performed", watch.Snapshot())
	}
	dropPerformed := time.Now()
	if dropPerformed.Sub(actionCompleted) > time.Second {
		t.Fatal("deliberate drop did not promptly follow action")
	}
	select {
	case receipt := <-done:
		if !receipt.valid || receipt.err != nil || ctx.Err() != nil {
			t.Fatal("actual uncancelled caller lost-reply receipt", receipt, ctx.Err())
		}
		t.Logf("reply_loss actual_receipt kind=%s transition=%s PID=%d caller_context_uncancelled=true receipt=%s", lifetime.kind, transition, lifetime.pid, receipt.description)
	case <-ctx.Done():
		t.Fatal("actual receipt missing", ctx.Err())
	}
	lifetime.records = append(lifetime.records, record)
	// Election time is external. Check counters immediately, but do not put a
	// short independent read ahead of the external majority-usability barrier.
	for _, old := range lifetime.records {
		snapshot := old.watch.Snapshot()
		if snapshot.MatchedRequests != 1 || snapshot.ForwardedItems != 1 || snapshot.Error != "" {
			t.Fatal("after-drop persistent no-replay", old.id, snapshot)
		}
	}
	t.Logf("reply_loss transition=%s action_completed_at=%s drop_performed_at=%s drop_after_action=%s", transition, actionCompleted.Format(time.RFC3339Nano), dropPerformed.Format(time.RFC3339Nano), dropPerformed.Sub(actionCompleted))
	exclude := -1
	if transition != "stable" {
		exclude = primary
	}
	external, cancel = context.WithTimeout(t.Context(), 45*time.Second)
	next, usable, err := lifetime.fixture.WaitUsable(external, exclude)
	cancel()
	if err != nil {
		t.Fatal("external majority usability", err)
	}
	recovery, cancel := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
	for {
		err := replicaCLIHealth(recovery, lifetime.client, lifetime.fixture.DB)
		if err == nil {
			break
		}
		if recovery.Err() != nil {
			cancel()
			t.Fatal("immutable health recovery", err)
		}
		select {
		case <-recovery.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
	replyLossNewWork(t, recovery, lifetime, "healthy-"+transition)
	completed := time.Now()
	cancel()
	if completed.Sub(usable) > 5*time.Second || lifetime.service.command.Process.Pid != lifetime.pid {
		t.Fatal("same PID/SDK 5s recovery gate")
	}
	lifetime.primary = next
	replyLossCoverage(t, lifetime)
	replicaCLIIdle(t, lifetime.service, time.Now().Add(3*time.Second))
	replyLossGauges(t, lifetime, "returned_after_recovery", false)
	replyLossOracles(t, lifetime, "after_new_ID_recovery")
	t.Logf("reply_loss kind=%s transition=%s same_PID=%d same_SDK=true old_primary=%d new_primary=%d drop_after_action=%s external_majority_usable_at=%s external_transition=%s recovery_complete=%s", lifetime.kind, transition, lifetime.pid, primary, next, dropPerformed.Sub(actionCompleted), usable.Format(time.RFC3339Nano), usable.Sub(started), completed.Sub(usable))
}

func replyLossNativeCommand(id string, bad bool) bson.D {
	filter := bson.D{{Key: "_id", Value: id}}
	amount := bson.D{{Key: "n", Value: int32(1)}}
	update := bson.D{{Key: "$inc", Value: amount}}
	if bad {
		update = bson.D{{Key: "$notAnOperator", Value: amount}}
	}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}}
	return command
}
func replyLossNativeRequest(t *testing.T, database string, command bson.D) *pb.NativeRequest {
	t.Helper()
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: database, Request: document}
	return request
}
func replyLossNewWork(t *testing.T, ctx context.Context, lifetime *replyLossLifetime, id string) {
	t.Helper()
	value := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
	raw, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	write := &weirclient.WriteRequest{Resource: lifetime.fixture.DB + "/records/s:" + id, Document: document}
	put := weirclient.WriteOptions{StoreName: "mongo", Request: write}
	result, err := lifetime.client.Put(ctx, put)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("new-ID typed business work", result, err)
	}
	request := replyLossNativeRequest(t, lifetime.fixture.DB, replyLossNativeCommand(id, false))
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 64<<10 {
			return fmt.Errorf("bounded Native reply overflow")
		}
		body = append(body, chunk...)
		return nil
	}
	nativeOptions := weirclient.NativeOptions{StoreName: "mongo", Request: request, Consume: consume}
	native, err := lifetime.client.Native(ctx, nativeOptions)
	if err != nil || native == nil || native.Completion != weirclient.NativeResponseComplete || native.Failure != nil {
		t.Fatal("new-ID Native business work", native, err)
	}
	reply := bson.Raw(body)
	if reply.Validate() != nil || reply.Lookup("ok").AsInt64() != 1 || reply.Lookup("writeConcernError").Type != 0 || reply.Lookup("value").Document().Lookup("n").AsInt64() != 1 {
		t.Fatal("new-ID business reply")
	}
	record := replyLossRecord{id: id, expected: int32(1)}
	replyLossEffect(t, lifetime, record)
}
func replyLossCoverage(t *testing.T, lifetime *replyLossLifetime) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		err := lifetime.proxy.CheckWireCoverage(lifetime.primary, lifetime.kind)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("observable CLI frontend closure/monitor/data/TLS coverage", err, lifetime.proxy.Snapshots())
		}
		time.Sleep(10 * time.Millisecond)
	}
	for i, snapshot := range lifetime.proxy.Snapshots() {
		for _, connection := range snapshot.Connections {
			t.Logf("CLI_wire member=%d frontend=%s actual=%s connection=%d TLS_front=%t TLS_upstream=%t discovery_replies=%d forwarded=%v", i, snapshot.Frontend, snapshot.Upstream, connection.ID, connection.FrontendTLS, connection.UpstreamTLS, connection.DiscoveryReplies, connection.Commands)
		}
	}
}

func replyLossBusinessErrors(t *testing.T, lifetime *replyLossLifetime) {
	t.Helper()
	for _, kind := range []string{"command_error", "item_error"} {
		record := replyLossRecord{id: kind, expected: int32(0)}
		command := replyLossNativeCommand(record.id, true)
		wireKind := "findAndModify"
		database := lifetime.fixture.DB
		if kind == "item_error" {
			record.expected = "not_numeric"
			wireKind = "bulkWrite"
			database = "admin"
			filter := bson.D{{Key: "_id", Value: record.id}}
			amount := bson.D{{Key: "n", Value: int32(1)}}
			update := bson.D{{Key: "$inc", Value: amount}}
			item := bson.D{{Key: "update", Value: int32(0)}, {Key: "filter", Value: filter}, {Key: "updateMods", Value: update}, {Key: "multi", Value: false}, {Key: "upsert", Value: false}}
			namespace := bson.D{{Key: "ns", Value: lifetime.fixture.DB + ".records"}}
			concern := bson.D{{Key: "w", Value: "majority"}}
			cursor := bson.D{{Key: "batchSize", Value: int32(1)}}
			command = bson.D{{Key: "bulkWrite", Value: 1}, {Key: "ops", Value: bson.A{item}}, {Key: "nsInfo", Value: bson.A{namespace}}, {Key: "errorsOnly", Value: false}, {Key: "ordered", Value: false}, {Key: "cursor", Value: cursor}, {Key: "writeConcern", Value: concern}}
		}
		replyLossSeed(t, lifetime, record)
		spec := testmongo.OwnedOperation{Namespace: lifetime.fixture.DB + ".records", ID: record.id, Kind: wireKind}
		watch, err := lifetime.proxy.WatchOperation(spec)
		if err != nil {
			t.Fatal(err)
		}
		record.watch = watch
		lifetime.records = append(lifetime.records, record)
		request := replyLossNativeRequest(t, database, command)
		var body []byte
		consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
			if len(body)+len(chunk) > 64<<10 {
				return fmt.Errorf("bounded error body overflow")
			}
			body = append(body, chunk...)
			return nil
		}
		nativeOptions := weirclient.NativeOptions{StoreName: "mongo", Request: request, Consume: consume}
		ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
		result, err := lifetime.client.Native(ctx, nativeOptions)
		cancel()
		if err != nil || result == nil || result.Completion != weirclient.NativeResponseComplete || result.Failure != nil {
			t.Fatal("complete business error became forwarding failure", kind, result, err)
		}
		reply := bson.Raw(body)
		if reply.Validate() != nil {
			t.Fatal("invalid business error BSON")
		}
		if kind == "command_error" {
			if reply.Lookup("ok").AsInt64() != 0 || reply.Lookup("code").AsInt64() != 9 {
				t.Fatal("real invalid operator command error envelope", reply.Lookup("ok").Type, reply.Lookup("code").AsInt64())
			}
		} else {
			cursor := reply.Lookup("cursor").Document()
			values, err := cursor.Lookup("firstBatch").Array().Values()
			if err != nil || len(values) != 1 {
				t.Fatal("real item error result count")
			}
			item := values[0].Document()
			if reply.Lookup("ok").AsInt64() != 1 || reply.Lookup("nErrors").AsInt64() != 1 || reply.Lookup("nModified").AsInt64() != 0 || cursor.Lookup("id").AsInt64() != 0 || item.Lookup("ok").AsInt64() != 0 || item.Lookup("idx").AsInt64() != 0 || item.Lookup("code").AsInt64() != 14 {
				t.Fatal("real top-ok item error envelope", reply.Lookup("ok").Type, reply.Lookup("nErrors").AsInt64(), item.Lookup("code").AsInt64())
			}
		}
		deadline := time.Now().Add(time.Second)
		for !watch.Snapshot().ReplyForwarded && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		s := watch.Snapshot()
		digest := sha256.Sum256(body)
		if !s.ReplyReceived || !s.ReplyForwarded || s.DropPerformed || s.ReplyDigest != digest || s.MatchedRequests != 1 || s.ForwardedItems != 1 || s.Error != "" {
			t.Fatal("actual as-received business body digest", s)
		}
		replyLossEffect(t, lifetime, record)
		t.Logf("Native_complete_business_error kind=%s completion=%v forwarding_failure=false observed_body_SHA256=%x consumer_body_SHA256=%x unchanged_effect=true", kind, result.Completion, s.ReplyDigest, digest)
	}
}

// Read actual metric samples, including presence, rather than inventing terminal
// zeros or using a forced collection to make ownership appear retired.
func replyLossGauges(t *testing.T, lifetime *replyLossLifetime, phase string, active bool) {
	t.Helper()
	metrics := testmetrics.Scrape(t, lifetime.service.diagnostic)
	values := make(map[string]float64)
	for _, name := range []string{"weir_store_pending_entries", "weir_store_active_executions", "weir_store_working_reserved_bytes", "weir_store_publishers", "weir_store_result_reserved_entries", "weir_store_result_reserved_bytes", "weir_store_retained_result_reserved_bytes", "weir_store_paused_native_outputs"} {
		if metrics[name] == nil {
			t.Fatal("missing reply-loss ownership gauge", name)
		}
		values[name] = testmetrics.Sum(metrics, name)
		if !active && values[name] != 0 {
			t.Fatal("idle reply-loss ownership gauge", phase, name, values[name])
		}
	}
	if active && values["weir_store_active_executions"] < 1 {
		t.Fatal("held reply had no actual active execution", values)
	}
	t.Logf("reply_loss phase=%s same_PID=%d actual_gauges=%v", phase, lifetime.pid, values)
}
