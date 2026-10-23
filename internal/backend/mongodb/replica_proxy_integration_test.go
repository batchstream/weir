//go:build integration

package mongodb

import (
	"context"
	"os"
	"testing"
	"time"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/store"
	"github.com/batchstream/weir/internal/testutil"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/topology"
)

// Focused discovery and stable held-reply driver ownership; transaction
// uncertainty and CLI lifecycle are covered by the internal/app replica tests.
func TestMongoReplicaProxyDiscoveryRecovery(t *testing.T) {
	if os.Getenv("WEIR_MONGO_REPLICA_INTEGRATION") != "1" {
		t.Skip("explicit owned replica integration opt-in")
	}
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			fixtureOptions := testmongo.ReplicaOptions{TLS: secure}
			fixture := testmongo.OpenReplica(t, fixtureOptions)
			probe, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			primary, _, err := fixture.WaitUsable(probe, -1)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			proxy := testmongo.StartReplicaProxy(t, fixture)
			seed := (primary + 1) % 3
			cfg := Config{Store: "mongo", URI: proxy.SeedURI(seed), Username: fixture.Username, Password: fixture.Password, CAFile: fixture.CAFile}
			open, stop := context.WithTimeout(t.Context(), 3*time.Second)
			adapter, err := Open(open, cfg)
			stop()
			if err != nil {
				for i, snapshot := range proxy.Snapshots() {
					t.Logf("bootstrap_failure member=%d observed=%+v", i, snapshot)
				}
				t.Fatal("secondary frontend bootstrap", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				if err := adapter.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			// Failure cleanup must unblock held relays before adapter retirement.
			t.Cleanup(proxy.Close)
			work := replicaProxyWorkOptions{adapter: adapter, fixture: fixture, id: "initial"}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			replicaProxyWork(t, ctx, work)
			cancel()
			replicaProxyCoverage(t, adapter, proxy, primary)
			replicaProxyOriginal(t, fixture)
			t.Logf("TLS=%t secondary_seed=%d selected_nonseed_primary=%d actual_topology=%v", secure, seed, primary, replicaProxyDiscovered(t, adapter))
			heldOptions := replicaHeldOptions{adapter: adapter, fixture: fixture, proxy: proxy, primary: primary}
			watches := replicaStableReplyLoss(t, heldOptions)
			// The same Runtime owns topology Lua, stable Lua and read retirement.
			delegate := &replicaReadAdapter{Adapter: adapter}
			runtime, err := store.New(delegate, store.DefaultLimits())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if !t.Failed() {
					return
				}
				proxy.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				if err := runtime.Close(ctx); err != nil {
					t.Error(err)
				}
			})
			var topologyAborts []testmongo.LuaCase
			var topologyCommits []testmongo.LuaCommitCase
			for _, transition := range []string{"stepdown", "primary_loss"} {
				luaOptions := replicaLuaRuntimeOptions{runtime: runtime, delegate: delegate, fixture: fixture, proxy: proxy, primary: primary}
				topology := replicaLuaTopologyBegin(t, luaOptions, transition)
				started := time.Now()
				if transition == "stepdown" {
					ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					err = fixture.StepDown(ctx, primary)
					cancel()
					if err != nil {
						t.Fatal("ordinary owned stepdown", err)
					}
				} else {
					fixture.LosePrimary(t, primary)
				}
				observeUsability := testmongo.ObserveLuaTopologyUsability(t, fixture.WaitUsable, primary)
				abortCase, commitCase := topology.finish(t)
				topologyAborts = append(topologyAborts, abortCase)
				topologyCommits = append(topologyCommits, commitCase)
				next, usable, err := observeUsability()
				if err != nil {
					t.Fatal("external majority usability", err)
				}
				recovery, cancel := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
				replicaProxyRecover(t, recovery, adapter, fixture.DB)
				work.id = transition
				replicaProxyWork(t, recovery, work)
				newID := replicaLuaTopologyRecovery(t, recovery, luaOptions, transition)
				topologyAborts[len(topologyAborts)-1].RecoveryID = newID
				topologyCommits[len(topologyCommits)-1].RecoveryID = newID
				completed := time.Now()
				cancel()
				proxy.CheckLuaCases(t, topologyAborts, "backend_topology_new_Runtime_work")
				proxy.CheckLuaCommitCases(t, topologyCommits, "backend_topology_new_Runtime_work")
				if completed.Sub(usable) > 5*time.Second {
					t.Fatal("same-adapter new work exceeded 5s")
				}
				replicaProxyCoverage(t, adapter, proxy, next)
				replicaProxyOriginal(t, fixture)
				t.Logf("TLS=%t transition=%s old_primary=%d new_primary=%d external_transition=%s Weir_new_work=%s", secure, transition, primary, next, usable.Sub(started), completed.Sub(usable))
				primary = next
			}
			if adapter.client.NumberSessionsInProgress() != 0 {
				t.Fatal("completed work retained a checked-out session")
			}
			luaOptions := replicaLuaRuntimeOptions{runtime: runtime, delegate: delegate, fixture: fixture, proxy: proxy, primary: primary}
			luaCases := replicaLuaRuntime(t, luaOptions)
			commitCases := replicaLuaCommitRuntime(t, luaOptions, luaCases)
			luaCases = append(topologyAborts, luaCases...)
			commitCases = append(topologyCommits, commitCases...)
			proxy.CheckLuaCases(t, luaCases, "backend_before_read_retirement")
			proxy.CheckLuaCommitCases(t, commitCases, "backend_before_read_retirement")
			var readWatches []*testmongo.ReadWatch
			var lifecycleWatches []*testmongo.LuaWatch
			t.Run("sent_read_retirement", func(t *testing.T) {
				readOptions := replicaReadRuntimeOptions{adapter: adapter, fixture: fixture, proxy: proxy, primary: primary, runtime: runtime, delegate: delegate}
				readWatches, lifecycleWatches = replicaReadRuntime(t, readOptions)
			})
			if t.Failed() {
				return
			}
			proxy.CheckLuaLifecycleCases(t, lifecycleWatches, "backend_actual_runtime_Close")
			proxy.CheckLuaCases(t, luaCases, "backend_actual_runtime_Close")
			proxy.CheckLuaCommitCases(t, commitCases, "backend_actual_runtime_Close")
			ownerZero(t, adapter.dialer)
			replicaFinalReplyLoss(t, heldOptions, watches)
			for _, watch := range readWatches {
				replicaReadFacts(t, watch, "backend_parent_product_owner_zero")
				ctx, cancel := context.WithTimeout(t.Context(), time.Second)
				err := watch.CloseHeldRelays(ctx)
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				if s := watch.Snapshot(); s.Joined != 2 || s.Tails != 2 {
					t.Fatal("actual read relay join", s)
				}
			}
			proxy.Close()
			for i, snapshot := range proxy.Snapshots() {
				if snapshot.Active != 0 || snapshot.Rejections != 0 {
					t.Fatal("observer did not retire cleanly", i, snapshot.Active, snapshot.Rejections)
				}
				t.Logf("member=%d frontend=%s actual=%s joined_connections=%d observer_upstream=0", i, snapshot.Frontend, snapshot.Upstream, len(snapshot.Connections))
			}
			replicaFinalReplyLoss(t, heldOptions, watches)
			proxy.CheckLuaLifecycleCases(t, lifecycleWatches, "backend_actual_relay_join")
			proxy.CheckLuaCases(t, luaCases, "backend_actual_relay_join")
			proxy.CheckLuaCommitCases(t, commitCases, "backend_actual_relay_join")
			fixture.Close(t)
		})
	}
}

type replicaProxyWorkOptions struct {
	adapter *Adapter
	fixture *testmongo.Replica
	id      string
}

func replicaProxyWork(t *testing.T, ctx context.Context, opts replicaProxyWorkOptions) {
	t.Helper()
	adapter, fixture, id := opts.adapter, opts.fixture, opts.id
	work := ownerMutation(t, adapter, fixture.DB, id)
	plans := []*execution.Plan{work}
	results := adapter.executeRecords(ctx, plans)
	if len(results) != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().GetFailure() != nil {
		t.Fatal("typed write through frontend", results)
	}
	read := prepareCounter(t, adapter, fixture.DB+"/records/s:"+id)
	plans = []*execution.Plan{read}
	results = adapter.executeRecords(ctx, plans)
	if len(results) != 1 || results[0].GetReadResult().GetDocument() == nil || results[0].GetReadResult().GetFailure() != nil {
		t.Fatal("typed read through frontend", results)
	}
	filter := bson.D{{Key: "_id", Value: id}}
	amount := bson.D{{Key: "n", Value: int32(1)}}
	update := bson.D{{Key: "$inc", Value: amount}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}}
	raw, err := bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	request := &pb.NativeRequest{Resource: fixture.DB, Request: document}
	native, failure := adapter.prepareNative(request)
	if failure != nil {
		t.Fatal(failure)
	}
	native.Command = testutil.NativeCommand(request)
	capture := &nativeCapture{}
	end := adapter.executeNative(ctx, native, capture.Emit)
	if end.GetCompletion() != pb.NativeCompletion_RESPONSE_COMPLETE || end.Failure != nil {
		t.Fatal("Native frontend completion", end)
	}
	reply := bson.Raw(capture.body.Bytes())
	if reply.Validate() != nil || reply.Lookup("ok").AsInt64() != 1 || reply.Lookup("writeConcernError").Type != 0 || reply.Lookup("value").Document().Lookup("n").AsInt64() != 2 {
		t.Fatal("Native majority effect reply invalid")
	}
	programOptions := batchOperationOptions{resource: fixture.DB + "/records/s:" + id, program: incrementProgram}
	program := prepareBatchProgram(t, adapter, programOptions)
	plans = []*execution.Plan{program}
	results = adapter.executePrograms(ctx, plans)
	if len(results) != 1 || results[0].GetMutationResult().GetOutcome() != pb.MutationOutcome_APPLIED || results[0].GetMutationResult().GetFailure() != nil {
		t.Fatal("Lua transaction through frontend", results)
	}
	collectionOptions := options.Collection().SetReadConcern(readconcern.Majority()).SetReadPreference(readpref.Primary())
	collection := fixture.Admin.Database(fixture.DB).Collection("records", collectionOptions)
	var persisted struct{ N int32 }
	if err := collection.FindOne(ctx, filter).Decode(&persisted); err != nil || persisted.N != 3 {
		t.Fatal("independent primary majority effect oracle", persisted.N, err)
	}
	if adapter.client.NumberSessionsInProgress() != 0 {
		t.Fatal("finished frontend work retained a client session")
	}
	t.Logf("id=%s primary_majority_n=%d", id, persisted.N)
}

func replicaProxyDiscovered(t *testing.T, adapter *Adapter) []string {
	t.Helper()
	deployment, ok := adapter.deployment.(*topology.Topology)
	if !ok {
		t.Fatal("production topology unavailable")
	}
	servers := deployment.Description().Servers
	addresses := make([]string, len(servers))
	for i, server := range servers {
		addresses[i] = server.Addr.String()
	}
	return addresses
}

func replicaProxyCoverage(t *testing.T, adapter *Adapter, proxy *testmongo.ReplicaProxy, primary int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		discovered := replicaProxyDiscovered(t, adapter)
		err := proxy.CheckCoverage(discovered, primary)
		deployment := adapter.deployment.(*topology.Topology)
		selected := ""
		for _, server := range deployment.Description().Servers {
			healthy := server.Kind == description.ServerKindRSPrimary || server.Kind == description.ServerKindRSSecondary
			if healthy && server.HeartbeatInterval != time.Second {
				t.Fatal("actual replica polling interval", server.HeartbeatInterval)
			}
			if server.Kind == description.ServerKindRSPrimary {
				selected = server.Addr.String()
			}
		}
		if err == nil && selected == proxy.Addresses()[primary] {
			for i, snapshot := range proxy.Snapshots() {
				for _, connection := range snapshot.Connections {
					t.Logf("member=%d frontend=%s actual=%s connection=%d TLS_front=%t TLS_upstream=%t discovery_replies=%d command_counts=%v", i, snapshot.Frontend, snapshot.Upstream, connection.ID, connection.FrontendTLS, connection.UpstreamTLS, connection.DiscoveryReplies, connection.Commands)
				}
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("actual discovered/selected frontend and monitor/data coverage", err, discovered, selected)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func replicaProxyOriginal(t *testing.T, fixture *testmongo.Replica) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	if err := fixture.CheckOriginalAddresses(ctx); err != nil {
		t.Fatal("direct original hello/config changed", err)
	}
	t.Logf("members=%v", fixture.Addresses())
}

func replicaProxyRecover(t *testing.T, ctx context.Context, adapter *Adapter, database string) {
	t.Helper()
	// Only the immutable initial document is retried. Business writes below are
	// new unique IDs, never resubmitted after an uncertain outcome.
	for {
		work := prepareCounter(t, adapter, database+"/records/s:initial")
		plans := []*execution.Plan{work}
		replies := adapter.executeRecords(ctx, plans)
		if len(replies) == 1 && replies[0].GetReadResult().GetDocument() != nil && replies[0].GetReadResult().GetFailure() == nil {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("same-adapter read recovery exceeded deadline", ctx.Err())
		}
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
	}
}
