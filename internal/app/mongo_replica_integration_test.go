//go:build integration

package app

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/testutil/testmetrics"
	"github.com/batchstream/weir/internal/testutil/testmongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

// TestMongoReplicaCLIRecovery checks that one CLI process recovers across
// replica-set topology changes over plain and TLS connections.
func TestMongoReplicaCLIRecovery(t *testing.T) {
	binary := buildWeirBinary(t)
	for _, secure := range []bool{false, true} {
		name := "plain"
		if secure {
			name = "tls"
		}
		t.Run(name, func(t *testing.T) {
			fixtureOptions := testmongo.ReplicaOptions{TLS: secure}
			fixture := testmongo.OpenReplica(t, fixtureOptions)
			external, stop := context.WithTimeout(t.Context(), 10*time.Second)
			primary, _, err := fixture.WaitUsable(external, -1)
			stop()
			if err != nil {
				t.Fatal(err)
			}
			seeds := []string{fixture.Addresses()[(primary+1)%3]}
			mongo := &MongoDB{URI: fixture.SeedURI(seeds)}
			store := StoreConfig{Name: "mongo", MongoDB: mongo}
			if secure {
				credential := &Credentials{Username: fixture.Username, Password: fixture.Password}
				trust := &TLS{CAFile: fixture.CAFile}
				store.Authentication, store.TLS = credential, trust
			}
			cfg := emptyConfig(t)
			cfg.Listeners.Diagnostics = "127.0.0.1:0"
			cfg.Stores = []StoreConfig{store}
			service := startProcessWithLifetime(t, binary, cfg, 3*time.Minute)
			pid := service.command.Process.Pid
			client, err := weirclient.New(service.address)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			initial, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			work := replicaCLIWorkOptions{Client: client, Fixture: fixture, ID: "initial"}
			replicaCLIWork(t, initial, work)
			cancel()
			// Seed an immutable health record independently. Read-only recovery probes
			// may repeat; a business mutation from a failed attempt never does.
			seed, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			health := bson.D{{Key: "_id", Value: "health"}, {Key: "n", Value: int32(0)}}
			_, err = fixture.Admin.Database(fixture.DB).Collection("records").InsertOne(seed, health)
			cancel()
			if err != nil {
				t.Fatal(err)
			}
			for _, fault := range []string{"stepdown", "primary_loss"} {
				started := time.Now()
				if fault == "stepdown" {
					command, cancel := context.WithTimeout(t.Context(), 10*time.Second)
					err := fixture.StepDown(command, primary)
					cancel()
					if err != nil {
						t.Fatal("owned stepdown command", err)
					}
				} else {
					fixture.LosePrimary(t, primary)
				}
				// A normal database election is an external prerequisite, not Weir's 5s clock.
				external, cancel := context.WithTimeout(t.Context(), 45*time.Second)
				next, usable, err := fixture.WaitUsable(external, primary)
				cancel()
				if err != nil {
					t.Fatal("external majority service did not recover", err)
				}
				recovered, cancel := context.WithDeadline(t.Context(), usable.Add(5*time.Second))
				for {
					err := replicaCLIHealth(recovered, client, fixture.DB)
					if err == nil {
						break
					}
					if recovered.Err() != nil {
						cancel()
						t.Fatal("same SDK primary recovery exceeded 5s", err)
					}
					select {
					case <-recovered.Done():
					case <-time.After(25 * time.Millisecond):
					}
				}
				firstSuccess := time.Now()
				work.ID = fault
				replicaCLIWork(t, recovered, work)
				cancel()
				if time.Since(usable) > 5*time.Second || service.command.Process.Pid != pid {
					t.Fatal("recovery gate or process identity changed")
				}
				t.Logf("TLS=%t fault=%s old_primary=%d new_primary=%d DB_transition=%s Weir_first_success=%s Weir_full_focused_work=%s PID=%d", secure, fault, primary, next, usable.Sub(started), firstSuccess.Sub(usable), time.Since(usable), pid)
				primary = next
			}
			// One member is already lost. Losing the current primary removes quorum.
			fixture.LosePrimary(t, primary)
			unavailable, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() { done <- replicaCLIHealth(unavailable, client, fixture.DB) }()
			replicaCLIActive(t, service, done)
			cancelled := time.Now()
			cancel()
			select {
			case err := <-done:
				cancellationElapsed := time.Since(cancelled)
				t.Logf("TLS=%t same_PID=%d unavailable_cancellation_elapsed=%s caller_error=%t", secure, pid, cancellationElapsed, err != nil)
				if err == nil || cancellationElapsed > 3*time.Second {
					t.Fatal("SDK cancellation gate", err)
				}
			case <-time.After(3 * time.Second):
				t.Logf("TLS=%t same_PID=%d unavailable_cancellation_elapsed=%s completion_observed=false", secure, pid, time.Since(cancelled))
				t.Fatal("SDK unavailable selection cancellation exceeded 3s")
			}
			replicaCLIIdle(t, service, cancelled.Add(3*time.Second))
			pending, cancel := context.WithTimeout(t.Context(), 12*time.Second)
			defer cancel()
			done = make(chan error, 1)
			go func() { done <- replicaCLIHealth(pending, client, fixture.DB) }()
			replicaCLIActive(t, service, done)
			select {
			case err := <-done:
				t.Fatal("pending caller returned before SIGTERM", err)
			default:
			}
			if pending.Err() != nil {
				t.Fatal("pending caller not live before SIGTERM", pending.Err())
			}
			deadline, _ := pending.Deadline()
			t.Logf("TLS=%t PID=%d remaining_deadline=%s", secure, pid, time.Until(deadline))
			terminal := service.stop(t)
			t.Logf("TLS=%t same_PID=%d SIGTERM_elapsed=%s exit_valid=%t exit_code=%d timed_out=%t wait_error=%v", secure, pid, terminal.Elapsed, terminal.ExitCodeValid, terminal.ExitCode, terminal.TimedOut, terminal.WaitErr)
			if pending.Err() != nil {
				t.Error("pending context expired or cancelled during CLI shutdown", pending.Err())
			}
			if !terminal.successful() {
				return
			}
			select {
			case err := <-done:
				t.Logf("TLS=%t pending_completion caller_error=%t caller_context_uncancelled=%t", secure, err != nil, pending.Err() == nil)
			case <-time.After(time.Second):
				t.Fatal("pending caller survived shutdown")
			}
			if err := client.Close(); err != nil {
				t.Error(err)
			}
			replicaClosedOwner(t, service)
			t.Logf("TLS=%t same_PID=%d focused lifecycle gates passed", secure, pid)
		})
	}
}

func replicaCLIHealth(ctx context.Context, client *weirclient.Client, database string) error {
	request := &weirclient.ReadRequest{Resource: database + "/records/s:health"}
	opts := weirclient.ReadOneOptions{StoreName: "mongo", Request: request}
	result, err := client.ReadOne(ctx, opts)
	if err != nil {
		return err
	}
	if result == nil || result.Failure != nil || result.Document == nil {
		return fmt.Errorf("health document unavailable")
	}
	var value struct {
		N int32 `bson:"n"`
	}
	if bson.Unmarshal(result.Document.Data, &value) != nil || value.N != 0 {
		return fmt.Errorf("health document changed")
	}
	return nil
}

type replicaCLIWorkOptions struct {
	Client  *weirclient.Client
	Fixture *testmongo.Replica
	ID      string
}

func replicaCLIWork(t *testing.T, ctx context.Context, opts replicaCLIWorkOptions) {
	t.Helper()
	client, fixture, id := opts.Client, opts.Fixture, opts.ID
	resource := fixture.DB + "/records/s:" + id
	value := bson.D{{Key: "_id", Value: id}, {Key: "n", Value: int32(0)}}
	raw, err := bson.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	document := &pb.Document{ContentType: "application/bson", Data: raw}
	write := &weirclient.WriteRequest{Resource: resource, Document: document}
	put := weirclient.WriteOptions{StoreName: "mongo", Request: write}
	result, err := client.Put(ctx, put)
	if err != nil || result == nil || result.Outcome != weirclient.MutationApplied || result.Failure != nil {
		t.Fatal("new typed mutation", result, err)
	}
	read := &weirclient.ReadRequest{Resource: resource}
	readOptions := weirclient.ReadOneOptions{StoreName: "mongo", Request: read}
	found, err := client.ReadOne(ctx, readOptions)
	if err != nil || found == nil || found.Failure != nil || found.Document == nil {
		t.Fatal("new typed read", found, err)
	}
	var typed struct {
		N int32 `bson:"n"`
	}
	if bson.Unmarshal(found.Document.Data, &typed) != nil || typed.N != 0 {
		t.Fatal("typed read value")
	}
	filter := bson.D{{Key: "_id", Value: id}}
	amount := bson.D{{Key: "n", Value: int32(1)}}
	update := bson.D{{Key: "$inc", Value: amount}}
	concern := bson.D{{Key: "w", Value: "majority"}}
	command := bson.D{{Key: "findAndModify", Value: "records"}, {Key: "query", Value: filter}, {Key: "update", Value: update}, {Key: "new", Value: true}, {Key: "writeConcern", Value: concern}}
	raw, err = bson.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	nativeDocument := &pb.Document{ContentType: "application/bson", Data: raw}
	nativeRequest := &pb.NativeRequest{Resource: fixture.DB, Request: nativeDocument}
	var body []byte
	consume := func(_ context.Context, _ *weirclient.NativeResponse, chunk []byte) error {
		if len(body)+len(chunk) > 64<<10 {
			return fmt.Errorf("small Native response exceeded bound")
		}
		body = append(body, chunk...)
		return nil
	}
	native := weirclient.NativeOptions{StoreName: "mongo", Request: nativeRequest, Consume: consume}
	response, err := client.Native(ctx, native)
	if err != nil || response == nil || response.Failure != nil || response.Completion != weirclient.NativeResponseComplete {
		t.Fatal("new Native mutation", response, err)
	}
	var reply struct {
		OK    float64 `bson:"ok"`
		Value struct {
			N int32 `bson:"n"`
		} `bson:"value"`
	}
	if bson.Unmarshal(body, &reply) != nil || reply.OK != 1 || reply.Value.N != 1 {
		t.Fatal("Native reply effect")
	}
	lua := &weirclient.LuaTransform{Source: []byte(`return function(current, incoming) current.n = current.n + 1; return current end`)}
	transform := &weirclient.AtomicTransformRequest{Resource: resource, Lua: lua}
	transformOptions := weirclient.AtomicTransformOptions{StoreName: "mongo", Request: transform}
	applied, err := client.AtomicTransform(ctx, transformOptions)
	if err != nil || applied == nil || applied.Failure != nil || applied.Outcome != weirclient.MutationApplied {
		t.Fatal("new Lua transaction", applied, err)
	}
	var persisted struct {
		N int32 `bson:"n"`
	}
	err = fixture.Admin.Database(fixture.DB).Collection("records").FindOne(ctx, filter).Decode(&persisted)
	if err != nil || persisted.N != 2 {
		t.Fatal("independent persisted typed/Native/Lua oracle", persisted.N, err)
	}
}

// Require an actual backend execution and an uncompleted caller before testing
// cancellation or SIGTERM. A slow scrape cannot move the absolute 3s boundary.
func replicaCLIActive(t *testing.T, service *process, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		select {
		case err := <-done:
			t.Fatal("caller returned before active execution barrier", err)
		default:
		}
		metrics := testmetrics.Scrape(t, service.diagnostic)
		if time.Now().After(deadline) {
			t.Fatal("active execution barrier exceeded 3s")
		}
		if metrics["weir_store_active_executions"] == nil {
			t.Fatal("missing active execution gauge")
		}
		active := storeActive(metrics)
		if active >= 1 {
			select {
			case err := <-done:
				t.Fatal("caller returned at active execution barrier", err)
			default:
			}
			if time.Now().After(deadline) {
				t.Fatal("active execution barrier exceeded 3s")
			}
			t.Logf("observed_active_executions=%g", active)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func replicaCLIIdle(t *testing.T, service *process, deadline time.Time) {
	t.Helper()
	for {
		metrics := testmetrics.Scrape(t, service.diagnostic)
		idle := true
		for _, name := range []string{"weir_store_queued_operations", "weir_store_active_executions", "weir_store_active_natives"} {
			if metrics[name] == nil {
				t.Fatal("missing cleanup gauge", name)
			}
			idle = idle && testmetrics.Sum(metrics, name) == 0
		}
		if time.Now().After(deadline) {
			t.Fatal("cancelled work ownership did not recover within 3s")
		}
		if idle {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func replicaClosedOwner(t *testing.T, service *process) {
	t.Helper()
	pattern := regexp.MustCompile(`backend_connections_closed backend=mongo store=mongo owned=(\d+) peak=(\d+) dialing=(\d+) closing=(\d+) acquired=(\d+) released=(\d+)`)
	matches := pattern.FindAllStringSubmatch(service.stderr.String(), -1)
	if len(matches) != 1 {
		t.Fatal("missing final connection-owner evidence")
	}
	match := matches[0]
	if match[1] != "0" || match[3] != "0" || match[4] != "0" || match[5] != match[6] {
		t.Fatal("final connection ownership not zero", match[1:])
	}
	peak, _ := strconv.Atoi(match[2])
	if peak < 4 {
		t.Fatal("three member topology/data sockets not observed")
	}
	t.Logf("final_connection_owner owned=%s peak=%s dialing=%s closing=%s acquired=%s released=%s", match[1], match[2], match[3], match[4], match[5], match[6])
}
