package weirclient

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/protobuf/proto"
)

type ingressReadAdapter struct{}

func (a *ingressReadAdapter) PrepareCall(id uint64, call *pb.Call) (*execution.Plan, *pb.Failure) {
	request := call.GetRead()
	if request == nil {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
	}
	variant := &pb.Operation_Read{Read: request}
	operation := &pb.Operation{Index: id, Operation: variant}
	plan := &execution.Plan{ID: id, Call: call, Operation: operation, Key: request.Resource, BatchKey: "reads", Bytes: proto.Size(call) + protocol.EntryOverhead, ResultBytes: protocol.ResultOverhead, WorkingBytes: protocol.ResultOverhead}
	return plan, nil
}

func (a *ingressReadAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
	for _, plan := range plans {
		read := protocol.Missing()
		variant := &pb.Result_Read{Read: read}
		result := &pb.Result{Index: plan.ID, Result: variant}
		value := &pb.Event_Result{Result: result}
		event := &pb.Event{Version: 1, Value: value}
		if err := emit(plan, event); err != nil {
			return execution.Neutral
		}
	}
	return execution.Healthy
}

func (a *ingressReadAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure { return nil }
func (a *ingressReadAdapter) Close() error                                           { return nil }

func TestOpenAndRefreshAtApplicationConnectionLimit(t *testing.T) {
	for _, count := range []int{1, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			address := listener.Addr().String()
			cfg := directory.Config{Group: "directory-fixture"}
			d, err := directory.New(cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = d.Close(t.Context()) })
			names := make([]string, count)
			runtimes := make(map[string]*store.Runtime, count)
			for i := range names {
				name := fmt.Sprintf("records-%02d", i)
				names[i] = name
				adapter := &ingressReadAdapter{}
				limits := store.DefaultLimits()
				limits.Collect = 0
				runtime, err := store.New(adapter, limits)
				if err != nil {
					t.Fatal(err)
				}
				runtimes[name] = runtime
				t.Cleanup(func() {
					ctx, cancel := context.WithTimeout(context.Background(), time.Second)
					defer cancel()
					if err := runtime.Close(ctx); err != nil {
						t.Error(err)
					}
				})
			}
			// Short advertisements exercise refresh past the original cache lease
			// using the actual directory protocol without a thirty-second test.
			advertisement := &peerpb.NodeAnnouncement{IncarnationId: strings.Repeat("1", 32), Revision: 1, ReplicaGroup: "records-group", StoreNames: names, StoreEndpoints: []string{address}, LeaseRemainingMs: 2000}
			publish := func() error {
				request := &peerpb.SyncDirectoryRequest{Announcements: []*peerpb.NodeAnnouncement{advertisement}}
				_, err := d.SyncDirectory(context.Background(), request)
				return err
			}
			if err := publish(); err != nil {
				t.Fatal(err)
			}
			publisher, stopPublisher := context.WithCancel(t.Context())
			published := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(100 * time.Millisecond)
				defer ticker.Stop()
				for {
					select {
					case <-publisher.Done():
						published <- nil
						return
					case <-ticker.C:
						advertisement.Revision++
						if err := publish(); err != nil {
							published <- err
							return
						}
					}
				}
			}()
			t.Cleanup(func() {
				stopPublisher()
				if err := <-published; err != nil {
					t.Error(err)
				}
			})
			limits := server.DefaultLimits()
			limits.Connections = count
			admission, err := server.NewAdmission(limits)
			if err != nil {
				t.Fatal(err)
			}
			serverConfig := server.Config{Stores: runtimes, Directory: d, Limits: limits, Admission: admission}
			application, err := server.New(serverConfig)
			if err != nil {
				t.Fatal(err)
			}
			served := make(chan error, 1)
			go func() { served <- application.Serve(listener) }()
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				if err := application.Shutdown(ctx); err != nil {
					t.Error(err)
				}
				if err := <-served; err != nil {
					t.Error(err)
				}
			})
			options := OpenOptions{Seed: address, Stores: names, RefreshInterval: 50 * time.Millisecond}
			client := openDiscovery(t, options)
			original := make(map[string]time.Time, count)
			until := time.Now()
			for name, entry := range client.stores {
				entry.mu.RLock()
				original[name] = entry.expires
				if entry.expires.After(until) {
					until = entry.expires
				}
				entry.mu.RUnlock()
			}
			until = until.Add(300 * time.Millisecond)
			for time.Now().Before(until) {
				for _, name := range names {
					discoveryRead(t, client, name)
				}
				time.Sleep(25 * time.Millisecond)
			}
			for name, entry := range client.stores {
				entry.mu.RLock()
				refreshed := entry.expires.After(original[name])
				entry.mu.RUnlock()
				if !refreshed {
					t.Fatalf("Store %s failed to refresh with %d occupied application connections", name, count)
				}
			}
		})
	}
}

func TestRefreshPrioritizesUnrenewedStoresAfterRoundDeadline(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	peer := &discoveryPeer{business: business}
	listener := listenDiscovery(t, peer, "127.0.0.1:0")
	names := make([]string, 16)
	for i := range names {
		name := fmt.Sprintf("records-%02d", i)
		names[i] = name
		response := discoveryRecord(name, listener.address)
		response.CacheTtlMs = 5000
		peer.set(name, response)
	}
	options := OpenOptions{Seed: listener.address, Stores: names, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 2 * time.Second}
	client := openDiscovery(t, options)
	original := make(map[string]time.Time, len(names))
	for name, entry := range client.stores {
		entry.mu.RLock()
		original[name] = entry.expires
		entry.mu.RUnlock()
	}
	peer.mu.Lock()
	peer.delay = 300 * time.Millisecond
	peer.mu.Unlock()
	until := time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		for _, name := range names {
			discoveryRead(t, client, name)
		}
		time.Sleep(75 * time.Millisecond)
	}
	for name, entry := range client.stores {
		entry.mu.RLock()
		refreshed := entry.expires.After(original[name])
		entry.mu.RUnlock()
		if !refreshed {
			t.Fatalf("deadline repeatedly starved Store %s", name)
		}
	}
	started := time.Now()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if time.Since(started) > 500*time.Millisecond {
		t.Fatal("Close did not cancel active bounded refresh round")
	}
}

func TestRefreshFailuresCannotStarveHealthyTail(t *testing.T) {
	business := &clientTestPeer{mode: "normal"}
	peer := &discoveryPeer{business: business}
	listener := listenDiscovery(t, peer, "127.0.0.1:0")
	names := []string{"abandoned-a", "abandoned-b", "abandoned-c", "abandoned-d", "records"}
	for _, name := range names {
		response := discoveryRecord(name, listener.address)
		response.CacheTtlMs = 5000
		peer.set(name, response)
	}
	options := OpenOptions{Seed: listener.address, Stores: names, RefreshInterval: 50 * time.Millisecond, ResolveTimeout: 2 * time.Second}
	client := openDiscovery(t, options)
	peer.mu.Lock()
	peer.delays = make(map[string]time.Duration)
	for _, name := range names[:4] {
		peer.records[name] = nil
		peer.delays[name] = time.Second
	}
	peer.mu.Unlock()
	until := time.Now().Add(6 * time.Second)
	for time.Now().Before(until) {
		discoveryRead(t, client, "records")
		time.Sleep(75 * time.Millisecond)
	}
	entry := client.stores["records"]
	entry.mu.RLock()
	valid := time.Now().Before(entry.expires)
	entry.mu.RUnlock()
	if !valid {
		t.Fatal("repeated failing control lookups starved the healthy Store")
	}
}
