package server_test

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	peerpb "github.com/batchstream/weir/internal/api/peer/v1"
	"github.com/batchstream/weir/internal/directory"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/protobuf/proto"
)

type sdkIngressReadAdapter struct{}

func (a *sdkIngressReadAdapter) PrepareCommand(id uint64, call *pb.Command) (*execution.Plan, *pb.Failure) {
	request := call.GetRead()
	if request == nil {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
	}
	variant := &pb.Operation_Read{Read: request}
	operation := &pb.Operation{Index: id, Operation: variant}
	plan := &execution.Plan{ID: id, Command: call, Operation: operation, Key: request.Resource, BatchKey: "reads", Bytes: proto.Size(call) + protocol.EntryOverhead, ResultBytes: protocol.ResultOverhead, WorkingBytes: protocol.ResultOverhead}
	return plan, nil
}

func (a *sdkIngressReadAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) execution.Feedback {
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

func (a *sdkIngressReadAdapter) ClosePlan(context.Context, *execution.Plan) *pb.Failure { return nil }
func (a *sdkIngressReadAdapter) Close() error                                           { return nil }

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
				adapter := &sdkIngressReadAdapter{}
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
			options := weirclient.OpenOptions{Seed: address, Stores: names, RefreshInterval: 50 * time.Millisecond}
			initialize, initializeCancel := context.WithTimeout(t.Context(), 5*time.Second)
			client, err := weirclient.Open(initialize, options)
			initializeCancel()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			// Every initial ResolveStore response has at most a two-second TTL.
			// Public reads beyond that boundary prove the occupied business
			// connections continue serving control refresh without a spare slot.
			until := time.Now().Add(2300 * time.Millisecond)
			for time.Now().Before(until) {
				for _, name := range names {
					sdkIngressRead(t, client, name)
				}
				time.Sleep(25 * time.Millisecond)
			}
		})
	}
}

func sdkIngressRead(t *testing.T, client *weirclient.Client, storeName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	request := &weirclient.ReadRequest{Resource: "records/s:key"}
	options := weirclient.ReadOptions{StoreName: storeName, Request: request}
	result, err := client.Read(ctx, options)
	if err != nil || !result.GetMissing() {
		t.Fatalf("Store %s direct read failed across directory refresh: result=%v error=%v", storeName, result, err)
	}
}
