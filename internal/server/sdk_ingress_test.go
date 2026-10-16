package server_test

import (
	"context"
	"fmt"
	"io"
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
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type sdkIngressReadAdapter struct{}

func (a *sdkIngressReadAdapter) PrepareCommand(context.Context, uint64, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
}

func (a *sdkIngressReadAdapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	command := record.Command()
	request := command.GetRead()
	if request == nil {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
	}
	plan := &execution.Plan{ID: record.Index(), Command: command, Key: request.Resource, BatchKey: "reads", Bytes: record.Bytes() + execution.EntryOverheadBytes, ResultBytes: execution.ResultOverheadBytes, WorkingBytes: execution.ResultOverheadBytes}
	return plan, nil
}

func (a *sdkIngressReadAdapter) Execute(_ context.Context, plans []*execution.Plan, emit execution.Emit) {
	for _, plan := range plans {
		read := protocol.Missing()
		result := &pb.Event{Value: &pb.Event_ReadResult{ReadResult: read}}
		output := result
		if err := emit(plan, output); err != nil {
			return
		}
	}
	return
}

func (a *sdkIngressReadAdapter) Close() error { return nil }

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
			peerListener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			peerServer := grpc.NewServer()
			peerpb.RegisterPeerDiscoveryServiceServer(peerServer, d)
			peerDone := make(chan error, 1)
			go func() { peerDone <- peerServer.Serve(peerListener) }()
			t.Cleanup(func() {
				peerServer.Stop()
				_ = peerListener.Close()
				<-peerDone
			})
			peerConnection, err := grpc.NewClient("passthrough:///"+peerListener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithNoProxy())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = peerConnection.Close() })
			peerClient := peerpb.NewPeerDiscoveryServiceClient(peerConnection)
			names := make([]string, count)
			runtimes := make(map[string]*store.Runtime, count)
			for i := range names {
				name := fmt.Sprintf("records-%02d", i)
				names[i] = name
				adapter := &sdkIngressReadAdapter{}
				limits := store.DefaultLimits()
				runtime, err := store.New(adapter, limits, nil)
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
			advertisement := &peerpb.NodeAnnouncement{IncarnationId: strings.Repeat("1", 32), Revision: 1, ReplicaGroup: "records-group", StoreNames: names, StoreEndpoints: []string{address}, LeaseRemainingMs: 2000, OriginLeaseMs: 2000}
			publish := func(parent context.Context) error {
				request := &peerpb.SyncDirectoryRequest{Announcements: []*peerpb.NodeAnnouncement{advertisement}}
				ctx, cancel := context.WithTimeout(parent, time.Second)
				defer cancel()
				stream, err := peerClient.SyncDirectory(ctx)
				if err != nil {
					return err
				}
				opening := &peerpb.SyncDirectoryRequest{}
				if err := stream.Send(opening); err != nil {
					return err
				}
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if err := stream.Send(request); err != nil {
					return err
				}
				if err := stream.CloseSend(); err != nil {
					return err
				}
				_, err = stream.Recv()
				if err == io.EOF {
					return nil
				}
				return err
			}
			if err := publish(t.Context()); err != nil {
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
						if err := publish(publisher); err != nil {
							if publisher.Err() != nil {
								published <- nil
							} else {
								published <- err
							}
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
	requests := make([]*weirclient.ReadRequest, 17)
	for i := range requests {
		request := &weirclient.ReadRequest{Resource: fmt.Sprintf("records/s:key-%d", i)}
		requests[i] = request
	}
	options := weirclient.ReadOptions{StoreName: storeName, Requests: requests}
	results, err := client.Read(ctx, options)
	if err != nil || len(results) != len(requests) {
		t.Fatalf("Store %s batch read failed across directory refresh: results=%v error=%v", storeName, results, err)
	}
	for i, result := range results {
		if result == nil || !result.Missing {
			t.Fatalf("Store %s batch item %d failed across directory refresh: result=%v", storeName, i, result)
		}
	}
}
