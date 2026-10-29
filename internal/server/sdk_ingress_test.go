package server_test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	weirclient "github.com/batchstream/weir-go"
	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/server"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type sdkIngressReadAdapter struct{}

func (a *sdkIngressReadAdapter) PrepareNative(context.Context, *pb.Command) (*execution.Plan, *pb.Failure) {
	return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
}

func (a *sdkIngressReadAdapter) PrepareRecord(ctx context.Context, record *execution.Record) (*execution.Plan, *pb.Failure) {
	command := record.Command()
	request := command.GetRead()
	if request == nil {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
	}
	plan := &execution.Plan{Command: command, Key: request.Resource, BatchKey: "reads", Bytes: record.Bytes()}
	return plan, nil
}

func (a *sdkIngressReadAdapter) ExecuteRecords(_ context.Context, plans []*execution.Plan) []*pb.Event {
	events := make([]*pb.Event, len(plans))
	for i := range plans {
		read := protocol.Missing()
		value := &pb.Event_ReadResult{ReadResult: read}
		events[i] = &pb.Event{Value: value}
	}
	return events
}

func (a *sdkIngressReadAdapter) ExecuteNative(context.Context, *execution.Plan, execution.Emit) *pb.NativeEnd {
	failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "read fixture")
	return protocol.NativeFailure(false, failure)
}

func (a *sdkIngressReadAdapter) Close(context.Context) error { return nil }

func TestSDKReadsEveryHostedStoreThroughOneAddress(t *testing.T) {
	for _, count := range []int{1, 16} {
		t.Run(fmt.Sprint(count), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = listener.Close() })
			names := make([]string, count)
			runtimes := make(map[string]*store.Runtime, count)
			for i := range names {
				name := fmt.Sprintf("records-%02d", i)
				names[i] = name
				adapter := &sdkIngressReadAdapter{}
				limits := store.DefaultLimits()
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
			limits := server.DefaultLimits()
			admission := server.NewAdmission()
			serverConfig := server.Config{Stores: runtimes, Limits: limits, Admission: admission}
			application := server.New(serverConfig)
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
			client, err := weirclient.New(listener.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			for _, name := range names {
				sdkIngressRead(t, client, name)
			}
			request := &weirclient.ReadRequest{Resource: "records/s:key"}
			missing := weirclient.ReadOptions{StoreName: "missing", Requests: []*weirclient.ReadRequest{request}}
			if _, err := client.Read(t.Context(), missing); status.Code(err) != codes.NotFound {
				t.Fatal("unhosted Store did not fail with NotFound through the SDK", err)
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
		t.Fatalf("Store %s batch read failed: results=%v error=%v", storeName, results, err)
	}
	for i, result := range results {
		if result == nil || !result.Missing {
			t.Fatalf("Store %s batch item %d failed: result=%v", storeName, i, result)
		}
	}
}
