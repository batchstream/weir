package server

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/status"
)

func readFrame(index uint64, requests []*pb.ReadRequest) *pb.ExecuteRequest {
	batch := &pb.ReadBatch{Requests: requests}
	operation := &pb.Command_Read{Read: batch}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: "records", Index: index, Command: command}
	return request
}

func TestRecordStreamLongInputAndLargeResultsStayBounded(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	document := &pb.Document{MediaType: "application/octet-stream", Data: bytes.Repeat([]byte("x"), protocol.MaxDocument)}
	adapter.documents[testRequest().Resource] = document
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	const count = 80
	first := readFrame(1, []*pb.ReadRequest{testRequest()})
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= count; index++ {
		response, err := stream.Recv()
		if err != nil || response.Index != index || len(response.Event.GetReadResult().GetDocument().GetData()) != protocol.MaxDocument {
			t.Fatal("lost indexed large result", index, response, err)
		}
		snapshot := local.Snapshot()
		if snapshot.ResultBytes > store.DefaultLimits().ResultBytes || snapshot.Retained > 1 || snapshot.Publishers != 0 {
			t.Fatal("logical stream accumulated result owners", snapshot)
		}
		if index < count {
			request := readFrame(index+1, []*pb.ReadRequest{testRequest()})
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal("long stream did not complete", err)
	}
	waitPeerIdle(t, server)
}

func TestRecordStreamInvalidLaterFramePreservesPublishedResults(t *testing.T) {
	for _, scenario := range []string{"ordinal", "Store", "kind", "request"} {
		t.Run(scenario, func(t *testing.T) {
			adapter, local := peerLocal(t, "records")
			opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
			server, address := startPeerServer(t, opts)
			_, client := peerClient(t, address)
			stream, err := client.Execute(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			request := readFrame(1, []*pb.ReadRequest{testRequest()})
			if err := stream.Send(request); err != nil {
				t.Fatal(err)
			}
			response, err := stream.Recv()
			if err != nil || response.Index != 1 || response.Event.GetReadResult().GetMissing() == nil {
				t.Fatal("first frame failed", response, err)
			}
			later := readFrame(2, []*pb.ReadRequest{testRequest()})
			switch scenario {
			case "ordinal":
				later.Index = 3
			case "Store":
				later.StoreName = "other"
			case "kind":
				batch := &pb.MutationBatch{Requests: []*pb.MutateRequest{testMutation("later")}}
				later.Command.Operation = &pb.Command_Mutate{Mutate: batch}
			case "request":
				later.Command.GetRead().Requests[0] = &pb.ReadRequest{Resource: "../bad"}
			}
			if err := stream.Send(later); err != nil {
				t.Fatal(err)
			}
			if err := stream.CloseSend(); err != nil {
				t.Fatal(err)
			}
			if response, err := stream.Recv(); response != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatal("invalid later frame accepted", response, err)
			}
			if adapter.commands.Load() != 0 {
				t.Fatal("invalid later frame reached mutation execution")
			}
			waitPeerIdle(t, server)
		})
	}
}

type ownedOutputStream struct {
	grpc.ServerStream
	ctx     context.Context
	codec   *responseCodec
	encoded chan mem.BufferSlice
}

func (stream *ownedOutputStream) Context() context.Context   { return stream.ctx }
func (*ownedOutputStream) Recv() (*pb.ExecuteRequest, error) { return nil, io.EOF }
func (stream *ownedOutputStream) Send(response *pb.ExecuteResponse) error {
	encoded, err := stream.codec.Marshal(response)
	if err != nil {
		return err
	}
	stream.encoded <- encoded
	return nil
}

func TestRecordWindowWaitsForFinalEncodedOwnership(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	request := readFrame(1, []*pb.ReadRequest{testRequest()})
	prepared, count, failure := local.PrepareWindow("records", request.Command, 0)
	if failure != nil || count != 1 {
		t.Fatal(failure, count)
	}
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: t.Context(), codec: codec, encoded: make(chan mem.BufferSlice, 1)}
	completed := make(chan error, 1)
	go func() { completed <- server.executeWindow(stream, local, prepared, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	encoded.Ref()
	encoded.Free()
	snapshot := local.Snapshot()
	if snapshot.Retained != 1 || snapshot.ResultBytes == 0 || server.admission.wireBytes.Load() == 0 {
		t.Fatal("window released before final transport owner", snapshot)
	}
	select {
	case err := <-completed:
		t.Fatal("publication returned while transport still owns bytes", err)
	default:
	}
	encoded.Free()
	encoded = nil
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	waitPeerIdle(t, server)
}

func TestRecordStreamIndexedOperationFailureDoesNotStopLaterResults(t *testing.T) {
	adapter, local := peerLocal(t, "records")
	adapter.ackFailureKey = testRequest().Resource
	adapter.ackFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "acknowledgement unavailable")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startPeerServer(t, opts)
	_, client := peerClient(t, address)
	stream, err := client.Execute(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	items := []*pb.MutateRequest{testMutation("first"), testMutation("second")}
	items[1].Resource = "records/s:other"
	batch := &pb.MutationBatch{Requests: items}
	operation := &pb.Command_Mutate{Mutate: batch}
	command := &pb.Command{Operation: operation}
	request := &pb.ExecuteRequest{StoreName: "records", Index: 1, Command: command}
	if err := stream.Send(request); err != nil {
		t.Fatal(err)
	}
	if err := stream.CloseSend(); err != nil {
		t.Fatal(err)
	}
	for index := uint64(1); index <= 2; index++ {
		response, err := stream.Recv()
		if err != nil || response.Index != index {
			t.Fatal("indexed result missing", response, err)
		}
		result := response.Event.GetMutationResult()
		if result == nil || result.Outcome != pb.MutationOutcome_APPLIED || (result.Failure != nil) != (index == 1) {
			t.Fatalf("incorrect operation evidence at %d: %v", index, result)
		}
	}
	if _, err := stream.Recv(); err != io.EOF {
		t.Fatal(err)
	}
	waitPeerIdle(t, server)
}

func TestRecordWindowCancellationReleasesSourceButKeepsEncodedCredits(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	request := readFrame(1, []*pb.ReadRequest{testRequest()})
	prepared, _, failure := local.PrepareWindow("records", request.Command, 0)
	if failure != nil {
		t.Fatal(failure)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: ctx, codec: codec, encoded: make(chan mem.BufferSlice, 1)}
	completed := make(chan error, 1)
	go func() { completed <- server.executeWindow(stream, local, prepared, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	cancel()
	if err := <-completed; status.Code(err) != codes.Canceled {
		t.Fatal("output cancellation lost its status", err)
	}
	if snapshot := local.Snapshot(); snapshot.Retained != 0 || snapshot.ResultBytes != 0 {
		t.Fatal("canceled stream retained source documents", snapshot)
	}
	if server.admission.wireBytes.Load() == 0 {
		t.Fatal("cancellation returned still-owned encoded credits")
	}
	server.admission.responses.Range(func(key, value any) bool {
		t.Error("canceled publication retained a source message", key)
		return true
	})
	encoded.Free()
	encoded = nil
	waitPeerIdle(t, server)
}

func TestOutputWatchdogTransfersToEncodedBufferAfterSend(t *testing.T) {
	_, local := peerLocal(t, "records")
	opts := peerServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, _ := startPeerServer(t, opts)
	server.limits.Stall = 50 * time.Millisecond
	codec := &responseCodec{admission: server.admission}
	stream := &ownedOutputStream{ctx: t.Context(), codec: codec, encoded: make(chan mem.BufferSlice)}
	read := protocol.Missing()
	value := &pb.Event_ReadResult{ReadResult: read}
	event := &pb.Event{Value: value}
	completed := make(chan error, 1)
	go func() { completed <- server.sendEvent(stream, event, 1) }()
	encoded := <-stream.encoded
	defer func() { encoded.Free() }()
	var owner *responseBufferOwner
	server.admission.responses.Range(func(key, value any) bool {
		owner = value.(*responseBufferOwner)
		return false
	})
	if owner == nil {
		t.Fatal("mock transport did not retain encoded ownership")
	}
	// No OutPayload stats notification has started the buffer watchdog yet.
	// A completed Send must not leave its earlier watchdog running.
	time.Sleep(2 * server.limits.Stall)
	metric := &dto.Metric{}
	counter := server.metrics.watchdogs.WithLabelValues("output")
	if err := counter.Write(metric); err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 0 {
		t.Fatal("Send watchdog remained active after Send returned", metric)
	}
	owner.watch(t.Context(), server)
	deadline := time.Now().Add(time.Second)
	for {
		if err := counter.Write(metric); err != nil {
			t.Fatal(err)
		}
		if metric.GetCounter().GetValue() > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("encoded ownership watchdog did not run")
		}
		time.Sleep(time.Millisecond)
	}
	encoded.Free()
	encoded = nil
	if err := <-completed; err != nil {
		t.Fatal(err)
	}
	if metric.GetCounter().GetValue() != 1 {
		t.Fatal("one stalled encoded owner produced duplicate expirations", metric)
	}
	waitPeerIdle(t, server)
}
