package server

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func TestRequestsInOneMessageActAsIfSentSeparately(t *testing.T) {
	_, local := localStore(t, "records")
	opts := testServerOptions{stores: map[string]*store.Runtime{"records": local}}
	server, address := startTestServer(t, opts)
	_, client := testClient(t, address)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	stream, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	first := &pb.ExecuteRequests{Requests: []*pb.ExecuteRequest{readFrame(1, testRequest()), readFrame(2, testRequest()), cancelFrame(1), readFrame(3, testRequest())}}
	if err := stream.Send(first); err != nil {
		t.Fatal(err)
	}
	// The records before an invalid request keep their results.
	broken := readFrame(6, testRequest())
	second := &pb.ExecuteRequests{Requests: []*pb.ExecuteRequest{readFrame(4, testRequest()), broken}}
	if err := stream.Send(second); err != nil {
		t.Fatal(err)
	}
	seen := make(map[uint64]bool)
	for {
		message, err := stream.Recv()
		if err != nil {
			if status.Code(err) != codes.InvalidArgument {
				t.Fatal("broken request did not end the stream", err)
			}
			break
		}
		if err := protocol.ValidateExecuteResponses(message); err != nil {
			t.Fatal(err)
		}
		for _, response := range message.Responses {
			if seen[response.Index] || response.Event.GetReadResult() == nil {
				t.Fatal("unexpected result", response)
			}
			seen[response.Index] = true
		}
	}
	if len(seen) != 4 {
		t.Fatal("packed records lost results", seen)
	}
	empty, err := client.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	none := &pb.ExecuteRequests{}
	if err := empty.Send(none); err != nil {
		t.Fatal(err)
	}
	if _, err := empty.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatal("a message without requests was accepted", err)
	}
	waitIdle(t, server)
}

// packedOutputStream records each message the server sends.
type packedOutputStream struct {
	grpc.ServerStream
	ctx  context.Context
	sent []*pb.ExecuteResponses
}

func (stream *packedOutputStream) Context() context.Context { return stream.ctx }

func (stream *packedOutputStream) Recv() (*pb.ExecuteRequests, error) { return nil, io.EOF }

func (stream *packedOutputStream) Send(message *pb.ExecuteResponses) error {
	stream.sent = append(stream.sent, proto.Clone(message).(*pb.ExecuteResponses))
	return nil
}

func TestWaitingResultsLeaveInOneMessage(t *testing.T) {
	stream := &packedOutputStream{ctx: t.Context()}
	server := &Server{limits: DefaultLimits()}
	records := &recordStream{server: server, stream: stream, credits: make(chan struct{}, 4), results: make(chan store.Result, 4), records: make(map[uint64]*admission)}
	value := &pb.Event_ReadResult{ReadResult: protocol.Missing()}
	event := &pb.Event{Value: value}
	for index := uint64(1); index <= 4; index++ {
		ctx, cancel := context.WithCancel(t.Context())
		records.records[index] = &admission{ctx: ctx, cancel: cancel}
		records.credits <- struct{}{}
		records.pending++
		if index > 1 {
			records.results <- store.Result{Index: index, Event: event}
		}
	}
	first := store.Result{Index: 1, Event: event}
	sent, err := records.deliver(first)
	if err != nil || sent != 4 || len(stream.sent) != 1 || len(stream.sent[0].Responses) != 4 {
		t.Fatal("waiting results were not packed into one message", sent, err, stream.sent)
	}
	if len(records.records) != 0 || len(records.credits) != 0 || records.pending != 0 {
		t.Fatal("sent records kept their credits", len(records.records), len(records.credits), records.pending)
	}
}
