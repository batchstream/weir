package testutil

import (
	"context"

	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"google.golang.org/grpc"
)

// RecordStream presents a packed Execute stream one request and one response
// at a time, as if each came in a message of its own.
type RecordStream struct {
	grpc.BidiStreamingClient[pb.ExecuteRequests, pb.ExecuteResponses]
	pending []*pb.ExecuteResponse
}

// Execute opens an Execute stream that sends and receives one at a time.
func Execute(ctx context.Context, client pb.StoreServiceClient) (*RecordStream, error) {
	stream, err := client.Execute(ctx)
	if err != nil {
		return nil, err
	}
	records := &RecordStream{BidiStreamingClient: stream}
	return records, nil
}

func (s *RecordStream) Send(request *pb.ExecuteRequest) error {
	message := &pb.ExecuteRequests{Requests: []*pb.ExecuteRequest{request}}
	return s.BidiStreamingClient.Send(message)
}

func (s *RecordStream) Recv() (*pb.ExecuteResponse, error) {
	for len(s.pending) == 0 {
		message, err := s.BidiStreamingClient.Recv()
		if err != nil {
			return nil, err
		}
		s.pending = message.Responses
	}
	response := s.pending[0]
	s.pending = s.pending[1:]
	return response, nil
}
