package server

import (
	"context"

	pb "github.com/batchstream/weir/api/weir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Server) Resolve(ctx context.Context, request *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	if s.directory == nil {
		return nil, status.Error(codes.Unavailable, "directory unavailable")
	}
	return s.directory.Resolve(ctx, request)
}
