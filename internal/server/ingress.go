package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

var tracePattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

func (s *Server) ingress(ctx context.Context) (context.Context, error) {
	headers, _ := metadata.FromIncomingContext(ctx)
	headers = headers.Copy()
	ids := headers.Get("weir-request-id")
	if len(ids) == 0 {
		raw := make([]byte, 16)
		if _, err := rand.Read(raw); err != nil {
			return nil, status.Error(codes.Internal, "request ID unavailable")
		}
		headers.Set("weir-request-id", hex.EncodeToString(raw))
	} else {
		if len(ids) != 1 || len(ids[0]) < 1 || len(ids[0]) > 128 {
			return nil, status.Error(codes.InvalidArgument, "invalid request ID")
		}
		for _, c := range ids[0] {
			if c < 33 || c > 126 {
				return nil, status.Error(codes.InvalidArgument, "invalid request ID")
			}
		}
	}
	traces := headers.Get("traceparent")
	if len(traces) > 0 && (len(traces) != 1 || !tracePattern.MatchString(traces[0])) {
		return nil, status.Error(codes.InvalidArgument, "invalid trace context")
	}
	ctx = metadata.NewIncomingContext(ctx, headers)
	return ctx, nil
}
