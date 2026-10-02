package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var tracePattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

func (s *Server) ingress(request *http.Request) (context.Context, error) {
	ids := request.Header.Values("weir-request-id")
	if len(ids) == 0 {
		raw := make([]byte, 16)
		if _, err := rand.Read(raw); err != nil {
			return nil, status.Error(codes.Internal, "request ID unavailable")
		}
		request.Header.Set("weir-request-id", hex.EncodeToString(raw))
	} else {
		if len(ids) != 1 || len(ids[0]) < 1 || len(ids[0]) > 128 {
			return nil, status.Error(codes.InvalidArgument, "invalid request ID")
		}
		for _, c := range ids[0] {
			if c < 33 || c > 126 {
				return nil, status.Error(codes.InvalidArgument, "invalid request ID")
			}
		}
		request.Header.Set("weir-request-id", ids[0])
	}
	traces := request.Header.Values("traceparent")
	if len(traces) > 0 {
		if len(traces) != 1 || !tracePattern.MatchString(traces[0]) {
			return nil, status.Error(codes.InvalidArgument, "invalid trace context")
		}
		request.Header.Set("traceparent", traces[0])
	}
	return request.Context(), nil
}
