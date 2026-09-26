package server

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
	"strconv"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/protocol"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const HopMetadata = "weir-remaining-forwards"

type ingressContextKey uint8

const ingressKey ingressContextKey = 0

type ingress struct {
	hops       int
	diagnostic metadata.MD
}

var tracePattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$`)

func (s *Server) ingress(request *http.Request) (context.Context, error) {
	state := &ingress{hops: s.initialForwards, diagnostic: make(metadata.MD)}
	hops, present := request.Header[http.CanonicalHeaderKey(HopMetadata)]
	if !s.peer {
		if present {
			return nil, status.Error(codes.InvalidArgument, "reserved peer metadata")
		}
	} else {
		if len(hops) != 1 || len(hops[0]) != 1 || hops[0][0] < '0' || hops[0][0] > '8' {
			return nil, status.Error(codes.InvalidArgument, "peer requires one canonical hop budget 0-8")
		}
		state.hops = int(hops[0][0] - '0')
	}
	ids := request.Header.Values("weir-request-id")
	if len(ids) == 0 {
		raw := make([]byte, 16)
		if _, err := rand.Read(raw); err != nil {
			return nil, status.Error(codes.Internal, "request ID unavailable")
		}
		state.diagnostic.Set("weir-request-id", hex.EncodeToString(raw))
	} else {
		if len(ids) != 1 || len(ids[0]) < 1 || len(ids[0]) > 128 {
			return nil, status.Error(codes.InvalidArgument, "invalid request ID")
		}
		for _, c := range ids[0] {
			if c < 33 || c > 126 {
				return nil, status.Error(codes.InvalidArgument, "invalid request ID")
			}
		}
		state.diagnostic.Set("weir-request-id", ids[0])
	}
	traces := request.Header.Values("traceparent")
	if len(traces) > 0 {
		if len(traces) != 1 || !tracePattern.MatchString(traces[0]) {
			return nil, status.Error(codes.InvalidArgument, "invalid trace context")
		}
		state.diagnostic.Set("traceparent", traces[0])
	}
	return context.WithValue(request.Context(), ingressKey, state), nil
}
func forwardContext(ctx context.Context) (context.Context, error) {
	state, ok := ctx.Value(ingressKey).(*ingress)
	if !ok {
		return nil, status.Error(codes.Internal, "missing ingress context")
	}
	if state.hops == 0 {
		return nil, status.Error(codes.ResourceExhausted, "forward budget exhausted")
	}
	md := state.diagnostic.Copy()
	md.Set(HopMetadata, strconv.Itoa(state.hops-1))
	// Replace, never append to inherited metadata; only bounded diagnostics and the hop budget cross peers.
	return metadata.NewOutgoingContext(ctx, md), nil
}
func checkOperation(name string, op *pb.BulkOperation) (*pb.Failure, error) {
	if op.GetRead() == nil && op.GetMutate() == nil {
		return nil, status.Error(codes.InvalidArgument, "missing operation family")
	}
	return protocol.Validate(op, name), nil
}
