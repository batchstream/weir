package search

import (
	"context"
	"encoding/json"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

const ExpressionContentType = "application/vnd.weir.search-update.v1+json"

func prepareExpression(ctx context.Context, d *pb.Document) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	if d == nil || d.ContentType != ExpressionContentType {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Search expression profile")
	}
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "expression requires exactly one doc object")
	if !object(d.Data) || validateJSON(ctx, d.Data) != nil {
		if ctx.Err() != nil {
			return protocol.ContextFailure(ctx)
		}
		return invalid
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(d.Data, &body) != nil || len(body) != 1 || !object(body["doc"]) {
		return invalid
	}
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	return nil
}
