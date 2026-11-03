package search

import (
	"encoding/json"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

const ExpressionContentType = "application/vnd.weir.search-update.v1+json"

func validateExpression(d *pb.Document) *pb.Failure {
	if d == nil || d.ContentType != ExpressionContentType {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Search expression profile")
	}
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "expression requires exactly one doc object")
	if !object(d.Data) || !validJSON(d.Data) {
		return invalid
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(d.Data, &body) != nil || len(body) != 1 || !object(body["doc"]) {
		return invalid
	}
	return nil
}
