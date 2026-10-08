package search

import (
	"encoding/json"
	"strings"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
)

const ExpressionContentType = "application/vnd.weir.search-update.v1+json"

func prepareExpression(d *pb.Document) *pb.Failure {
	if d == nil || d.ContentType != ExpressionContentType {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Search expression profile")
	}
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "expression requires exactly one bounded doc object")
	if len(d.Data) > protocol.MaxExpression || !object(d.Data) || validateJSON(d.Data, len(d.Data)) != nil {
		return invalid
	}
	var body map[string]json.RawMessage
	if json.Unmarshal(d.Data, &body) != nil || len(body) != 1 || !object(body["doc"]) {
		return invalid
	}
	// The walker has already bounded the tree. No numeric value ever passes
	// through float64; RawMessage retains caller number spelling and width.
	if !expressionFields(body["doc"]) {
		return invalid
	}
	return nil
}

func expressionFields(raw json.RawMessage) bool {
	if object(raw) {
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &fields) != nil {
			return false
		}
		for name, child := range fields {
			// Dotted names conflict with the search mapper's object paths.
			// Metadata names are not record identity override mechanisms.
			if name == "" || len(name) > 1024 || strings.ContainsAny(name, ".\x00") {
				return false
			}
			switch name {
			case "_id", "_index", "_routing", "_version", "_seq_no", "_primary_term":
				return false
			}
			if !expressionFields(child) {
				return false
			}
		}
	} else if strings.HasPrefix(strings.TrimSpace(string(raw)), "[") {
		var items []json.RawMessage
		if json.Unmarshal(raw, &items) != nil {
			return false
		}
		for _, child := range items {
			if !expressionFields(child) {
				return false
			}
		}
	}
	return true
}
