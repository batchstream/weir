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
	if len(d.Data) > protocol.MaxExpression || !object(d.Data) || validateJSON(d.Data, 4096) != nil {
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

type expressionResponse struct {
	Index   string                                    `json:"_index"`
	ID      string                                    `json:"_id"`
	Result  string                                    `json:"result"`
	Version *int64                                    `json:"_version"`
	Seq     *int64                                    `json:"_seq_no"`
	Term    *int64                                    `json:"_primary_term"`
	Shards  *struct{ Total, Successful, Failed *int } `json:"_shards"`
	Error   *nativeError                              `json:"error"`
	Status  int                                       `json:"status"`
}

type expressionReplyOptions struct {
	native *plan
	status int
	raw    []byte
}

func (a *Adapter) expressionReply(opts expressionReplyOptions) *pb.MutationResult {
	n, status, raw := opts.native, opts.status, opts.raw
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "update acknowledgement unavailable or incomplete"))
	if len(raw) > metadataLimit || validateJSON(raw, 4096) != nil {
		return unknown
	}
	var response expressionResponse
	if json.Unmarshal(raw, &response) != nil {
		return unknown
	}
	if response.Error != nil {
		if response.Status != status ||
			response.Result != "" ||
			response.Version != nil ||
			response.Seq != nil ||
			response.Term != nil ||
			response.Shards != nil {
			return unknown
		}
		failure := a.reject(response.Error.Type, status)
		if status == 404 && response.Error.Type == "document_missing_exception" {
			failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
		}
		if failure != nil {
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure)
		}
		return unknown
	}
	if status != 200 ||
		response.Index != n.index ||
		response.ID != n.id ||
		response.Version == nil ||
		*response.Version < 1 ||
		response.Seq == nil ||
		*response.Seq < 0 ||
		response.Term == nil ||
		*response.Term < 1 ||
		response.Shards == nil {
		return unknown
	}
	shards := response.Shards
	if shards.Total == nil ||
		shards.Successful == nil ||
		shards.Failed == nil ||
		*shards.Total < 0 ||
		*shards.Successful < 0 ||
		*shards.Failed < 0 ||
		*shards.Successful > *shards.Total ||
		*shards.Failed > *shards.Total-*shards.Successful {
		return unknown
	}
	switch response.Result {
	case "noop":
		if *shards.Successful < 1 || *shards.Failed != 0 {
			return unknown
		}
	case "updated":
		if *shards.Successful < 1 {
			return unknown
		}
	default:
		return unknown
	}
	if *shards.Failed > 0 {
		return protocol.Mutation(pb.MutationOutcome_APPLIED, protocol.Fail(pb.FailureCode_UNAVAILABLE, "update acknowledged but replica acknowledgement failed"))
	}
	return protocol.Mutation(pb.MutationOutcome_APPLIED, nil)
}
