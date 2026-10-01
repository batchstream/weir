package search

import (
	"encoding/json"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

type nativeError struct {
	Type string `json:"type"`
}

func (a *Adapter) reject(errorType string, status int) (*pb.Failure, execution.Feedback) {
	switch {
	case status == 409 && errorType == "version_conflict_engine_exception":
		return protocol.Fail(pb.FailureCode_CONFLICT, "native conditional conflict"), execution.Neutral
	case status == 404 && errorType == "index_not_found_exception":
		return protocol.Fail(pb.FailureCode_NOT_FOUND, "requested index missing"), execution.Neutral
	case status == 429 &&
		(a.dialect == ElasticsearchProduct && errorType == "es_rejected_execution_exception" ||
			a.dialect == OpenSearchProduct && errorType == "rejected_execution_exception"),
		status == 503 && errorType == "unavailable_shards_exception":
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable"), execution.Congested
	}
	if status != 400 {
		return nil, execution.Neutral
	}
	switch errorType {
	case "mapper_parsing_exception", "document_parsing_exception", "strict_dynamic_mapping_exception", "illegal_argument_exception", "routing_missing_exception":
		return protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "native document rejection"), execution.Neutral
	default:
		return nil, execution.Neutral
	}
}

func (a *Adapter) bulkResults(works []*execution.Plan, status int, raw []byte, err error) ([]*pb.BulkResult, execution.Feedback) {
	results := make([]*pb.BulkResult, len(works))
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledgement unavailable or incomplete")
	for i, work := range works {
		results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_UNKNOWN, failure)
	}
	if err != nil || len(raw) > responseLimit || validateJSON(raw, 16384) != nil {
		return results, execution.Neutral
	}
	if status != 200 {
		var envelope struct {
			Error  *nativeError
			Status int
		}
		var fields map[string]json.RawMessage
		if json.Unmarshal(raw, &envelope) == nil &&
			json.Unmarshal(raw, &fields) == nil &&
			len(fields) == 2 &&
			envelope.Error != nil &&
			envelope.Status == status {
			failure, feedback := a.reject(envelope.Error.Type, status)
			if failure != nil {
				for i, work := range works {
					results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, failure)
				}
				return results, feedback
			}
		}
		return results, execution.Neutral
	}
	var envelope struct {
		Errors *bool                        `json:"errors"`
		Took   *int64                       `json:"took"`
		Items  []map[string]json.RawMessage `json:"items"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.Errors == nil || envelope.Took == nil || len(envelope.Items) != len(works) {
		return results, execution.Neutral
	}
	// Validate the entire positional correspondence before trusting any item.
	hadErrors := false
	for i, work := range works {
		native := work.Backend.(*plan)
		action := native.action
		if action == "replace" {
			action = "index"
		} else if action == "expression" {
			action = "update"
		}
		entry := envelope.Items[i]
		encoded, ok := entry[action]
		var item expressionResponse
		if !ok || len(entry) != 1 || json.Unmarshal(encoded, &item) != nil || item.Index != native.index || item.ID != native.id {
			return results, execution.Neutral
		}
		hadErrors = hadErrors || item.Error != nil
	}
	if hadErrors != *envelope.Errors {
		return results, execution.Neutral
	}
	feedback := execution.Healthy
	for i, work := range works {
		native := work.Backend.(*plan)
		action := native.action
		if action == "replace" {
			action = "index"
		} else if action == "expression" {
			action = "update"
		}
		encoded := envelope.Items[i][action]
		var item expressionResponse
		_ = json.Unmarshal(encoded, &item)
		if native.program != nil || native.action == "expression" {
			var mutation *pb.MutationResult
			var signal execution.Feedback
			if native.program != nil {
				opts := programWriteReplyOptions{
					index:          native.index,
					id:             native.id,
					expectedResult: native.expectedResult,
					status:         item.Status,
					raw:            encoded,
				}
				mutation, signal = a.programWriteReply(opts)
			} else {
				opts := expressionReplyOptions{native: native, status: item.Status, raw: encoded, bulk: true}
				mutation, signal = a.expressionReply(opts)
			}
			results[i] = protocol.ResultError(work.Operation, mutation.Outcome, mutation.Failure)
			feedback = combineFeedback(feedback, signal)
			continue
		}
		if item.Error != nil {
			failure, signal := a.reject(item.Error.Type, item.Status)
			if item.Status < 400 || failure == nil || item.Result != "" || item.Version != nil || item.Seq != nil || item.Term != nil || item.Shards != nil {
				feedback = combineFeedback(feedback, execution.Neutral)
				continue
			}
			feedback = combineFeedback(feedback, signal)
			if native.action == "create" && failure.Code == pb.FailureCode_CONFLICT {
				failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record already exists")
			}
			results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		valid := false
		switch action {
		case "create":
			valid = item.Status == 201 && item.Result == "created"
		case "index":
			valid = item.Status == 201 && item.Result == "created" || item.Status == 200 && item.Result == "updated"
		case "delete":
			valid = item.Status == 200 && item.Result == "deleted" || item.Status == 404 && item.Result == "not_found"
		}
		if native.action == "replace" && item.Result != "updated" {
			valid = false
		}
		if !valid ||
			item.Seq == nil ||
			item.Term == nil ||
			*item.Seq < 0 ||
			*item.Term < 1 ||
			item.Shards == nil ||
			item.Shards.Successful == nil ||
			item.Shards.Total == nil ||
			item.Shards.Failed == nil ||
			*item.Shards.Successful < 1 ||
			*item.Shards.Total < *item.Shards.Successful ||
			*item.Shards.Failed < 0 ||
			*item.Shards.Failed > *item.Shards.Total-*item.Shards.Successful {
			if feedback == execution.Healthy {
				feedback = execution.Neutral
			}
			continue
		}
		var postWriteFailure *pb.Failure
		if *item.Shards.Failed != 0 {
			postWriteFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledged but replica acknowledgement failed")
			if feedback == execution.Healthy {
				feedback = execution.Neutral
			}
		}
		results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_APPLIED, postWriteFailure)
	}
	return results, feedback
}
