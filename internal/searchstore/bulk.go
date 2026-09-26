package searchstore

import (
	"encoding/json"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
)

type nativeError struct {
	Type string `json:"type"`
}
type itemReply struct {
	Index  string                                   `json:"_index"`
	ID     string                                   `json:"_id"`
	Status int                                      `json:"status"`
	Result string                                   `json:"result"`
	Seq    *int64                                   `json:"_seq_no"`
	Term   *int64                                   `json:"_primary_term"`
	Shards *struct{ Total, Successful, Failed int } `json:"_shards"`
	Error  *nativeError                             `json:"error"`
}

func (a *Adapter) reject(errorType string, status int) (*pb.Failure, execution.Feedback) {
	switch {
	case status == 409 && errorType == "version_conflict_engine_exception":
		return protocol.Fail(pb.FailureCode_CONFLICT, "native conditional conflict"), execution.Neutral
	case status == 404 && errorType == "index_not_found_exception":
		return protocol.Fail(pb.FailureCode_NOT_FOUND, "configured index missing"), execution.Neutral
	case status == 429 && (a.config.Profile == "elasticsearch-8.17.0" && errorType == "es_rejected_execution_exception" || a.config.Profile == "opensearch-2.19.0" && errorType == "rejected_execution_exception"), status == 503 && errorType == "unavailable_shards_exception":
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
	if err != nil {
		return results, execution.Neutral
	}
	if status != 200 {
		var envelope struct {
			Error  *nativeError
			Status int
		}
		if json.Unmarshal(raw, &envelope) == nil && envelope.Error != nil && envelope.Status == status {
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
		Errors *bool                  `json:"errors"`
		Took   *int64                 `json:"took"`
		Items  []map[string]itemReply `json:"items"`
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
		}
		entry := envelope.Items[i]
		item, ok := entry[action]
		if !ok || len(entry) != 1 || item.Index != a.config.Index || item.ID != native.id {
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
		}
		item := envelope.Items[i][action]
		if item.Error != nil {
			failure, signal := a.reject(item.Error.Type, item.Status)
			if signal == execution.Congested {
				feedback = execution.Congested
			} else if feedback == execution.Healthy {
				feedback = execution.Neutral
			}
			if item.Status < 400 || failure == nil {
				continue
			}
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
		if !valid || item.Seq == nil || item.Term == nil || *item.Seq < 0 || *item.Term < 1 || item.Shards == nil || item.Shards.Successful < 1 || item.Shards.Total < item.Shards.Successful || item.Shards.Failed < 0 || item.Shards.Failed > item.Shards.Total-item.Shards.Successful {
			if feedback == execution.Healthy {
				feedback = execution.Neutral
			}
			continue
		}
		var postWriteFailure *pb.Failure
		if item.Shards.Failed != 0 {
			postWriteFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledged but replica acknowledgement failed")
			if feedback == execution.Healthy {
				feedback = execution.Neutral
			}
		}
		results[i] = protocol.ResultError(work.Operation, pb.MutationOutcome_APPLIED, postWriteFailure)
	}
	return results, feedback
}
