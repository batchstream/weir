package search

import (
	"encoding/json"
	"strconv"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativeError struct {
	Type string `json:"type"`
}

func (a *Adapter) reject(errorType string, status int) *pb.Failure {
	switch {
	case status == 401 && errorType == "security_exception":
		return protocol.Fail(pb.FailureCode_UNAUTHENTICATED, "backend authentication required")
	case status == 403 && errorType == "security_exception":
		return protocol.Fail(pb.FailureCode_PERMISSION_DENIED, "backend permission denied")
	case status == 409 && errorType == "version_conflict_engine_exception":
		return protocol.Fail(pb.FailureCode_CONFLICT, "native conditional conflict")
	case status == 404 && errorType == "index_not_found_exception":
		return protocol.Fail(pb.FailureCode_TARGET_NOT_FOUND, "requested index missing")
	case status == 429 &&
		(a.dialect == ElasticsearchProduct && errorType == "es_rejected_execution_exception" ||
			a.dialect == OpenSearchProduct && errorType == "rejected_execution_exception"),
		status == 503 && errorType == "unavailable_shards_exception":
		return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend capacity unavailable")
	}
	if status != 400 {
		return nil
	}
	switch errorType {
	case "mapper_parsing_exception", "document_parsing_exception", "strict_dynamic_mapping_exception", "illegal_argument_exception", "routing_missing_exception":
		return protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "native document rejection")
	default:
		return nil
	}
}
func (a *Adapter) bulkResults(works []*execution.Plan, status int, raw []byte, err error) []*pb.Event {
	results := make([]*pb.Event, len(works))
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write acknowledgement invalid or incomplete")
	// A timeout changes what we know about the reply, never whether a connected
	// mutation might have committed. Keep causes fixed and omit native bodies.
	switch err {
	case errTimeout:
		failure = protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "backend write deadline exceeded; acknowledgement unavailable")
	case errCanceled:
		failure = protocol.Fail(pb.FailureCode_CANCELLED, "backend write canceled; acknowledgement unavailable")
	case errTransport:
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write transport failed; acknowledgement unavailable")
	case errResponseLimit:
		failure = protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "backend write response exceeds byte limit; acknowledgement unavailable")
	case errResponse:
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write response invalid or incomplete; acknowledgement unavailable")
	}
	for i, work := range works {
		results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_UNKNOWN, failure)
	}
	if err == errWriteNotSent {
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write request not sent to backend")
		for i, work := range works {
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
		}
		return results
	}
	if err != nil || len(raw) > responseLimit || validateJSON(raw, 16384) != nil {
		return results
	}
	if status != 200 {
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write HTTP status "+strconv.Itoa(status)+"; acknowledgement unavailable")
		for i, work := range works {
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_UNKNOWN, failure)
		}
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
			failure := a.reject(envelope.Error.Type, status)
			if failure != nil {
				for i, work := range works {
					results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
				}
				return results
			}
		}
		return results
	}
	var envelope struct {
		Errors *bool                        `json:"errors"`
		Took   *int64                       `json:"took"`
		Items  []map[string]json.RawMessage `json:"items"`
	}
	if json.Unmarshal(raw, &envelope) != nil || envelope.Errors == nil || envelope.Took == nil || len(envelope.Items) != len(works) {
		return results
	}
	// Validate the entire positional correspondence before trusting any item.
	hadErrors := false
	items := make([]expressionResponse, len(works))
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
		item := &items[i]
		if !ok || len(entry) != 1 || json.Unmarshal(encoded, item) != nil || item.Index != native.index || item.ID != native.id {
			return results
		}
		hadErrors = hadErrors || item.Error != nil
	}
	if hadErrors != *envelope.Errors {
		return results
	}
	for i, work := range works {
		native := work.Backend.(*plan)
		action := native.action
		if action == "replace" {
			action = "index"
		} else if action == "expression" {
			action = "update"
		}
		encoded := envelope.Items[i][action]
		item := &items[i]
		if native.program != nil || native.action == "expression" {
			var mutation *pb.MutationResult
			if native.program != nil {
				opts := programWriteReplyOptions{
					index:          native.index,
					id:             native.id,
					expectedResult: native.expectedResult,
					status:         item.Status,
					raw:            encoded,
				}
				mutation = a.programWriteReply(opts)
			} else {
				opts := expressionReplyOptions{native: native, status: item.Status, raw: encoded}
				mutation = a.expressionReply(opts)
			}
			results[i] = execution.FailedEvent(work.Command, mutation.Outcome, mutation.Failure)
			continue
		}
		if item.Error != nil {
			failure := a.reject(item.Error.Type, item.Status)
			if item.Status < 400 || failure == nil || item.Result != "" || item.Version != nil || item.Seq != nil || item.Term != nil || item.Shards != nil {
				continue
			}
			if native.action == "create" && failure.Code == pb.FailureCode_CONFLICT {
				failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record already exists")
			}
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
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
			continue
		}
		var postWriteFailure *pb.Failure
		if *item.Shards.Failed != 0 {
			postWriteFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write acknowledged but replica acknowledgement failed")
		}
		results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, postWriteFailure)
	}
	return results
}

// Metadata/read failures describe availability, without claiming a write outcome.
func (a *Adapter) nativeResponseFailure(status int, raw []byte) *pb.Failure {
	var envelope struct {
		Error  *nativeError
		Status int
	}
	if len(raw) <= metadataLimit && validateJSON(raw, 4096) == nil && json.Unmarshal(raw, &envelope) == nil && envelope.Error != nil && envelope.Status == status {
		if failure := a.reject(envelope.Error.Type, status); failure != nil {
			return failure
		}
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend request failed without reliable native error evidence")
}
