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

type bulkItem struct {
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

func (item *bulkItem) validShards() bool {
	shards := item.Shards
	return shards != nil &&
		shards.Total != nil && shards.Successful != nil && shards.Failed != nil &&
		*shards.Successful >= 1 && *shards.Total >= *shards.Successful &&
		*shards.Failed >= 0 && *shards.Failed <= *shards.Total-*shards.Successful
}

func (native *plan) bulkAction() string {
	switch native.action {
	case "replace":
		return "index"
	case "expression":
		return "update"
	default:
		return native.action
	}
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
	outcome := pb.MutationOutcome_UNKNOWN
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write acknowledgement invalid or incomplete")
	// A timeout changes what we know about the reply, never whether a connected
	// mutation might have committed. Keep causes fixed and omit native bodies.
	switch err {
	case errWriteNotSent:
		outcome = pb.MutationOutcome_NOT_APPLIED
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write request not sent to backend")
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
		results[i] = execution.FailedEvent(work.Command, outcome, failure)
	}
	if err != nil || len(raw) > responseLimit || validateJSON(raw, len(raw)) != nil {
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
	items := make([]bulkItem, len(works))
	for i, work := range works {
		native := work.Backend.(*plan)
		entry := envelope.Items[i]
		encoded, ok := entry[native.bulkAction()]
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
		item := &items[i]
		transformed := native.program != nil || native.action == "expression"
		acknowledgement := "write"
		if transformed {
			acknowledgement = "update"
			if native.program != nil {
				acknowledgement = "conditional write"
			}
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, acknowledgement+" acknowledgement unavailable or incomplete")
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_UNKNOWN, failure)
			encoded := envelope.Items[i][native.bulkAction()]
			if len(encoded) > metadataLimit || validateJSON(encoded, len(encoded)) != nil {
				continue
			}
		}
		if item.Error != nil {
			failure := a.reject(item.Error.Type, item.Status)
			if native.action == "expression" && item.Status == 404 && item.Error.Type == "document_missing_exception" {
				failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
			}
			if item.Status < 400 || failure == nil || item.Result != "" || item.Version != nil || item.Seq != nil || item.Term != nil || item.Shards != nil {
				continue
			}
			if native.action == "create" && native.program == nil && failure.Code == pb.FailureCode_CONFLICT {
				failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record already exists")
			}
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_NOT_APPLIED, failure)
			continue
		}
		valid := false
		switch native.action {
		case "create":
			valid = item.Status == 201 && item.Result == "created"
		case "index":
			valid = item.Status == 200 && item.Result == "updated" ||
				native.program == nil && item.Status == 201 && item.Result == "created"
		case "replace":
			valid = item.Status == 200 && item.Result == "updated"
		case "delete":
			valid = item.Status == 200 && item.Result == "deleted" ||
				native.program == nil && item.Status == 404 && item.Result == "not_found"
		case "expression":
			valid = item.Status == 200 && (item.Result == "updated" || item.Result == "noop")
		}
		if !valid ||
			transformed && (item.Version == nil || *item.Version < 1) ||
			item.Seq == nil ||
			item.Term == nil ||
			*item.Seq < 0 ||
			*item.Term < 1 ||
			!item.validShards() {
			continue
		}
		if native.action == "expression" && item.Result == "noop" && *item.Shards.Failed != 0 {
			continue
		}
		var postWriteFailure *pb.Failure
		if *item.Shards.Failed != 0 {
			postWriteFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, acknowledgement+" acknowledged but replica acknowledgement failed")
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
	if len(raw) <= metadataLimit && validateJSON(raw, len(raw)) == nil && json.Unmarshal(raw, &envelope) == nil && envelope.Error != nil && envelope.Status == status {
		if failure := a.reject(envelope.Error.Type, status); failure != nil {
			return failure
		}
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend request failed without reliable native error evidence")
}
