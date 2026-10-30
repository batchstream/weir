package search

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
)

type nativeError struct {
	Type string `json:"type"`
}

func (item *bulkItem) validShards() bool {
	shards := &item.shards
	return item.hasShards &&
		shards.total.set && shards.successful.set && shards.failed.set &&
		shards.successful.value >= 1 && shards.total.value >= shards.successful.value &&
		shards.failed.value >= 0 && shards.failed.value <= shards.total.value-shards.successful.value
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

type bulkResponse struct {
	works  []*execution.Plan
	status int
	raw    []byte
	err    error
}

func (a *Adapter) bulkResults(ctx context.Context, response bulkResponse) []*pb.Event {
	works, status, raw, err := response.works, response.status, response.raw, response.err
	results := make([]*pb.Event, len(works))
	outcome := pb.MutationOutcome_UNKNOWN
	failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write acknowledgement invalid or incomplete")
	// A timeout changes what we know about the reply, never whether a connected
	// mutation might have committed. Keep causes fixed and omit native bodies.
	switch {
	case err == errWriteNotSent:
		outcome = pb.MutationOutcome_NOT_APPLIED
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "write request not sent to backend")
	case err == errTimeout:
		failure = protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, "backend write deadline exceeded; acknowledgement unavailable")
	case err == errCanceled:
		failure = protocol.Fail(pb.FailureCode_CANCELLED, "backend write canceled; acknowledgement unavailable")
	case err == errTransport:
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write transport failed; acknowledgement unavailable")
	case err == errResponse:
		failure = protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend write response invalid or incomplete; acknowledgement unavailable")
	}
	// Every action without evidence of its own reports this failure.
	unacknowledged := func() []*pb.Event {
		for i, work := range works {
			if results[i] == nil {
				results[i] = execution.FailedEvent(work.Command, outcome, failure)
			}
		}
		return results
	}
	if err != nil {
		return unacknowledged()
	}
	if validateJSON(ctx, raw) != nil {
		if ctx.Err() != nil {
			for i, work := range works {
				results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_UNKNOWN, protocol.ContextFailure(ctx))
			}
		}
		return unacknowledged()
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
	// Validate the entire positional correspondence before trusting any item.
	reply, ok := decodeBulkReply(raw, works)
	if !ok {
		return unacknowledged()
	}
	for i, work := range works {
		native := work.Backend.(*plan)
		item := &reply.items[i]
		transformed := native.program != nil || native.action == "expression"
		acknowledgement := "write"
		if transformed {
			acknowledgement = "update"
			if native.program != nil {
				acknowledgement = "conditional write"
			}
			failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, acknowledgement+" acknowledgement unavailable or incomplete")
			results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_UNKNOWN, failure)
		}
		if item.hasError {
			status := int(item.status)
			failure := a.reject(item.errorType, status)
			if native.action == "expression" && status == 404 && item.errorType == "document_missing_exception" {
				failure = protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, "record missing")
			}
			if status < 400 || failure == nil || item.result != "" || item.version.set || item.seq.set || item.term.set || item.hasShards {
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
			valid = item.status == 201 && item.result == "created"
		case "index":
			valid = item.status == 200 && item.result == "updated" ||
				native.program == nil && item.status == 201 && item.result == "created"
		case "replace":
			valid = item.status == 200 && item.result == "updated"
		case "delete":
			valid = item.status == 200 && item.result == "deleted" ||
				native.program == nil && item.status == 404 && item.result == "not_found"
		case "expression":
			valid = item.status == 200 && (item.result == "updated" || item.result == "noop")
		}
		if !valid ||
			transformed && (!item.version.set || item.version.value < 1) ||
			!item.seq.set ||
			!item.term.set ||
			item.seq.value < 0 ||
			item.term.value < 1 ||
			!item.validShards() {
			continue
		}
		if native.action == "expression" && item.result == "noop" && item.shards.failed.value != 0 {
			continue
		}
		var postWriteFailure *pb.Failure
		if item.shards.failed.value != 0 {
			postWriteFailure = protocol.Fail(pb.FailureCode_UNAVAILABLE, acknowledgement+" acknowledged but replica acknowledgement failed")
		}
		results[i] = execution.FailedEvent(work.Command, pb.MutationOutcome_APPLIED, postWriteFailure)
	}
	return unacknowledged()
}

// Metadata/read failures describe availability, without claiming a write outcome.
func (a *Adapter) nativeResponseFailure(ctx context.Context, status int, raw []byte) *pb.Failure {
	var envelope struct {
		Error  *nativeError
		Status int
	}
	if validateJSON(ctx, raw) == nil && json.Unmarshal(raw, &envelope) == nil && envelope.Error != nil && envelope.Status == status {
		if failure := a.reject(envelope.Error.Type, status); failure != nil {
			return failure
		}
	}
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	return protocol.Fail(pb.FailureCode_UNAVAILABLE, "backend request failed without reliable native error evidence")
}
