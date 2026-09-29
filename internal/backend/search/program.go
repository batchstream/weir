package search

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"github.com/batchstream/weir/internal/value"
)

const programAttempts = 5
const programLifetime = 5 * time.Second

func (a *Adapter) executeProgram(parent context.Context, work *execution.Plan) ([]*pb.BulkResult, execution.Feedback) {
	native := work.Backend.(*plan)
	mutation, signal := a.runProgram(parent, native)
	variant := &pb.BulkResult_Mutation{Mutation: mutation}
	result := &pb.BulkResult{Index: work.Operation.Index, Result: variant}
	return []*pb.BulkResult{result}, signal
}

func (a *Adapter) runProgram(parent context.Context, native *plan) (*pb.MutationResult, execution.Feedback) {
	if parent.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(parent)), execution.Neutral
	}
	ctx, cancel := context.WithTimeout(parent, programLifetime)
	defer cancel()
	caps, failure, signal := a.inspect(ctx, false)
	if ctx.Err() != nil {
		return searchProgramDeadline(parent, ctx.Err())
	}
	if failure == nil && (!caps.source || !caps.nativeWrite) {
		failure = protocol.Fail(pb.FailureCode_UNSUPPORTED, "program requires full stored source and no default or final pipeline")
	}
	if failure != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), signal
	}
	for attempt := 0; attempt < programAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return searchProgramDeadline(parent, err)
		}
		current, failure, signal := a.get(ctx, native)
		if failure != nil {
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), signal
		}
		currentValue := value.Value{Kind: value.Missing}
		if *current.Found {
			var err error
			currentValue, err = value.DecodeJSON(current.Source)
			if err != nil {
				failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored JSON source cannot be transformed losslessly")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
		}
		program := *native.program
		program.Current = currentValue
		transformed, err := a.config.LuaRunner.Run(ctx, program)
		if err != nil {
			return searchLuaFailure(parent, ctx, err)
		}
		switch transformed.Action {
		case "keep":
			return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
		case "reject":
			failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Healthy
		case "delete":
			if !*current.Found {
				return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
			}
			query := "if_primary_term=" + strconv.FormatInt(*current.Term, 10) + "&if_seq_no=" + strconv.FormatInt(*current.Seq, 10) + "&refresh=false&wait_for_active_shards=1&timeout=1s"
			path := "/" + a.config.Index + "/_doc/" + url.PathEscape(native.id) + "?" + query
			call := exchange{path: path, method: "DELETE", limit: metadataLimit}
			status, raw, requestErr := a.request(ctx, call)
			if isProgramConflict(status, raw, requestErr) {
				continue
			}
			mutation, signal := a.programWriteReply(native.id, "deleted", status, raw, requestErr)
			return mutation, signal
		case "replace":
			if !safeProgramSource(transformed.Value) {
				failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement contains unsupported Search fields")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
			source, err := value.EncodeJSON(transformed.Value)
			if err != nil || len(source) > protocol.MaxDocument {
				failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua replacement exceeds Search source limits")
				return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
			}
			query := "refresh=false&wait_for_active_shards=1&timeout=1s"
			method := "PUT"
			if *current.Found {
				query += "&if_primary_term=" + strconv.FormatInt(*current.Term, 10) + "&if_seq_no=" + strconv.FormatInt(*current.Seq, 10)
			} else {
				query += "&op_type=create"
			}
			path := "/" + a.config.Index + "/_doc/" + url.PathEscape(native.id) + "?" + query
			call := exchange{path: path, method: method, body: source, contentType: "application/json", limit: metadataLimit}
			status, raw, requestErr := a.request(ctx, call)
			if isProgramConflict(status, raw, requestErr) {
				continue
			}
			expected := "updated"
			if !*current.Found {
				expected = "created"
			}
			mutation, signal := a.programWriteReply(native.id, expected, status, raw, requestErr)
			return mutation, signal
		default:
			failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua worker returned an invalid action")
			return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
	}
	failure = protocol.Fail(pb.FailureCode_CONFLICT, "Search record changed during every transform attempt")
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
}

func safeProgramSource(v value.Value) bool {
	if v.Kind == value.Object {
		for _, field := range v.Fields {
			if field.Name == "" || len(field.Name) > 1024 || strings.ContainsAny(field.Name, ".\x00") {
				return false
			}
			switch field.Name {
			case "_id", "_index", "_routing", "_version", "_seq_no", "_primary_term":
				return false
			}
			if !safeProgramSource(field.Value) {
				return false
			}
		}
	}
	for _, item := range v.Items {
		if !safeProgramSource(item) {
			return false
		}
	}
	return v.Kind != value.Missing && v.Kind != value.Bytes && (v.Kind != value.Extended || value.IsJSONNumber(v))
}

func isProgramConflict(status int, raw []byte, err error) bool {
	if err != nil || status != 409 {
		return false
	}
	var response struct {
		Error  *nativeError
		Status int
	}
	if json.Unmarshal(raw, &response) != nil || response.Error == nil || response.Status != status {
		return false
	}
	return response.Error.Type == "version_conflict_engine_exception"
}

func (a *Adapter) programWriteReply(id, expectedResult string, status int, raw []byte, err error) (*pb.MutationResult, execution.Feedback) {
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "conditional write acknowledgement unavailable or incomplete"))
	if err != nil || len(raw) > metadataLimit || validateJSON(raw, 4096) != nil {
		return unknown, execution.Neutral
	}
	var reply expressionResponse
	if json.Unmarshal(raw, &reply) != nil || reply.Index != a.config.Index || reply.ID != id {
		return unknown, execution.Neutral
	}
	if reply.Error != nil {
		if reply.Status != status || reply.Result != "" || reply.Version != nil || reply.Seq != nil || reply.Term != nil || reply.Shards != nil {
			return unknown, execution.Neutral
		}
		failure, signal := a.reject(reply.Error.Type, status)
		if failure == nil {
			return unknown, execution.Neutral
		}
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), signal
	}
	if status != 200 && status != 201 || reply.Result != expectedResult || reply.Version == nil || *reply.Version < 1 || reply.Seq == nil || *reply.Seq < 0 || reply.Term == nil || *reply.Term < 1 || reply.Shards == nil {
		return unknown, execution.Neutral
	}
	shards := reply.Shards
	if shards.Total == nil || shards.Successful == nil || shards.Failed == nil || *shards.Total < 0 || *shards.Successful < 0 || *shards.Failed < 0 || *shards.Successful > *shards.Total || *shards.Failed > *shards.Total-*shards.Successful {
		return unknown, execution.Neutral
	}
	if expectedResult == "created" && status != 201 || expectedResult == "updated" && status != 200 || expectedResult == "deleted" && status != 200 {
		return unknown, execution.Neutral
	}
	if *shards.Failed > 0 {
		failure := protocol.Fail(pb.FailureCode_UNAVAILABLE, "conditional write acknowledged but replica acknowledgement failed")
		return protocol.Mutation(pb.MutationOutcome_APPLIED, failure), execution.Neutral
	}
	return protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
}

func searchLuaFailure(parent, ctx context.Context, err error) (*pb.MutationResult, execution.Feedback) {
	if parent.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, protocol.ContextFailure(parent)), execution.Neutral
	}
	code := pb.FailureCode_INVALID_ARGUMENT
	message := "Lua program evaluation failed"
	if errors.Is(err, context.DeadlineExceeded) {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua program execution limit exceeded"
	} else if ctx.Err() != nil {
		code = pb.FailureCode_DEADLINE_EXCEEDED
		message = "Lua transform execution deadline exceeded"
	}
	failure := protocol.Fail(code, message)
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
}

func searchProgramDeadline(parent context.Context, err error) (*pb.MutationResult, execution.Feedback) {
	if parent.Err() != nil {
		return protocol.Mutation(pb.MutationOutcome_NOT_STARTED, protocol.ContextFailure(parent)), execution.Neutral
	}
	failure := protocol.Fail(pb.FailureCode_DEADLINE_EXCEEDED, fmt.Sprint(err))
	return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
}
