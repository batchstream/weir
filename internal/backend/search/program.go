package search

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
)

const programAttempts = 5
const programLifetime = 5 * time.Second

// evaluateProgram runs Lua in the main process. A returned write carries the
// exact observed OCC condition; only a confirmed conflict permits reevaluation.
func evaluateProgram(ctx context.Context, native *plan, current *getReply) (*plan, *pb.MutationResult, execution.Feedback) {
	currentValue := value.Value{Kind: value.Missing}
	if *current.Found {
		var err error
		currentValue, err = value.DecodeJSON(current.Source)
		if err != nil {
			failure := protocol.Fail(pb.FailureCode_UNSUPPORTED, "stored JSON source cannot be transformed losslessly")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
	}
	program := *native.program
	program.Current = currentValue
	transformed, err := luaengine.Evaluate(ctx, program)
	if err != nil {
		mutation, feedback := searchLuaFailure(ctx, ctx, err)
		return nil, mutation, feedback
	}
	next := *native
	switch transformed.Action {
	case "keep":
		return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
	case "reject":
		failure := protocol.Fail(pb.FailureCode_PRECONDITION_FAILED, transformed.Message)
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Healthy
	case "delete":
		if !*current.Found {
			return nil, protocol.Mutation(pb.MutationOutcome_APPLIED, nil), execution.Healthy
		}
		next.action, next.expectedResult = "delete", "deleted"
	case "replace":
		if !safeProgramSource(transformed.Value) {
			failure := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Lua replacement contains unsupported Search fields")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
		source, err := value.EncodeJSON(transformed.Value)
		if err != nil || len(source) > protocol.MaxDocument {
			failure := protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Lua replacement exceeds Search source limits")
			return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
		}
		next.source = source
		next.action, next.expectedResult = "create", "created"
		if *current.Found {
			next.action, next.expectedResult = "index", "updated"
		}
	default:
		failure := protocol.Fail(pb.FailureCode_INTERNAL, "Lua evaluation returned an invalid action")
		return nil, protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), execution.Neutral
	}
	return &next, nil, execution.Healthy
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

type programWriteReplyOptions struct {
	index          string
	id             string
	expectedResult string
	status         int
	raw            []byte
	err            error
}

func (a *Adapter) programWriteReply(opts programWriteReplyOptions) (*pb.MutationResult, execution.Feedback) {
	unknown := protocol.Mutation(pb.MutationOutcome_UNKNOWN, protocol.Fail(pb.FailureCode_UNAVAILABLE, "conditional write acknowledgement unavailable or incomplete"))
	if opts.err != nil || len(opts.raw) > metadataLimit || validateJSON(opts.raw, 4096) != nil {
		return unknown, execution.Neutral
	}
	var reply expressionResponse
	if json.Unmarshal(opts.raw, &reply) != nil {
		return unknown, execution.Neutral
	}
	if reply.Error != nil {
		if reply.Status != opts.status || reply.Result != "" || reply.Version != nil || reply.Seq != nil || reply.Term != nil || reply.Shards != nil {
			return unknown, execution.Neutral
		}
		failure, signal := a.reject(reply.Error.Type, opts.status)
		if failure == nil {
			return unknown, execution.Neutral
		}
		return protocol.Mutation(pb.MutationOutcome_NOT_APPLIED, failure), signal
	}
	if opts.status != 200 && opts.status != 201 ||
		reply.Index != opts.index ||
		reply.ID != opts.id ||
		reply.Result != opts.expectedResult ||
		reply.Version == nil ||
		*reply.Version < 1 ||
		reply.Seq == nil ||
		*reply.Seq < 0 ||
		reply.Term == nil ||
		*reply.Term < 1 ||
		reply.Shards == nil {
		return unknown, execution.Neutral
	}
	shards := reply.Shards
	if shards.Total == nil ||
		shards.Successful == nil ||
		shards.Failed == nil ||
		*shards.Total < 0 ||
		*shards.Successful < 0 ||
		*shards.Failed < 0 ||
		*shards.Successful > *shards.Total ||
		*shards.Failed > *shards.Total-*shards.Successful {
		return unknown, execution.Neutral
	}
	if opts.expectedResult == "created" && opts.status != 201 ||
		opts.expectedResult == "updated" && opts.status != 200 ||
		opts.expectedResult == "deleted" && opts.status != 200 {
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
