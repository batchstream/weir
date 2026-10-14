package mongodb

import (
	"context"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver"
	"go.mongodb.org/mongo-driver/v2/x/mongo/driver/description"
	"google.golang.org/protobuf/proto"
)

const NativeCommandLimit = 4 << 20

const NativeResponseLimit = 4 << 20

func (a *Adapter) prepareNative(request *pb.NativeRequest) (*execution.Plan, *pb.Failure) {
	if f := protocol.ValidateNative(request); f != nil {
		return nil, f
	}
	parts, _ := protocol.ParseRelativeResource(request.Resource)
	if len(parts) > 2 {
		return nil, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid MongoDB Native target")
	}
	if request.Request.ContentType != "application/bson" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Native requires application/bson")
	}

	target := namespace{}
	if len(parts) > 0 {
		target.database = parts[0]
	}
	if len(parts) == 2 {
		target.collection = parts[1]
	}
	p := &execution.Plan{
		Backend:      target,
		Key:          request.Resource,
		Bytes:        proto.Size(request) + execution.EntryOverheadBytes,
		ResultBytes:  protocol.NativeChunk + execution.ResultOverheadBytes,
		WorkingBytes: scanPageBudget,
	}
	return p, nil
}

func (a *Adapter) nativeCommand(raw []byte) *pb.Failure {
	if len(raw) > NativeCommandLimit {
		return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native BSON command limit")
	}
	command := bson.Raw(raw)
	if !scanFraming(raw) || command.Validate() != nil {
		return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid ordered BSON command")
	}
	if len(raw) == 5 {
		return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "empty BSON command")
	}
	return nil
}

func (a *Adapter) executeNative(ctx context.Context, work *execution.Plan, emit execution.Emit) *pb.NativeEnd {
	raw := work.Command.GetNative().Request.Data
	target := work.Backend.(namespace)
	if f := a.nativeCommand(raw); f != nil {
		return protocol.NativeFailure(false, f)
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx))
	}
	database := target.database
	commandElements := raw[4 : len(raw)-1]
	prefix, suffix := commandElements, []byte(nil)
	foundDatabase := false
	for remaining := commandElements; len(remaining) != 0; {
		element, rest, _ := bsoncore.ReadElement(remaining)
		if element.Key() == "$db" {
			value, valid := element.Value().StringValueOK()
			if foundDatabase || !valid || value == "" {
				return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Native requires one nonempty string $db"))
			}
			foundDatabase = true
			database = value
			prefix = commandElements[:len(commandElements)-len(remaining)]
			suffix = rest
		}
		remaining = rest
	}
	if database == "" {
		return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "Native requires a resource database or $db"))
	}
	var reply bsoncore.Document
	retry := driver.RetryNone
	operation := driver.Operation{
		Database:       database,
		Deployment:     a.deployment,
		CommandMonitor: a.commandMonitor,
		// Opaque commands must not acquire driver session state, inferred
		// deadlines, cluster times, read preferences or automatic replays.
		Type:          driver.Write,
		RetryMode:     &retry,
		OmitMaxTimeMS: true,
		CommandFn: func(dst []byte, _ description.SelectedServer) ([]byte, error) {
			dst = append(dst, prefix...)
			return append(dst, suffix...), nil
		},
		ProcessResponseFn: func(_ context.Context, response bsoncore.Document, _ driver.ResponseInfo) error {
			reply = response
			return nil
		},
	}
	attempt, release := nativeAttemptContext(ctx)
	defer release()
	err := operation.Execute(attempt)
	failure := backendFailure(ctx, err)
	// Raw is the actual driver-retained wire response, including ok:0 and write
	// errors. Never reconstruct BSON from the Go error or normalize write effects.
	if len(reply) == 0 {
		return protocol.NativeFailure(true, failure)
	}
	if len(reply) > NativeResponseLimit || !scanFraming(reply) || bson.Raw(reply).Validate() != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "invalid or excessive Native BSON reply"))
	}
	head := &pb.NativeHead{BodyContentType: "application/bson"}
	value := &pb.Event_Head{Head: head}
	event := &pb.Event{Value: value}
	if err := emit(work, event); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed"))
	}
	for len(reply) > 0 {
		n := min(len(reply), protocol.NativeChunk)
		value := &pb.Event_Chunk{Chunk: append([]byte(nil), reply[:n]...)}
		event := &pb.Event{Value: value}
		if err := emit(work, event); err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed"))
		}
		reply = reply[n:]
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end
}
