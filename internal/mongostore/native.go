package mongostore

import (
	"context"
	"errors"
	"io"

	pb "github.com/batchstream/weir/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/protocol"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
	"google.golang.org/protobuf/proto"
)

const NativeDescriptor = "application/vnd.weir.mongodb-command.v1+protobuf"
const NativeCommandLimit = 4 << 20
const NativeResponseLimit = 4 << 20

func (a *Adapter) PrepareNative(open *pb.NativeOpen) (*execution.Plan, *pb.Failure) {
	if !a.nativeNoReplay {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native profile excludes driver reauthentication/replay")
	}
	if f := protocol.ValidateNative(open, a.config.Store); f != nil {
		return nil, f
	}
	_, parts, _ := protocol.ParseResource(open.Resource)
	if len(parts) != 2 || parts[0] != a.config.Database || parts[1] != a.config.Collection {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native requires the configured collection")
	}
	if open.Descriptor_.MediaType != NativeDescriptor || len(open.Descriptor_.Data) != 0 || open.BodyMediaType != "application/bson" {
		return nil, protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native requires empty Mongo command descriptor and BSON body")
	}
	p := &execution.Plan{Native: true, Key: open.Resource, Token: "native", Bytes: proto.Size(open) + protocol.EntryOverhead, ResultBytes: protocol.NativeChunk + protocol.ResultOverhead, PageBytes: scanPageBudget}
	return p, nil
}

func (a *Adapter) nativeCommand(raw []byte) *pb.Failure {
	invalid := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid or excessive ordered BSON command")
	nodes := 65536
	if len(raw) > NativeCommandLimit {
		return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "Native BSON command limit")
	}
	if !validScanBSON(raw, 0, &nodes) {
		return invalid
	}
	if !nativeNoCode(raw) {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native JavaScript expressions unsupported")
	}
	fields, err := scanFields(raw)
	if err != nil || len(fields) == 0 {
		return invalid
	}
	elements, err := bson.Raw(raw).Elements()
	if err != nil {
		return invalid
	}
	command := elements[0].Key()
	if command != "count" && command != "findAndModify" {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native Mongo command")
	}
	target, ok := elements[0].Value().StringValueOK()
	if !ok || target != a.config.Collection {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "Native command target differs from resource")
	}
	for key, value := range fields {
		if key == command {
			continue
		}
		allowed := false
		switch key {
		case "query":
			allowed = value.Type == bson.TypeEmbeddedDocument
		case "sort", "fields", "update":
			allowed = command == "findAndModify" && value.Type == bson.TypeEmbeddedDocument
		case "new", "upsert", "remove":
			allowed = command == "findAndModify" && value.Type == bson.TypeBoolean
		case "limit", "skip":
			allowed = command == "count" && (value.Type == bson.TypeInt32 || value.Type == bson.TypeInt64)
		case "hint":
			allowed = value.Type == bson.TypeString || value.Type == bson.TypeEmbeddedDocument
		}
		if !allowed {
			return protocol.Fail(pb.FailureCode_UNSUPPORTED, "unsupported Native Mongo option")
		}
	}
	// No aggregation pipeline, namespace-bearing stage, session or unacknowledged
	// write options exist in this profile. Query semantics remain MongoDB's.
	return nil
}

func (a *Adapter) ExecuteNative(ctx context.Context, _ *execution.Plan, exchange *execution.NativeExchange) (*pb.NativeEnd, execution.Feedback) {
	raw, err := io.ReadAll(io.LimitReader(exchange.Source, NativeCommandLimit+1))
	if err != nil {
		return protocol.NativeFailure(false, protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "incomplete Native BSON input")), execution.Neutral
	}
	if f := a.nativeCommand(raw); f != nil {
		return protocol.NativeFailure(false, f), execution.Neutral
	}
	if ctx.Err() != nil {
		return protocol.NativeFailure(false, protocol.ContextFailure(ctx)), execution.Neutral
	}
	command := bson.Raw(raw)
	reply, err := a.client.Database(a.config.Database).RunCommand(ctx, command).Raw()
	if err != nil && len(reply) == 0 {
		var native mongo.CommandError
		if errors.As(err, &native) {
			reply = native.Raw
		}
	}
	// Raw is the actual driver-retained wire response, including ok:0 and write
	// errors. Never reconstruct BSON from the Go error or normalize write effects.
	if len(reply) == 0 {
		return protocol.NativeFailure(true, backendFailure(ctx, err)), execution.Neutral
	}
	nodes := 65536
	fields, framingErr := scanFields(reply)
	envelopeOK := false
	switch fields["ok"].Type {
	case bson.TypeDouble:
		n := fields["ok"].Double()
		envelopeOK = n == 0 || n == 1
	case bson.TypeInt32:
		n := fields["ok"].Int32()
		envelopeOK = n == 0 || n == 1
	case bson.TypeInt64:
		n := fields["ok"].Int64()
		envelopeOK = n == 0 || n == 1
	}
	if len(reply) > NativeResponseLimit || framingErr != nil || !envelopeOK || !validScanBSON(reply, 0, &nodes) {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, "invalid or excessive Native BSON reply")), execution.Neutral
	}
	head := &pb.NativeHead{BodyMediaType: "application/bson"}
	if err := exchange.Sink.Head(head); err != nil {
		return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed")), execution.Neutral
	}
	for len(reply) > 0 {
		n := min(len(reply), protocol.NativeChunk)
		if err := exchange.Sink.Chunk(reply[:n]); err != nil {
			return protocol.NativeFailure(true, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native response delivery failed")), execution.Neutral
		}
		reply = reply[n:]
	}
	end := &pb.NativeEnd{Completion: pb.NativeCompletion_RESPONSE_COMPLETE}
	return end, execution.Neutral
}

// Walk already validated bounded bytes, without materializing a query tree.
// This profile excludes server-side JavaScript rather than supplying a runtime.
func nativeNoCode(raw []byte) bool {
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		element, tail, _ := bsoncore.ReadElement(rest)
		rest = tail
		key := element.Key()
		if key == "$where" || key == "$function" || key == "$accumulator" {
			return false
		}
		value := element.Value()
		switch value.Type {
		case bsoncore.TypeJavaScript, bsoncore.TypeCodeWithScope:
			return false
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			if !nativeNoCode(value.Data) {
				return false
			}
		}
	}
	return true
}
