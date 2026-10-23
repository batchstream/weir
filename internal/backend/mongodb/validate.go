package mongodb

import (
	"context"
	"strconv"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// Full payload validation runs at execution, before target qualification or
// business commands. Prepared plans retain only wire bytes.
func (a *Adapter) validateRecord(ctx context.Context, work *execution.Plan) *pb.Failure {
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	native := work.Backend.(*plan)
	if native.validated || work.Command.GetRead() != nil {
		return nil
	}
	mutation := work.Command.GetMutate()
	var raw []byte
	if transform := mutation.GetAtomicTransform(); transform != nil {
		if program := transform.GetLua(); program != nil {
			if program.Input != nil {
				raw = program.Input.Data
			}
		} else {
			raw = transform.GetBackendExpression().Data
		}
	} else {
		raw = native.document
	}
	if transform := mutation.GetAtomicTransform(); transform != nil {
		if program := transform.GetLua(); program != nil {
			if program.Input != nil {
				if _, err := Decode(ctx, raw); err != nil {
					if ctx.Err() != nil {
						return protocol.ContextFailure(ctx)
					}
					return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON transform input")
				}
			}
			native.program.Source = string(program.Source)
		} else if failure := a.prepareExpression(ctx, transform.GetBackendExpression()); failure != nil {
			return failure
		}
	} else if len(raw) != 0 {
		valid := validRecordBSON(ctx, raw, 0, false)
		if !valid {
			if ctx.Err() != nil {
				return protocol.ContextFailure(ctx)
			}
			return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON document")
		}
		first, _, ok := bsoncore.ReadElement(raw[4 : len(raw)-1])
		if !ok || first.Key() != "_id" {
			return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "explicit first _id must match resource")
		}
		id, validID := rawRecordID(bson.Raw(raw).Lookup("_id"))
		ids := 0
		for rest := raw[4 : len(raw)-1]; len(rest) != 0; {
			element, tail, _ := bsoncore.ReadElement(rest)
			if element.Key() == "_id" {
				ids++
			}
			rest = tail
		}
		if !validID || ids != 1 || id != native.id {
			return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "explicit first _id must match resource")
		}
	}
	if ctx.Err() != nil {
		return protocol.ContextFailure(ctx)
	}
	native.validated = true
	return nil
}

// Plain writes are forwarded as BSON. Validate the same supported value types,
// UTF-8 and canonical arrays as Decode without constructing a disposable Value
// tree or copying scalar payloads merely to check the first _id.
func validRecordBSON(ctx context.Context, raw []byte, depth int, array bool) bool {
	if depth > 100 || !bsonFraming(raw) {
		return false
	}
	position := 0
	for rest := raw[4 : len(raw)-1]; len(rest) != 0; {
		if ctx.Err() != nil {
			return false
		}
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return false
		}
		rest = tail
		key, err := element.KeyErr()
		if err != nil || !utf8.ValidString(key) || array && key != strconv.Itoa(position) {
			return false
		}
		position++
		item, err := element.ValueErr()
		if err != nil || item.Validate() != nil {
			return false
		}
		switch item.Type {
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			valid := validRecordBSON(ctx, item.Data, depth+1, item.Type == bsoncore.TypeArray)
			if !valid {
				return false
			}
		case bsoncore.TypeString:
			if len(item.Data) < 5 || !utf8.Valid(item.Data[4:len(item.Data)-1]) {
				return false
			}
		case bsoncore.TypeBoolean:
			if len(item.Data) != 1 || item.Data[0] > 1 {
				return false
			}
		case bsoncore.TypeNull, bsoncore.TypeInt32, bsoncore.TypeInt64, bsoncore.TypeDouble:
		default:
			if _, ok := extendedTypes[bson.Type(item.Type)]; !ok {
				return false
			}
		}
	}
	return true
}
