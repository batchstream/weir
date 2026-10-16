package mongodb

import (
	"context"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

type recordMemoryKey uint8

const recordMemoryContextKey recordMemoryKey = 0

// One execution owns one driver reply window. Retained program documents are
// added to this window; ordinary read documents belong to their Ticket instead.
type recordMemory struct {
	owner execution.Memory
	base  int
	held  int
}

func recordWorking(plans []*execution.Plan) int {
	working, reply := 0, 0
	for _, work := range plans {
		working += work.WorkingBytes
		reply = max(reply, work.IOReserve)
	}
	return working + reply
}

func reserveRecordWorking(ctx context.Context, plans []*execution.Plan, extra int) error {
	if memory, ok := ctx.Value(recordMemoryContextKey).(*recordMemory); ok {
		if memory.owner != nil {
			return memory.owner.SetWorking(memory.base + memory.held + extra)
		}
		return nil
	}
	if len(plans) != 0 && plans[0].Memory != nil {
		return plans[0].Memory.SetWorking(recordWorking(plans) + extra)
	}
	return nil
}

func recordMemoryFailure() *pb.Failure {
	return protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, execution.ErrExecutionMemory.Error())
}

// Count decoded values before materializing slices, strings and extended BSON
// copies. A kilobyte per value includes Value/Field growth and validation maps;
// scalar bytes are charged separately rather than multiplying a long string.
func bsonDecodeWorking(raw []byte) int {
	if len(raw) > (math.MaxInt-8192)/3 {
		return math.MaxInt
	}
	nodes := bsonDecodeNodes(raw, 0)
	if nodes > (math.MaxInt-8192-3*len(raw))/1024 {
		return math.MaxInt
	}
	return 3*len(raw) + 8192 + nodes*1024
}

func bsonDecodeNodes(document []byte, depth int) int {
	if depth > 100 || !bsonFraming(document) {
		return 0
	}
	nodes := 0
	for rest := document[4 : len(document)-1]; len(rest) != 0; {
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return 0
		}
		nodes++
		rest = tail
		value, err := element.ValueErr()
		if err == nil && (value.Type == bsoncore.TypeEmbeddedDocument || value.Type == bsoncore.TypeArray) {
			nodes += bsonDecodeNodes(value.Data, depth+1)
		}
	}
	return nodes
}

// Full payload validation runs after execution admission and before target
// qualification or business commands. Prepared plans retain only wire bytes.
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
	if work.Memory != nil {
		plans := []*execution.Plan{work}
		extra := 0
		if mutation.GetAtomicTransform() != nil {
			extra = bsonDecodeWorking(raw)
		}
		if err := reserveRecordWorking(ctx, plans, extra); err != nil {
			return recordMemoryFailure()
		}
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

func bsonValueBytes(current value.Value) int {
	bytes := len(current.Text) + len(current.Data) + 32
	for _, field := range current.Fields {
		bytes += len(field.Name) + bsonValueBytes(field.Value)
	}
	for _, item := range current.Items {
		bytes += bsonValueBytes(item)
	}
	return bytes
}
