package mongodb

import (
	"context"
	"strconv"
	"unicode/utf8"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

// setDocument validates a whole-document write: raw BSON whose first and only
// _id matches the resource.
func (native *plan) setDocument(document *pb.Document) *pb.Failure {
	if document.GetContentType() != "application/bson" {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "only raw BSON is supported")
	}
	raw := document.Data
	if !validRecordBSON(raw, 0, false) {
		return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON document")
	}
	mismatch := protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "explicit first _id must match resource")
	first, _, ok := bsoncore.ReadElement(raw[4 : len(raw)-1])
	if !ok || first.Key() != "_id" {
		return mismatch
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
		return mismatch
	}
	native.document = raw
	return nil
}

func validateProgramInput(ctx context.Context, input *pb.Document) *pb.Failure {
	if input == nil {
		return nil
	}
	if input.ContentType != "application/bson" {
		return protocol.Fail(pb.FailureCode_UNSUPPORTED, "MongoDB Lua input must use BSON")
	}
	if _, err := Decode(ctx, input.Data); err != nil {
		return protocol.Fail(pb.FailureCode_INVALID_ARGUMENT, "invalid BSON transform input")
	}
	return nil
}

// Plain writes are forwarded as BSON. Validate the same supported value types,
// UTF-8 and canonical arrays as Decode without constructing a disposable Value
// tree or copying scalar payloads merely to check the first _id.
func validRecordBSON(raw []byte, depth int, array bool) bool {
	if depth > 100 || !bsonFraming(raw) {
		return false
	}
	position := 0
	for rest := raw[4 : len(raw)-1]; len(rest) != 0; {
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
			if !validRecordBSON(item.Data, depth+1, item.Type == bsoncore.TypeArray) {
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
