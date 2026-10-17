package mongodb

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/value"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

var extendedTypes = map[bson.Type]string{
	bson.TypeObjectID: "mongodb.bson.objectid.v1", bson.TypeDecimal128: "mongodb.bson.decimal128.v1",
	bson.TypeBinary: "mongodb.bson.binary.v1", bson.TypeDateTime: "mongodb.bson.datetime.v1",
	bson.TypeTimestamp: "mongodb.bson.timestamp.v1", bson.TypeRegex: "mongodb.bson.regex.v1",
}

// BSON uses MongoDB's native nesting boundary; document values have no Weir size or node quota.
func Decode(ctx context.Context, raw bson.Raw) (value.Value, error) {
	return decodeObject(ctx, raw, false, 0)
}
func decodeObject(ctx context.Context, raw []byte, array bool, depth int) (value.Value, error) {
	var zero value.Value
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	if depth > 100 || len(raw) < 5 || int(binary.LittleEndian.Uint32(raw)) != len(raw) || raw[len(raw)-1] != 0 {
		return zero, fmt.Errorf("invalid BSON or depth limit")
	}
	result := value.Value{Kind: value.Object}
	if array {
		result.Kind = value.Array
	}
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		el, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return zero, fmt.Errorf("invalid element")
		}
		rest = tail
		key, err := el.KeyErr()
		if err != nil || !utf8.ValidString(key) {
			return zero, fmt.Errorf("invalid field")
		}
		rawValue, err := el.ValueErr()
		if err != nil {
			return zero, err
		}
		rv := bson.RawValue{Type: bson.Type(rawValue.Type), Value: rawValue.Data}
		v, err := decodeValue(ctx, rv, depth)
		if err != nil {
			return zero, err
		}
		if array {
			if key != strconv.Itoa(len(result.Items)) {
				return zero, fmt.Errorf("noncanonical array")
			}
			result.Items = append(result.Items, v)
		} else {
			f := value.Field{Name: key, Value: v}
			result.Fields = append(result.Fields, f)
		}
	}
	return result, nil
}
func decodeValue(ctx context.Context, rv bson.RawValue, depth int) (value.Value, error) {
	var zero value.Value
	v := value.Value{}
	switch rv.Type {
	case bson.TypeEmbeddedDocument:
		return decodeObject(ctx, rv.Value, false, depth+1)
	case bson.TypeArray:
		return decodeObject(ctx, rv.Value, true, depth+1)
	case bson.TypeNull:
		v.Kind = value.Null
	case bson.TypeBoolean:
		if len(rv.Value) != 1 || rv.Value[0] > 1 {
			return zero, fmt.Errorf("invalid bool")
		}
		v.Kind = value.Bool
		v.Boolean = rv.Boolean()
	case bson.TypeInt32:
		v.Kind = value.Int32
		v.Integer = int64(rv.Int32())
	case bson.TypeInt64:
		v.Kind = value.Int64
		v.Integer = rv.Int64()
	case bson.TypeDouble:
		v.Kind = value.Float64
		v.Float = rv.Double()
	case bson.TypeString:
		if len(rv.Value) < 5 || binary.LittleEndian.Uint32(rv.Value) < 1 || rv.Value[len(rv.Value)-1] != 0 {
			return zero, fmt.Errorf("invalid BSON string terminator")
		}
		v.Kind = value.String
		v.Text = rv.StringValue()
		if !utf8.ValidString(v.Text) {
			return zero, fmt.Errorf("invalid UTF8")
		}
	default:
		name, ok := extendedTypes[rv.Type]
		if !ok {
			return zero, fmt.Errorf("unsupported BSON type %v", rv.Type)
		}
		if err := rv.Validate(); err != nil {
			return zero, err
		}
		v.Kind = value.Extended
		v.Type = name
		v.Data = append([]byte(nil), rv.Value...)
	}
	return v, nil
}

// Encode appends every scalar and container into one document buffer. Nested
// documents patch their own length after closing, avoiding ancestor copies.
func Encode(ctx context.Context, v value.Value) (bson.Raw, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if v.Kind != value.Object && v.Kind != value.Array {
		return nil, fmt.Errorf("invalid object or depth")
	}
	type encodeFrame struct {
		node  *value.Value
		start int
		index int
	}
	first := encodeFrame{node: &v}
	stack := []encodeFrame{first}
	output := make([]byte, 4)
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		frame := &stack[len(stack)-1]
		length := len(frame.node.Fields)
		if frame.node.Kind == value.Array {
			length = len(frame.node.Items)
		}
		if frame.index == length {
			output = append(output, 0)
			size := len(output) - frame.start
			if size > math.MaxInt32 {
				return nil, fmt.Errorf("invalid BSON document length")
			}
			binary.LittleEndian.PutUint32(output[frame.start:], uint32(size))
			stack = stack[:len(stack)-1]
			continue
		}
		var name string
		var child *value.Value
		if frame.node.Kind == value.Array {
			name = strconv.Itoa(frame.index)
			child = &frame.node.Items[frame.index]
		} else {
			field := &frame.node.Fields[frame.index]
			name, child = field.Name, &field.Value
		}
		frame.index++
		if !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
			return nil, fmt.Errorf("invalid name")
		}
		switch child.Kind {
		case value.Array, value.Object:
			if len(stack) > 100 {
				return nil, fmt.Errorf("invalid object or depth")
			}
			kind := bsoncore.TypeEmbeddedDocument
			if child.Kind == value.Array {
				kind = bsoncore.TypeArray
			}
			output = bsoncore.AppendHeader(output, kind, name)
			start := len(output)
			output = append(output, 0, 0, 0, 0)
			next := encodeFrame{node: child, start: start}
			stack = append(stack, next)
		case value.Null:
			output = bsoncore.AppendNullElement(output, name)
		case value.Bool:
			output = bsoncore.AppendBooleanElement(output, name, child.Boolean)
		case value.Int32:
			if child.Integer < math.MinInt32 || child.Integer > math.MaxInt32 {
				return nil, fmt.Errorf("int32 overflow")
			}
			output = bsoncore.AppendInt32Element(output, name, int32(child.Integer))
		case value.Int64:
			output = bsoncore.AppendInt64Element(output, name, child.Integer)
		case value.Float64:
			output = bsoncore.AppendDoubleElement(output, name, child.Float)
		case value.String:
			if !utf8.ValidString(child.Text) {
				return nil, fmt.Errorf("invalid UTF8")
			}
			output = bsoncore.AppendStringElement(output, name, child.Text)
		case value.Extended:
			raw := bsoncore.Value{Data: child.Data}
			for kind, typeName := range extendedTypes {
				if typeName == child.Type {
					raw.Type = bsoncore.Type(kind)
					break
				}
			}
			if raw.Type == 0 {
				return nil, fmt.Errorf("unsupported extended type")
			}
			if err := raw.Validate(); err != nil {
				return nil, err
			}
			output = bsoncore.AppendValueElement(output, name, raw)
		default:
			return nil, fmt.Errorf("unsupported value")
		}
	}
	return bson.Raw(output), nil
}
