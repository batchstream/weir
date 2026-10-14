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
func Encode(ctx context.Context, v value.Value) (bson.Raw, error) {
	data, err := encodeObject(ctx, v, 0)
	return bson.Raw(data), err
}
func encodeObject(ctx context.Context, v value.Value, depth int) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > 100 || v.Kind != value.Object && v.Kind != value.Array {
		return nil, fmt.Errorf("invalid object or depth")
	}
	out := make([]byte, 4)
	n := len(v.Fields)
	if v.Kind == value.Array {
		n = len(v.Items)
	}
	for i := 0; i < n; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var name string
		var child value.Value
		if v.Kind == value.Array {
			name = strconv.Itoa(i)
			child = v.Items[i]
		} else {
			name = v.Fields[i].Name
			child = v.Fields[i].Value
		}
		if !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
			return nil, fmt.Errorf("invalid name")
		}
		rv, err := encodeValue(ctx, child, depth)
		if err != nil {
			return nil, err
		}
		out = bsoncore.AppendValueElement(out, name, rv)
	}
	out = append(out, 0)
	binary.LittleEndian.PutUint32(out, uint32(len(out)))
	return out, nil
}
func encodeValue(ctx context.Context, v value.Value, depth int) (bsoncore.Value, error) {
	var zero bsoncore.Value
	rv := bsoncore.Value{}
	switch v.Kind {
	case value.Null:
		rv.Type = bsoncore.TypeNull
	case value.Bool:
		rv.Type = bsoncore.TypeBoolean
		rv.Data = bsoncore.AppendBoolean(nil, v.Boolean)
	case value.Int32:
		if v.Integer < math.MinInt32 || v.Integer > math.MaxInt32 {
			return zero, fmt.Errorf("int32 overflow")
		}
		rv.Type = bsoncore.TypeInt32
		rv.Data = bsoncore.AppendInt32(nil, int32(v.Integer))
	case value.Int64:
		rv.Type = bsoncore.TypeInt64
		rv.Data = bsoncore.AppendInt64(nil, v.Integer)
	case value.Float64:
		rv.Type = bsoncore.TypeDouble
		rv.Data = bsoncore.AppendDouble(nil, v.Float)
	case value.String:
		if !utf8.ValidString(v.Text) {
			return zero, fmt.Errorf("invalid UTF8")
		}
		rv.Type = bsoncore.TypeString
		rv.Data = bsoncore.AppendString(nil, v.Text)
	case value.Array, value.Object:
		data, err := encodeObject(ctx, v, depth+1)
		if err != nil {
			return zero, err
		}
		rv.Data = data
		rv.Type = bsoncore.TypeEmbeddedDocument
		if v.Kind == value.Array {
			rv.Type = bsoncore.TypeArray
		}
	case value.Extended:
		for t, name := range extendedTypes {
			if name == v.Type {
				rv.Type = bsoncore.Type(t)
				break
			}
		}
		if rv.Type == 0 {
			return zero, fmt.Errorf("unsupported extended type")
		}
		rv.Data = v.Data
		if err := rv.Validate(); err != nil {
			return zero, err
		}
	default:
		return zero, fmt.Errorf("unsupported value")
	}
	return rv, nil
}
