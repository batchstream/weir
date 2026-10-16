package mongodb

import (
	"encoding/binary"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/x/bsonx/bsoncore"
)

func bsonCommandOK(value bson.RawValue) bool {
	switch value.Type {
	case bson.TypeDouble:
		return value.Double() == 1
	case bson.TypeInt32:
		return value.Int32() == 1
	case bson.TypeInt64:
		return value.Int64() == 1
	}
	return false
}

// Validation walks bounded raw bytes; it never builds a native document tree or
// converts BSON types. MongoDB's native maximum nesting depth is 100.
func validBSONDocument(raw []byte, depth int) bool {
	if depth > 100 || !bsonFraming(raw) {
		return false
	}
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return false
		}
		rest = tail
		value, err := element.ValueErr()
		if err != nil {
			return false
		}
		switch value.Type {
		case bsoncore.TypeEmbeddedDocument, bsoncore.TypeArray:
			if !validBSONDocument(value.Data, depth+1) {
				return false
			}
		case bsoncore.TypeCodeWithScope:
			_, scope, ok := value.CodeWithScopeOK()
			if !ok || !validBSONDocument(scope, depth+1) {
				return false
			}
		default:
			if value.Validate() != nil {
				return false
			}
		}
	}
	return true
}

func bsonFraming(raw []byte) bool {
	return len(raw) >= 5 && int64(binary.LittleEndian.Uint32(raw)) == int64(len(raw)) && raw[len(raw)-1] == 0
}

func bsonFields(raw []byte) (map[string]bson.RawValue, error) {
	if !bsonFraming(raw) {
		return nil, fmt.Errorf("invalid BSON framing")
	}
	fields := make(map[string]bson.RawValue)
	rest := raw[4 : len(raw)-1]
	for len(rest) > 0 {
		element, tail, ok := bsoncore.ReadElement(rest)
		if !ok {
			return nil, fmt.Errorf("invalid element")
		}
		rest = tail
		key, err := element.KeyErr()
		if err != nil {
			return nil, err
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate field")
		}
		value, err := element.ValueErr()
		if err != nil {
			return nil, err
		}
		fields[key] = bson.RawValue{Type: bson.Type(value.Type), Value: value.Data}
	}
	return fields, nil
}
