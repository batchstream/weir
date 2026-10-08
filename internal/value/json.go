package value

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
)

const JSONNumberType = "weir.json.number.v1"

func IsJSONNumber(v Value) bool {
	if v.Kind != Extended || v.Type != JSONNumberType || len(v.Data) == 0 || string(bytes.TrimSpace(v.Data)) != string(v.Data) || !json.Valid(v.Data) {
		return false
	}
	first := v.Data[0]
	return first == '-' || first >= '0' && first <= '9'
}

func DecodeJSON(raw []byte, limits Limits) (Value, error) {
	var zero Value
	if len(raw) == 0 || len(raw) > limits.MaxBytes {
		return zero, fmt.Errorf("JSON size limit")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	used := budget{limits: limits}
	result, err := decodeJSONValue(decoder, 0, &used)
	if err != nil {
		return zero, err
	}
	if _, err := decoder.Token(); err != io.EOF {
		return zero, fmt.Errorf("trailing JSON data")
	}
	if result.Kind != Object {
		return zero, fmt.Errorf("JSON document must be an object")
	}
	return result, nil
}

func decodeJSONValue(decoder *json.Decoder, depth int, used *budget) (Value, error) {
	var zero Value
	if depth > used.limits.MaxDepth {
		return zero, fmt.Errorf("JSON depth limit")
	}
	used.nodes++
	if used.nodes > used.limits.MaxNodes {
		return zero, fmt.Errorf("JSON node limit")
	}
	token, err := decoder.Token()
	if err != nil {
		return zero, err
	}
	switch item := token.(type) {
	case json.Delim:
		switch item {
		case '{':
			result := Value{Kind: Object}
			seen := make(map[string]struct{})
			for decoder.More() {
				keyToken, err := decoder.Token()
				if err != nil {
					return zero, err
				}
				key, ok := keyToken.(string)
				if !ok {
					return zero, fmt.Errorf("invalid JSON object key")
				}
				if _, exists := seen[key]; exists {
					return zero, fmt.Errorf("duplicate JSON object key")
				}
				seen[key] = struct{}{}
				used.bytes += len(key)
				if used.bytes > used.limits.MaxBytes {
					return zero, fmt.Errorf("JSON size limit")
				}
				child, err := decodeJSONValue(decoder, depth+1, used)
				if err != nil {
					return zero, err
				}
				field := Field{Name: key, Value: child}
				result.Fields = append(result.Fields, field)
			}
			close, err := decoder.Token()
			if err != nil {
				return zero, err
			}
			if close != json.Delim('}') {
				return zero, fmt.Errorf("invalid JSON object terminator")
			}
			return result, nil
		case '[':
			result := Value{Kind: Array}
			for decoder.More() {
				child, err := decodeJSONValue(decoder, depth+1, used)
				if err != nil {
					return zero, err
				}
				result.Items = append(result.Items, child)
			}
			close, err := decoder.Token()
			if err != nil {
				return zero, err
			}
			if close != json.Delim(']') {
				return zero, fmt.Errorf("invalid JSON array terminator")
			}
			return result, nil
		default:
			return zero, fmt.Errorf("invalid JSON delimiter")
		}
	case nil:
		decoded := Value{Kind: Null}
		return decoded, nil
	case bool:
		decoded := Value{Kind: Bool, Boolean: item}
		return decoded, nil
	case string:
		used.bytes += len(item)
		if used.bytes > used.limits.MaxBytes {
			return zero, fmt.Errorf("JSON size limit")
		}
		decoded := Value{Kind: String, Text: item}
		return decoded, nil
	case json.Number:
		text := item.String()
		if !strings.ContainsAny(text, ".eE") && text != "-0" {
			integer, err := strconv.ParseInt(text, 10, 64)
			if err == nil {
				kind := Int64
				if integer >= math.MinInt32 && integer <= math.MaxInt32 {
					kind = Int32
				}
				return Integer(kind, integer)
			}
		}
		used.bytes += len(text)
		if used.bytes > used.limits.MaxBytes {
			return zero, fmt.Errorf("JSON size limit")
		}
		decoded := Value{Kind: Extended, Type: JSONNumberType, Data: []byte(text)}
		return decoded, nil
	default:
		return zero, fmt.Errorf("invalid JSON value")
	}
}

func EncodeJSON(v Value, limits Limits) ([]byte, error) {
	if v.Kind != Object {
		return nil, fmt.Errorf("JSON document must be an object")
	}
	if err := Validate(v, limits); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendJSON(&output, v); err != nil {
		return nil, err
	}
	if output.Len() > limits.MaxBytes {
		return nil, fmt.Errorf("JSON size limit")
	}
	return output.Bytes(), nil
}

func appendJSON(output *bytes.Buffer, v Value) error {
	switch v.Kind {
	case Null:
		output.WriteString("null")
	case Bool:
		output.WriteString(strconv.FormatBool(v.Boolean))
	case Int32, Int64:
		output.WriteString(strconv.FormatInt(v.Integer, 10))
	case Float64:
		output.WriteString(strconv.FormatFloat(v.Float, 'g', -1, 64))
	case String:
		quoted, err := json.Marshal(v.Text)
		if err != nil {
			return err
		}
		output.Write(quoted)
	case Extended:
		if !IsJSONNumber(v) {
			return fmt.Errorf("extended value is not a JSON number")
		}
		output.Write(v.Data)
	case Array:
		output.WriteByte('[')
		for i, item := range v.Items {
			if i > 0 {
				output.WriteByte(',')
			}
			if err := appendJSON(output, item); err != nil {
				return err
			}
		}
		output.WriteByte(']')
	case Object:
		output.WriteByte('{')
		for i, field := range v.Fields {
			if i > 0 {
				output.WriteByte(',')
			}
			quoted, err := json.Marshal(field.Name)
			if err != nil {
				return err
			}
			output.Write(quoted)
			output.WriteByte(':')
			if err := appendJSON(output, field.Value); err != nil {
				return err
			}
		}
		output.WriteByte('}')
	default:
		return fmt.Errorf("value kind is not representable in JSON")
	}
	return nil
}
