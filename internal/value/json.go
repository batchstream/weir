package value

import (
	"bytes"
	"context"
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

type jsonDecodeFrame struct {
	node *Value
	seen map[string]bool
}

func DecodeJSON(ctx context.Context, raw []byte) (Value, error) {
	var zero Value
	if ctx == nil {
		return zero, fmt.Errorf("JSON decoding context is required")
	}
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	reader := jsonReader{ctx: ctx, reader: bytes.NewReader(raw)}
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	token, err := decoder.Token()
	if err != nil {
		return zero, err
	}
	result, err := jsonTokenValue(token)
	if err != nil || result.Kind != Object {
		return zero, fmt.Errorf("JSON document must be an object")
	}
	first := jsonDecodeFrame{node: &result, seen: make(map[string]bool)}
	stack := []jsonDecodeFrame{first}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return zero, err
		}
		frame := &stack[len(stack)-1]
		if !decoder.More() {
			close, err := decoder.Token()
			if err != nil {
				return zero, err
			}
			if frame.node.Kind == Object && close != json.Delim('}') || frame.node.Kind == Array && close != json.Delim(']') {
				return zero, fmt.Errorf("invalid JSON container terminator")
			}
			stack = stack[:len(stack)-1]
			continue
		}
		name := ""
		if frame.node.Kind == Object {
			key, err := decoder.Token()
			if err != nil {
				return zero, err
			}
			var ok bool
			name, ok = key.(string)
			if !ok || frame.seen[name] {
				return zero, fmt.Errorf("invalid or duplicate JSON object key")
			}
			frame.seen[name] = true
		}
		token, err := decoder.Token()
		if err != nil {
			return zero, err
		}
		child, err := jsonTokenValue(token)
		if err != nil {
			return zero, err
		}
		var destination *Value
		if frame.node.Kind == Object {
			field := Field{Name: name, Value: child}
			frame.node.Fields = append(frame.node.Fields, field)
			destination = &frame.node.Fields[len(frame.node.Fields)-1].Value
		} else {
			frame.node.Items = append(frame.node.Items, child)
			destination = &frame.node.Items[len(frame.node.Items)-1]
		}
		if child.Kind == Object || child.Kind == Array {
			frame := jsonDecodeFrame{node: destination}
			if child.Kind == Object {
				frame.seen = make(map[string]bool)
			}
			stack = append(stack, frame)
		}
	}
	if _, err := decoder.Token(); err != io.EOF {
		return zero, fmt.Errorf("trailing JSON data")
	}
	return result, nil
}

type jsonReader struct {
	ctx    context.Context
	reader *bytes.Reader
}

func (r jsonReader) Read(data []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(data)
}

func jsonTokenValue(token any) (Value, error) {
	var zero Value
	switch item := token.(type) {
	case json.Delim:
		switch item {
		case '{':
			result := Value{Kind: Object}
			return result, nil
		case '[':
			result := Value{Kind: Array}
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
		decoded := Value{Kind: Extended, Type: JSONNumberType, Data: []byte(text)}
		return decoded, nil
	default:
		return zero, fmt.Errorf("invalid JSON value")
	}
}

// MaxJSONDepth is the deepest container nesting that JSON decoders accept.
const MaxJSONDepth = 10000

func EncodeJSON(ctx context.Context, v Value) ([]byte, error) {
	if v.Kind != Object {
		return nil, fmt.Errorf("JSON document must be an object")
	}
	if err := Validate(ctx, v); err != nil {
		return nil, err
	}
	var output bytes.Buffer
	if err := appendJSON(ctx, &output, v); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

type jsonEncodeFrame struct {
	node  *Value
	index int
	open  bool
}

func appendJSON(ctx context.Context, output *bytes.Buffer, v Value) error {
	first := jsonEncodeFrame{node: &v}
	stack := []jsonEncodeFrame{first}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame := &stack[len(stack)-1]
		v := frame.node
		if v.Kind == Object || v.Kind == Array {
			if !frame.open {
				// Only containers stay on the stack while their children encode.
				if len(stack) > MaxJSONDepth {
					return fmt.Errorf("JSON nesting exceeds %d levels", MaxJSONDepth)
				}
				if v.Kind == Object {
					output.WriteByte('{')
				} else {
					output.WriteByte('[')
				}
				frame.open = true
			}
			length := len(v.Fields)
			if v.Kind == Array {
				length = len(v.Items)
			}
			if frame.index == length {
				if v.Kind == Object {
					output.WriteByte('}')
				} else {
					output.WriteByte(']')
				}
				stack = stack[:len(stack)-1]
				continue
			}
			if frame.index != 0 {
				output.WriteByte(',')
			}
			var child *Value
			if v.Kind == Object {
				field := &v.Fields[frame.index]
				quoted, err := json.Marshal(field.Name)
				if err != nil {
					return err
				}
				output.Write(quoted)
				output.WriteByte(':')
				child = &field.Value
			} else {
				child = &v.Items[frame.index]
			}
			frame.index++
			next := jsonEncodeFrame{node: child}
			stack = append(stack, next)
			continue
		}
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
			if !IsJSONNumber(*v) {
				return fmt.Errorf("extended value is not a JSON number")
			}
			output.Write(v.Data)
		default:
			return fmt.Errorf("value kind is not representable in JSON")
		}
		stack = stack[:len(stack)-1]
	}
	return nil
}
