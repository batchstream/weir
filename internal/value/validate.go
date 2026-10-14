package value

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

type validationFrame struct {
	node    *Value
	index   int
	entered bool
}

// Validate walks the value without Go recursion or data-size admission limits.
// Track the active path rather than all visited nodes so shared trees stay valid.
func Validate(ctx context.Context, v Value) error {
	if ctx == nil {
		return fmt.Errorf("value validation context is required")
	}
	first := validationFrame{node: &v}
	stack := []validationFrame{first}
	active := make(map[*Value]bool)
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame := &stack[len(stack)-1]
		if !frame.entered {
			if active[frame.node] {
				return fmt.Errorf("cyclic value")
			}
			if err := validateNode(ctx, *frame.node); err != nil {
				return err
			}
			active[frame.node] = true
			frame.entered = true
		}
		if frame.index == len(frame.node.Fields)+len(frame.node.Items) {
			delete(active, frame.node)
			stack = stack[:len(stack)-1]
			continue
		}
		var node *Value
		if frame.node.Kind == Object {
			node = &frame.node.Fields[frame.index].Value
		} else {
			node = &frame.node.Items[frame.index]
		}
		frame.index++
		child := validationFrame{node: node}
		stack = append(stack, child)
	}
	return nil
}

func validateNode(ctx context.Context, v Value) error {
	if !utf8.ValidString(v.Text) || !utf8.ValidString(v.Type) {
		return fmt.Errorf("invalid UTF-8")
	}
	switch v.Kind {
	case Missing, Null:
		if v.Integer != 0 || v.Float != 0 || v.Boolean || v.Text != "" || len(v.Data) != 0 || len(v.Fields) != 0 || len(v.Items) != 0 || v.Type != "" {
			return fmt.Errorf("noncanonical missing or null")
		}
	case Bool:
		if hasPayloadExcept(v, "boolean") {
			return fmt.Errorf("noncanonical boolean")
		}
	case Int32:
		if v.Integer < math.MinInt32 || v.Integer > math.MaxInt32 || hasPayloadExcept(v, "integer") {
			return fmt.Errorf("invalid int32")
		}
	case Int64:
		if hasPayloadExcept(v, "integer") {
			return fmt.Errorf("noncanonical int64")
		}
	case Float64:
		if math.IsNaN(v.Float) || math.IsInf(v.Float, 0) || hasPayloadExcept(v, "float") {
			return fmt.Errorf("invalid float64")
		}
	case String:
		if hasPayloadExcept(v, "text") {
			return fmt.Errorf("noncanonical string")
		}
	case Bytes:
		if hasPayloadExcept(v, "data") {
			return fmt.Errorf("noncanonical bytes")
		}
	case Array:
		if v.Integer != 0 || v.Float != 0 || v.Boolean || v.Text != "" || len(v.Data) != 0 || len(v.Fields) != 0 || v.Type != "" {
			return fmt.Errorf("noncanonical array")
		}
	case Object:
		if v.Integer != 0 || v.Float != 0 || v.Boolean || v.Text != "" || len(v.Data) != 0 || len(v.Items) != 0 || v.Type != "" {
			return fmt.Errorf("noncanonical object")
		}
		seen := make(map[string]struct{}, len(v.Fields))
		for _, field := range v.Fields {
			if err := ctx.Err(); err != nil {
				return err
			}
			if !utf8.ValidString(field.Name) || strings.IndexByte(field.Name, 0) >= 0 {
				return fmt.Errorf("invalid field name")
			}
			if _, ok := seen[field.Name]; ok {
				return fmt.Errorf("duplicate field")
			}
			seen[field.Name] = struct{}{}
		}
	case Extended:
		if v.Type == "" || v.Integer != 0 || v.Float != 0 || v.Boolean || v.Text != "" || len(v.Fields) != 0 || len(v.Items) != 0 {
			return fmt.Errorf("invalid extended value")
		}
	default:
		return fmt.Errorf("invalid value kind")
	}
	return nil
}

func hasPayloadExcept(v Value, allowed string) bool {
	if allowed != "integer" && v.Integer != 0 ||
		allowed != "float" && v.Float != 0 ||
		allowed != "boolean" && v.Boolean ||
		allowed != "text" && v.Text != "" ||
		allowed != "data" && len(v.Data) != 0 ||
		len(v.Fields) != 0 ||
		len(v.Items) != 0 ||
		v.Type != "" {
		return true
	}
	return false
}
