package value

import (
	"context"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

// Validate walks the value without Go recursion or data-size admission limits.
// It rejects what a backend codec could not represent; a Value's unused fields
// are not checked, since the codecs read only those of its Kind.
func Validate(ctx context.Context, v Value) error {
	stack := []*Value{&v}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		node := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if err := validateNode(ctx, node); err != nil {
			return err
		}
		for i := range node.Fields {
			stack = append(stack, &node.Fields[i].Value)
		}
		for i := range node.Items {
			stack = append(stack, &node.Items[i])
		}
	}
	return nil
}

func validateNode(ctx context.Context, v *Value) error {
	switch v.Kind {
	case Missing, Null, Bool, Int64, Bytes, Array:
	case Int32:
		if v.Integer < math.MinInt32 || v.Integer > math.MaxInt32 {
			return fmt.Errorf("invalid int32")
		}
	case Float64:
		if math.IsNaN(v.Float) || math.IsInf(v.Float, 0) {
			return fmt.Errorf("invalid float64")
		}
	case String:
		if !utf8.ValidString(v.Text) {
			return fmt.Errorf("invalid UTF-8")
		}
	case Object:
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
		if v.Type == "" || !utf8.ValidString(v.Type) {
			return fmt.Errorf("invalid extended value")
		}
	default:
		return fmt.Errorf("invalid value kind")
	}
	return nil
}
