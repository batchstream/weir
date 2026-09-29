package value

import (
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	MaxDepth = 32
	MaxNodes = 4096
	MaxBytes = 256 << 10
)

type budget struct {
	nodes int
	bytes int
}

func Validate(v Value) error {
	var used budget
	return validate(v, 0, &used)
}

func Clone(v Value) (Value, error) {
	var zero Value
	if err := Validate(v); err != nil {
		return zero, err
	}
	var used budget
	return clone(v, 0, &used)
}

func Merge(base, patch Value) (Value, error) {
	var zero Value
	if base.Kind == Missing {
		base = Value{Kind: Object}
	}
	if base.Kind != Object || patch.Kind != Object {
		return zero, fmt.Errorf("merge operands must be objects")
	}
	if err := Validate(base); err != nil {
		return zero, fmt.Errorf("invalid base: %w", err)
	}
	if err := Validate(patch); err != nil {
		return zero, fmt.Errorf("invalid patch: %w", err)
	}
	var used budget
	result, err := mergeObjects(base, patch, 0, &used)
	if err != nil {
		return zero, err
	}
	if err := Validate(result); err != nil {
		return zero, fmt.Errorf("invalid merged value: %w", err)
	}
	if containsMissing(result) {
		return zero, fmt.Errorf("merged object contains missing array values")
	}
	return result, nil
}

func containsMissing(v Value) bool {
	if v.Kind == Missing {
		return true
	}
	for _, field := range v.Fields {
		if containsMissing(field.Value) {
			return true
		}
	}
	for _, item := range v.Items {
		if containsMissing(item) {
			return true
		}
	}
	return false
}

func validate(v Value, depth int, used *budget) error {
	if depth > MaxDepth {
		return fmt.Errorf("value depth limit")
	}
	used.nodes++
	if used.nodes > MaxNodes {
		return fmt.Errorf("value node limit")
	}
	used.bytes += len(v.Text) + len(v.Data) + len(v.Type)
	if used.bytes > MaxBytes {
		return fmt.Errorf("value byte limit")
	}
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
		if len(v.Items) > MaxNodes-used.nodes {
			return fmt.Errorf("value node limit")
		}
		for _, item := range v.Items {
			if err := validate(item, depth+1, used); err != nil {
				return err
			}
		}
	case Object:
		if v.Integer != 0 || v.Float != 0 || v.Boolean || v.Text != "" || len(v.Data) != 0 || len(v.Items) != 0 || v.Type != "" {
			return fmt.Errorf("noncanonical object")
		}
		if len(v.Fields) > MaxNodes-used.nodes {
			return fmt.Errorf("value node limit")
		}
		seen := make(map[string]struct{}, len(v.Fields))
		for _, field := range v.Fields {
			if !utf8.ValidString(field.Name) || strings.IndexByte(field.Name, 0) >= 0 {
				return fmt.Errorf("invalid field name")
			}
			if _, ok := seen[field.Name]; ok {
				return fmt.Errorf("duplicate field")
			}
			seen[field.Name] = struct{}{}
			used.bytes += len(field.Name)
			if used.bytes > MaxBytes {
				return fmt.Errorf("value byte limit")
			}
			if err := validate(field.Value, depth+1, used); err != nil {
				return err
			}
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
	if allowed != "integer" && v.Integer != 0 || allowed != "float" && v.Float != 0 || allowed != "boolean" && v.Boolean || allowed != "text" && v.Text != "" || allowed != "data" && len(v.Data) != 0 || len(v.Fields) != 0 || len(v.Items) != 0 || v.Type != "" {
		return true
	}
	return false
}

func mergeObjects(base, patch Value, depth int, used *budget) (Value, error) {
	if depth > MaxDepth {
		var zero Value
		return zero, fmt.Errorf("value depth limit")
	}
	patchFields := make(map[string]Value, len(patch.Fields))
	for _, field := range patch.Fields {
		patchFields[field.Name] = field.Value
	}
	baseNames := make(map[string]struct{}, len(base.Fields))
	result := Value{Kind: Object, Fields: make([]Field, 0, len(base.Fields)+len(patch.Fields))}
	for _, field := range base.Fields {
		baseNames[field.Name] = struct{}{}
		child, exists := patchFields[field.Name]
		if exists && child.Kind == Missing {
			continue
		}
		if !exists {
			copied, err := clone(field.Value, depth+1, used)
			if err != nil {
				var zero Value
				return zero, err
			}
			field.Value = copied
		} else if child.Kind == Object {
			baseObject := field.Value
			if baseObject.Kind != Object {
				baseObject = Value{Kind: Object}
			}
			merged, err := mergeObjects(baseObject, child, depth+1, used)
			if err != nil {
				var zero Value
				return zero, err
			}
			field.Value = merged
		} else {
			copied, err := clone(child, depth+1, used)
			if err != nil {
				var zero Value
				return zero, err
			}
			field.Value = copied
		}
		result.Fields = append(result.Fields, field)
	}
	for _, field := range patch.Fields {
		if _, exists := baseNames[field.Name]; exists || field.Value.Kind == Missing {
			continue
		}
		copied := field.Value
		var err error
		if copied.Kind == Object {
			copied, err = mergeObjects(Value{Kind: Object}, copied, depth+1, used)
		} else {
			copied, err = clone(copied, depth+1, used)
		}
		if err != nil {
			var zero Value
			return zero, err
		}
		field.Value = copied
		result.Fields = append(result.Fields, field)
	}
	used.nodes++
	if used.nodes > MaxNodes {
		var zero Value
		return zero, fmt.Errorf("value node limit")
	}
	return result, nil
}

func clone(v Value, depth int, used *budget) (Value, error) {
	if depth > MaxDepth {
		var zero Value
		return zero, fmt.Errorf("value depth limit")
	}
	used.nodes++
	if used.nodes > MaxNodes {
		var zero Value
		return zero, fmt.Errorf("value node limit")
	}
	used.bytes += len(v.Text) + len(v.Data) + len(v.Type)
	if used.bytes > MaxBytes {
		var zero Value
		return zero, fmt.Errorf("value byte limit")
	}
	result := v
	result.Data = append([]byte(nil), v.Data...)
	if len(v.Fields) != 0 {
		result.Fields = make([]Field, 0, len(v.Fields))
		for _, field := range v.Fields {
			used.bytes += len(field.Name)
			if used.bytes > MaxBytes {
				var zero Value
				return zero, fmt.Errorf("value byte limit")
			}
			child, err := clone(field.Value, depth+1, used)
			if err != nil {
				var zero Value
				return zero, err
			}
			field.Value = child
			result.Fields = append(result.Fields, field)
		}
	}
	if len(v.Items) != 0 {
		result.Items = make([]Value, 0, len(v.Items))
		for _, item := range v.Items {
			child, err := clone(item, depth+1, used)
			if err != nil {
				var zero Value
				return zero, err
			}
			result.Items = append(result.Items, child)
		}
	}
	return result, nil
}
