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
	MaxBytes = 2 << 20
)

type Limits struct {
	MaxDepth, MaxNodes, MaxBytes int
}

func DefaultLimits() Limits {
	limits := Limits{MaxDepth: MaxDepth, MaxNodes: MaxNodes, MaxBytes: MaxBytes}
	return limits
}

func (limits Limits) Validate() error {
	if limits.MaxDepth < 1 || limits.MaxNodes < 1 || limits.MaxBytes < 1 {
		return fmt.Errorf("invalid value limits")
	}
	return nil
}

type budget struct {
	nodes  int
	bytes  int
	limits Limits
}

func Validate(v Value, limits Limits) error {
	used := budget{limits: limits}
	return validate(v, 0, &used)
}

func validate(v Value, depth int, used *budget) error {
	if depth > used.limits.MaxDepth {
		return fmt.Errorf("value depth limit")
	}
	used.nodes++
	if used.nodes > used.limits.MaxNodes {
		return fmt.Errorf("value node limit")
	}
	used.bytes += len(v.Text) + len(v.Data) + len(v.Type)
	if used.bytes > used.limits.MaxBytes {
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
		if len(v.Items) > used.limits.MaxNodes-used.nodes {
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
		if len(v.Fields) > used.limits.MaxNodes-used.nodes {
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
			if used.bytes > used.limits.MaxBytes {
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
