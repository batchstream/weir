// Package luaengine evaluates bounded Lua transforms in the Weir process.
package luaengine

import (
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/value"
)

const (
	MaxSourceBytes  = 16 << 10
	MaxMessageBytes = 1024
)

type Action string

const (
	Keep    Action = "keep"
	Replace Action = "replace"
	Delete  Action = "delete"
	Reject  Action = "reject"
)

type Limits struct {
	MaxInstructions             int64
	MaxCallDepth, MaxStackSlots int
	Values                      value.Limits
}

func DefaultLimits() Limits {
	limits := Limits{Values: value.DefaultLimits()}
	return limits
}

func (limits Limits) Validate() error {
	if limits.MaxInstructions < 0 || limits.MaxCallDepth < 0 || limits.MaxStackSlots < 0 {
		return fmt.Errorf("invalid Lua VM limits")
	}
	return limits.Values.Validate()
}

type Program struct {
	Limits     *Limits
	Source     string
	Current    value.Value
	Input      value.Value
	ObservedAt time.Time
}

type Result struct {
	Action  Action
	Value   value.Value
	Message string
}

func ValidateProgram(program Program) error {
	limits := DefaultLimits()
	if program.Limits != nil {
		limits = *program.Limits
	}
	if err := limits.Validate(); err != nil {
		return err
	}
	if len(program.Source) == 0 ||
		len(program.Source) > MaxSourceBytes ||
		!utf8.ValidString(program.Source) ||
		strings.IndexByte(program.Source, 0) >= 0 ||
		strings.HasPrefix(program.Source, "\x1bLua") {
		return fmt.Errorf("invalid or oversized Lua source")
	}
	if err := value.Validate(program.Current, limits.Values); err != nil {
		return fmt.Errorf("invalid current value: %w", err)
	}
	if err := value.Validate(program.Input, limits.Values); err != nil {
		return fmt.Errorf("invalid input value: %w", err)
	}
	return nil
}

func ValidateResult(result Result, limits Limits) error {
	switch result.Action {
	case Keep, Delete:
		if result.Value.Kind != value.Missing || result.Message != "" {
			return fmt.Errorf("unexpected action payload")
		}
	case Replace:
		if result.Value.Kind != value.Object || result.Message != "" {
			return fmt.Errorf("replacement must be an object")
		}
	case Reject:
		if result.Value.Kind != value.Missing || len(result.Message) > MaxMessageBytes || !utf8.ValidString(result.Message) {
			return fmt.Errorf("invalid rejection")
		}
	default:
		return fmt.Errorf("invalid action")
	}
	if err := value.Validate(result.Value, limits.Values); err != nil {
		return err
	}
	if result.Action == Replace {
		return validateDocumentValue(result.Value, 0, limits.Values)
	}
	return nil
}

func validateDocumentValue(v value.Value, depth int, limits value.Limits) error {
	if depth > limits.MaxDepth || v.Kind == value.Missing {
		return fmt.Errorf("replacement contains missing value or exceeds depth")
	}
	for _, field := range v.Fields {
		if err := validateDocumentValue(field.Value, depth+1, limits); err != nil {
			return err
		}
	}
	for _, item := range v.Items {
		if err := validateDocumentValue(item, depth+1, limits); err != nil {
			return err
		}
	}
	return nil
}
