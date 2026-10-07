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
	MaxSourceBytes   = 16 << 10
	MaxMessageBytes  = 1024
	ExecutionTimeout = 500 * time.Millisecond
	maxConcurrent    = 4
)

type Action string

const (
	Keep    Action = "keep"
	Replace Action = "replace"
	Delete  Action = "delete"
	Reject  Action = "reject"
)

type Program struct {
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
	if len(program.Source) == 0 ||
		len(program.Source) > MaxSourceBytes ||
		!utf8.ValidString(program.Source) ||
		strings.IndexByte(program.Source, 0) >= 0 ||
		strings.HasPrefix(program.Source, "\x1bLua") {
		return fmt.Errorf("invalid or oversized Lua source")
	}
	if err := value.Validate(program.Current); err != nil {
		return fmt.Errorf("invalid current value: %w", err)
	}
	if err := value.Validate(program.Input); err != nil {
		return fmt.Errorf("invalid input value: %w", err)
	}
	return nil
}

func ValidateResult(result Result) error {
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
	if err := value.Validate(result.Value); err != nil {
		return err
	}
	if result.Action == Replace {
		return validateDocumentValue(result.Value, 0)
	}
	return nil
}

func validateDocumentValue(v value.Value, depth int) error {
	if depth > value.MaxDepth || v.Kind == value.Missing {
		return fmt.Errorf("replacement contains missing value or exceeds depth")
	}
	for _, field := range v.Fields {
		if err := validateDocumentValue(field.Value, depth+1); err != nil {
			return err
		}
	}
	for _, item := range v.Items {
		if err := validateDocumentValue(item, depth+1); err != nil {
			return err
		}
	}
	return nil
}
