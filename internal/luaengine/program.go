// Package luaengine evaluates cancellable Lua transforms in the Weir process.
package luaengine

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/value"
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
}

func DefaultLimits() Limits {
	limits := Limits{}
	return limits
}

func (limits Limits) Validate() error {
	if limits.MaxInstructions < 0 || limits.MaxCallDepth < 0 || limits.MaxStackSlots < 0 {
		return fmt.Errorf("invalid Lua VM limits")
	}
	return nil
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

func ValidateProgram(ctx context.Context, program Program) error {
	limits := DefaultLimits()
	if program.Limits != nil {
		limits = *program.Limits
	}
	if err := limits.Validate(); err != nil {
		return err
	}
	if len(program.Source) == 0 ||
		!utf8.ValidString(program.Source) ||
		strings.IndexByte(program.Source, 0) >= 0 ||
		strings.HasPrefix(program.Source, "\x1bLua") {
		return fmt.Errorf("invalid Lua source")
	}
	if err := value.Validate(ctx, program.Current); err != nil {
		return fmt.Errorf("invalid current value: %w", err)
	}
	if err := value.Validate(ctx, program.Input); err != nil {
		return fmt.Errorf("invalid input value: %w", err)
	}
	return nil
}

func ValidateResult(ctx context.Context, result Result) error {
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
		if result.Value.Kind != value.Missing || !utf8.ValidString(result.Message) {
			return fmt.Errorf("invalid rejection")
		}
	default:
		return fmt.Errorf("invalid action")
	}
	if err := value.Validate(ctx, result.Value); err != nil {
		return err
	}
	if result.Action == Replace {
		return validateDocumentValue(ctx, result.Value)
	}
	return nil
}

func validateDocumentValue(ctx context.Context, v value.Value) error {
	type documentFrame struct {
		node  *value.Value
		index int
	}
	first := documentFrame{node: &v}
	stack := []documentFrame{first}
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		frame := &stack[len(stack)-1]
		if frame.node.Kind == value.Missing {
			return fmt.Errorf("document contains missing value")
		}
		if frame.index == len(frame.node.Fields)+len(frame.node.Items) {
			stack = stack[:len(stack)-1]
			continue
		}
		var child *value.Value
		if frame.node.Kind == value.Object {
			child = &frame.node.Fields[frame.index].Value
		} else {
			child = &frame.node.Items[frame.index]
		}
		frame.index++
		next := documentFrame{node: child}
		stack = append(stack, next)
	}
	return nil
}
