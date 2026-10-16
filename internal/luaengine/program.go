// Package luaengine evaluates cancellable Lua transforms in the Weir process.
package luaengine

import (
	"context"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/budget"
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
	// Reserve grows the complete evaluation's allocation promise. The caller
	// retains it while returned values or VM-derived strings remain reachable.
	Reserve    func(totalBytes int) error
	Limits     *Limits
	Source     string
	Current    value.Value
	Input      value.Value
	ObservedAt time.Time
}

type Result struct {
	Allocations *budget.Budget
	Action      Action
	Value       value.Value
	Message     string
}

func ValidateProgram(ctx context.Context, program Program) (err error) {
	defer budget.Recover(&err)
	scope := newEvaluationBudget(ctx, program.Reserve)
	return validateProgram(ctx, program, scope)
}

func validateProgram(ctx context.Context, program Program, scope *budget.Budget) error {
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
	if err := ReserveValueValidation(scope, program.Current); err != nil {
		return err
	}
	if err := value.Validate(ctx, program.Current); err != nil {
		return fmt.Errorf("invalid current value: %w", err)
	}
	if err := ReserveValueValidation(scope, program.Input); err != nil {
		return err
	}
	if err := value.Validate(ctx, program.Input); err != nil {
		return fmt.Errorf("invalid input value: %w", err)
	}
	return nil
}

func ValidateResult(ctx context.Context, result Result) (err error) {
	defer budget.Recover(&err)
	scope := newEvaluationBudget(ctx, nil)
	return validateResult(ctx, result, scope)
}

func validateResult(ctx context.Context, result Result, scope *budget.Budget) error {
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
	if err := ReserveValueValidation(scope, result.Value); err != nil {
		return err
	}
	if err := value.Validate(ctx, result.Value); err != nil {
		return err
	}
	if result.Action == Replace {
		return validateDocumentValue(ctx, result.Value, scope)
	}
	return nil
}

func validateDocumentValue(ctx context.Context, v value.Value, scope *budget.Budget) error {
	type documentFrame struct {
		node  *value.Value
		index int
	}
	first := documentFrame{node: &v}
	stack := budget.Append(scope, []documentFrame(nil), first)
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
		stack = budget.Append(scope, stack, next)
	}
	return nil
}
