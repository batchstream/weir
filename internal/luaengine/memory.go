package luaengine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/budget"
)

const standaloneMemoryBytes = 64 << 20

func standaloneReserve(bytes int) error {
	if bytes > standaloneMemoryBytes {
		return execution.ErrExecutionMemory
	}
	return nil
}

func newEvaluationBudget(ctx context.Context, reserve func(int) error) *budget.Budget {
	if reserve == nil {
		reserve = standaloneReserve
	}
	options := budget.Options{Context: ctx, Reserve: reserve}
	return budget.New(options)
}

func cloneLuaText(scope *budget.Budget, text string) string {
	if text == "" {
		return ""
	}
	scope.RequireArray(1, len(text))
	return strings.Clone(text)
}

type validationReservation struct {
	node    *value.Value
	index   int
	entered bool
}

// Value validation allocates path and duplicate-key maps. Reserve that work
// before invoking it, including each occurrence of a shared input subtree.
// The bounded prepass also prevents a compact DAG from expanding unchecked.
func ReserveValueValidation(scope *budget.Budget, v value.Value) error {
	var reservation validationReservation
	scope.Require(256)
	active := make(map[*value.Value]bool)
	first := validationReservation{node: &v}
	stack := budget.Append(scope, []validationReservation(nil), first)
	depth := 0
	for len(stack) != 0 {
		if err := scope.Check(); err != nil {
			return err
		}
		frame := &stack[len(stack)-1]
		if !frame.entered {
			if active[frame.node] {
				return nil // The normal validator supplies the canonical error.
			}
			// Covers the two path-map entries and their geometric growth.
			scope.Require(256)
			active[frame.node] = true
			if frame.node.Kind == value.Object {
				scope.RequireArray(64, len(frame.node.Fields))
			}
			if len(stack) > depth {
				scope.RequireArray(unsafe.Sizeof(reservation), 2*len(stack))
				depth = 2 * len(stack)
			}
			frame.entered = true
		}
		if frame.index == len(frame.node.Fields)+len(frame.node.Items) {
			delete(active, frame.node)
			stack = stack[:len(stack)-1]
			continue
		}
		var child *value.Value
		if frame.node.Kind == value.Object {
			if frame.index >= len(frame.node.Fields) {
				return nil
			}
			child = &frame.node.Fields[frame.index].Value
		} else if frame.index < len(frame.node.Items) {
			child = &frame.node.Items[frame.index]
		} else {
			return nil // Noncanonical containers fail in the normal validator.
		}
		frame.index++
		next := validationReservation{node: child}
		stack = budget.Append(scope, stack, next)
	}
	return nil
}

// ValueBytes estimates the existing BSON or JSON encoding workspace without
// recursion. Its traversal allocations use the evaluation's retained scope.
func (r Result) ValueBytes(ctx context.Context, v value.Value, quoted bool) (bytes int, err error) {
	defer func() {
		if budget.IsExhausted(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %w", execution.ErrExecutionMemory, err)
		}
	}()
	defer budget.Recover(&err)
	scope := r.Allocations
	if scope == nil {
		scope = newEvaluationBudget(ctx, nil)
	}
	factor := 1
	if quoted {
		factor = 6
	}
	type sizeFrame struct {
		node  *value.Value
		index int
	}
	first := sizeFrame{node: &v, index: -1}
	stack := budget.Append(scope, []sizeFrame(nil), first)
	for len(stack) != 0 {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		frame := &stack[len(stack)-1]
		if frame.index < 0 {
			if len(frame.node.Text) > (math.MaxInt-32)/factor {
				scope.Require(-1)
			}
			additional := factor*len(frame.node.Text) + 32
			if len(frame.node.Data) > math.MaxInt-additional || bytes > math.MaxInt-additional-len(frame.node.Data) {
				scope.Require(-1)
			}
			bytes += additional + len(frame.node.Data)
			frame.index = 0
		}
		if frame.index == len(frame.node.Fields)+len(frame.node.Items) {
			stack = stack[:len(stack)-1]
			continue
		}
		var child *value.Value
		if frame.node.Kind == value.Object {
			field := &frame.node.Fields[frame.index]
			if len(field.Name) > (math.MaxInt-bytes)/factor {
				scope.Require(-1)
			}
			bytes += factor * len(field.Name)
			child = &field.Value
		} else {
			child = &frame.node.Items[frame.index]
		}
		frame.index++
		next := sizeFrame{node: child, index: -1}
		stack = budget.Append(scope, stack, next)
	}
	return bytes, nil
}
