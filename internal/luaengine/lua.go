package luaengine

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/batchstream/weir/internal/execution"
	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/vm"
)

var errInvalidLuaResult = errors.New("Lua callback must return one object or explicit action")

func Evaluate(ctx context.Context, program Program) (result Result, err error) {
	defer func() {
		if budget.IsExhausted(err) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			err = fmt.Errorf("%w: %w", execution.ErrExecutionMemory, err)
		}
		if err != nil {
			result = Result{}
		}
	}()
	defer budget.Recover(&err)
	var empty Result
	if ctx == nil {
		return empty, errors.New("Lua execution context is required")
	}
	if program.ObservedAt.IsZero() {
		program.ObservedAt = time.Now()
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	scope := newEvaluationBudget(ctx, program.Reserve)
	if err := validateProgram(ctx, program, scope); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	block, err := parser.ParseWithBudget("transform.lua", program.Source, scope)
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	prototype, err := compiler.Compile("transform.lua", block, compiler.WithBudget(scope))
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	settings := DefaultLimits()
	if program.Limits != nil {
		settings = *program.Limits
	}
	limits := vm.Limits{
		MaxCallDepth:    settings.MaxCallDepth,
		MaxStackSlots:   settings.MaxStackSlots,
		MaxInstructions: settings.MaxInstructions,
		MinGCInterval:   -1,
	}
	state := vm.New(vm.WithContext(ctx), vm.WithLimits(limits), vm.WithBudget(scope))
	defer state.Close(context.Background())
	module := installEnvironment(state, program.ObservedAt)
	conversion := newBridge(ctx, state)
	conversion.install(state, module)
	functions, err := state.Run(prototype)
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	if len(functions) != 1 || !functions[0].IsFunction() {
		return empty, errors.New("Lua source must return exactly one function")
	}
	current, err := conversion.toLua(program.Current)
	if err != nil {
		return empty, err
	}
	incoming, err := conversion.toLua(program.Input)
	if err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	var argument vm.Value
	scope.RequireArray(unsafe.Sizeof(argument), 2)
	arguments := []vm.Value{current, incoming}
	outputs, err := state.ProtectedCall(functions[0], arguments)
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	if err := state.CheckInterrupt(); err != nil {
		return empty, err
	}
	if len(outputs) != 1 {
		return empty, errInvalidLuaResult
	}
	result = Result{Action: Replace}
	if userdata := outputs[0].AsUserdata(); userdata != nil {
		action, ok := userdata.Data.(Result)
		if !ok {
			return empty, errInvalidLuaResult
		}
		result = action
		result.Message = cloneLuaText(scope, result.Message)
	} else {
		result.Value, err = conversion.fromLua(outputs[0])
		if err != nil {
			return empty, fmt.Errorf("invalid Lua result: %w", err)
		}
	}
	if err := validateResult(ctx, result, scope); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if err := scope.Check(); err != nil {
		return empty, err
	}
	result.Allocations = scope
	return result, nil
}

func luaKeep(state *vm.VM) int {
	if state.ArgCount() != 0 {
		panic("weir.keep takes no arguments")
	}
	result := Result{Action: Keep}
	state.Budget().RequireArray(unsafe.Sizeof(result), 1)
	state.Set(0, state.NewUserdataValue(result, nil))
	return 1
}

func luaDelete(state *vm.VM) int {
	if state.ArgCount() != 0 {
		panic("weir.delete takes no arguments")
	}
	result := Result{Action: Delete}
	state.Budget().RequireArray(unsafe.Sizeof(result), 1)
	state.Set(0, state.NewUserdataValue(result, nil))
	return 1
}

func luaReject(state *vm.VM) int {
	if state.ArgCount() != 1 || !state.Get(1).IsString() {
		panic("weir.reject requires one message string")
	}
	result := Result{Action: Reject, Message: state.Get(1).AsString()}
	if !utf8.ValidString(result.Message) {
		panic("invalid rejection")
	}
	state.Budget().RequireArray(unsafe.Sizeof(result), 1)
	state.Set(0, state.NewUserdataValue(result, nil))
	return 1
}
