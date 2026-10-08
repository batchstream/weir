package luaengine

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/vm"
)

var errInvalidLuaResult = errors.New("Lua callback must return one object or explicit action")

func Evaluate(ctx context.Context, program Program) (Result, error) {
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
	if err := ValidateProgram(program); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	compileLimits := compiler.CompilerLimits{MaxVars: 128, MaxRegs: 128, MaxUpvals: 128}
	block, err := parser.ParseWithMaxVars("transform.lua", program.Source, compileLimits.MaxVars)
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	prototype, err := compiler.Compile("transform.lua", block, compiler.WithLimits(compileLimits))
	if contextErr := ctx.Err(); contextErr != nil {
		return empty, contextErr
	}
	if err != nil {
		return empty, err
	}
	limits := vm.Limits{
		MaxCallDepth:    128,
		MaxStackSlots:   4096,
		MaxInstructions: 1_000_000,
		MinGCInterval:   -1,
	}
	state := vm.New(vm.WithContext(ctx), vm.WithLimits(limits))
	defer state.Close(context.Background())
	module := installEnvironment(state, program.ObservedAt)
	conversion := newBridge()
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
	result := Result{Action: Replace}
	if userdata := outputs[0].AsUserdata(); userdata != nil {
		action, ok := userdata.Data.(Result)
		if !ok {
			return empty, errInvalidLuaResult
		}
		result = action
	} else {
		result.Value, err = conversion.fromLua(outputs[0])
		if err != nil {
			return empty, fmt.Errorf("invalid Lua result: %w", err)
		}
	}
	if err := ValidateResult(result); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return result, nil
}

func luaKeep(state *vm.VM) int {
	if state.ArgCount() != 0 {
		panic("weir.keep takes no arguments")
	}
	result := Result{Action: Keep}
	state.Set(0, vm.NewUserdataValue(result, nil))
	return 1
}

func luaDelete(state *vm.VM) int {
	if state.ArgCount() != 0 {
		panic("weir.delete takes no arguments")
	}
	result := Result{Action: Delete}
	state.Set(0, vm.NewUserdataValue(result, nil))
	return 1
}

func luaReject(state *vm.VM) int {
	if state.ArgCount() != 1 || !state.Get(1).IsString() {
		panic("weir.reject requires one message string")
	}
	result := Result{Action: Reject, Message: state.Get(1).AsString()}
	if err := ValidateResult(result); err != nil {
		panic(err)
	}
	state.Set(0, vm.NewUserdataValue(result, nil))
	return 1
}
