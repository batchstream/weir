package vm_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

func TestNativeProtectedCallsReserveBeforeEveryRecursiveEntry(t *testing.T) {
	// The compiler's highest usable freeReg is 254: one function plus 253 args.
	// pcall is itself native: this reaches the bottom Lua closure only after
	// 253 nested native entries, all of which need advance Go-stack credit.
	const argumentCount = 253
	source := "pcall(" + strings.Repeat("pcall,", argumentCount-1) + "function() reached=true end)"
	block, err := parser.Parse("native-stack", source)
	if err != nil {
		t.Fatal(err)
	}
	prototype, err := compiler.Compile("native-stack", block)
	if err != nil {
		t.Fatal(err)
	}
	for _, limit := range []int{32 << 20, 4 << 20} {
		label := "roomy"
		if limit == 4<<20 {
			label = "denied"
		}
		t.Run(label, func(t *testing.T) {
			var state *vm.VM
			last, payments, deepest := 0, 0, 0
			options := budget.Options{Context: t.Context(), Reserve: func(total int) error {
				if state != nil {
					deepest = max(deepest, state.StackDepth())
				}
				// One 32 KiB physical prepayment is 32 KiB + 25% + 64.
				if total-last == 41_024 {
					payments++
				}
				if total > limit {
					return errTestMemory
				}
				last = total
				return nil
			}}
			owner := budget.New(options)
			state = vm.New(vm.WithBudget(owner))
			defer state.Close(context.Background())
			stdlib.Open(state)
			var runErr error
			func() {
				defer budget.Recover(&runErr)
				_, runErr = state.Run(prototype)
			}()
			if limit == 32<<20 {
				if runErr != nil || state.GetGlobal("reached").IsNil() || payments < argumentCount {
					t.Fatal("native recursion lacked per-entry stack prepayment", runErr, payments, deepest)
				}
			} else if !errors.Is(runErr, errTestMemory) || !state.GetGlobal("reached").IsNil() || deepest >= 200 {
				t.Fatal("native chain consumed deep Go stack before refusal", runErr, payments, deepest)
			}
		})
	}
}

func TestRepeatedNativeCallsReuseStackPeakCredit(t *testing.T) {
	owner := newBudget(t, 1<<20)
	state := vm.New(vm.WithBudget(owner))
	defer state.Close(context.Background())
	function := vm.NewNativeFunc(func(*vm.VM) int { return 0 })
	_, err := state.ProtectedCall(function, nil)
	if err != nil {
		t.Fatal(err)
	}
	paid := owner.UsedBytes()
	for range 1000 {
		if _, err := state.ProtectedCall(function, nil); err != nil {
			t.Fatal(err)
		}
	}
	if owner.UsedBytes() != paid {
		t.Fatal("repeated fixed-depth native calls consumed new stack credit", paid, owner.UsedBytes())
	}
}
