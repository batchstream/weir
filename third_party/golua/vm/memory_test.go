package vm_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

var errTestMemory = errors.New("test node allocation capacity")

func newBudget(t *testing.T, limit int) *budget.Budget {
	t.Helper()
	options := budget.Options{Context: t.Context(), Reserve: func(total int) error {
		if total > limit {
			return errTestMemory
		}
		return nil
	}}
	return budget.New(options)
}

func runBudgeted(t *testing.T, source string) (state *vm.VM, owner *budget.Budget, err error) {
	t.Helper()
	defer budget.Recover(&err)
	owner = newBudget(t, 2<<20)
	state = vm.New(vm.WithBudget(owner))
	stdlib.Open(state)
	block, err := parser.Parse("budget-test", source)
	if err != nil {
		return state, owner, err
	}
	prototype, err := compiler.Compile("budget-test", block)
	if err != nil {
		return state, owner, err
	}
	_, err = state.Run(prototype)
	return state, owner, err
}

func TestMemoryQuotaGuardsAllReachableStorageFamilies(t *testing.T) {
	sources := []string{
		`local s="x"; for i=1,29 do s=s..s end`,
		`local s="x"; for i=1,29 do s=s..s..s end`,
		`local t={}; for i=1,100000 do t[i]=i end`,
		`local t={}; for i=1,100000 do t[i*2]=i end; t[1]=1`,
		`local t={}; for i=1,100000 do t[tostring(i)]=i end`,
		`local t={}; for i=1,100000 do t[{}]=i end`,
		`local t={}; for i=1,100000 do local n=i; t[i]=function() return n end end`,
		`local s="x"; for i=1,18 do s=s..s end; return string.byte(s,1,#s)`,
		`local s="x"; for i=1,18 do s=s..s end; for i=1,20 do s=string.format("%s%s",s,s) end`,
	}
	for _, source := range sources {
		t.Run(source[:min(35, len(source))], func(t *testing.T) {
			state, owner, err := runBudgeted(t, source)
			if state != nil {
				defer state.Close(context.Background())
			}
			if !errors.Is(err, errTestMemory) || !budget.IsExhausted(err) || owner.UsedBytes() > 2<<20 {
				t.Fatal("allocation escaped the common prepayment", err, owner.UsedBytes())
			}
		})
	}
}

func TestMemoryTerminationSkipsProtectedHandlersAndCloseAllocations(t *testing.T) {
	for _, source := range []string{
		`local ok=pcall(function() local s="x"; for i=1,29 do s=s..s end end); after=true`,
		`xpcall(function() local s="x"; for i=1,29 do s=s..s end end,function() handler=true; return "caught" end); after=true`,
	} {
		state, owner, err := runBudgeted(t, source)
		if !errors.Is(err, errTestMemory) || !state.GetGlobal("after").IsNil() || !state.GetGlobal("handler").IsNil() {
			t.Fatal("Lua caught terminal allocation refusal", err)
		}
		paid := owner.UsedBytes()
		if err := state.Close(context.Background()); err != nil || owner.UsedBytes() != paid {
			t.Fatal("terminal cleanup allocated or masked the cause", err, paid, owner.UsedBytes())
		}
	}
}

func TestOwnedTableUpdatesReuseAllExistingCredit(t *testing.T) {
	owner := newBudget(t, 1<<20)
	state := vm.New(vm.WithBudget(owner))
	defer state.Close(context.Background())
	table := state.NewEmptyTable()
	table.SetString("key", vm.NewInt(1))
	table.SetInt(1, vm.NewInt(1))
	paid := owner.UsedBytes()
	for i := 0; i < 10000; i++ {
		table.SetString("key", vm.NewInt(int64(i)))
		table.SetInt(1, vm.NewInt(int64(i)))
	}
	if owner.UsedBytes() != paid {
		t.Fatal("in-place writes consumed allocation budget", paid, owner.UsedBytes())
	}
}

func TestBudgetCancellationCannotBeHiddenByPcall(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Millisecond)
	defer cancel()
	options := budget.Options{Context: ctx}
	owner := budget.New(options)
	state := vm.New(vm.WithContext(ctx), vm.WithBudget(owner))
	defer state.Close(context.Background())
	stdlib.Open(state)
	block, err := parser.Parse("cancel-test", `pcall(function() while true do end end); after=true`)
	if err != nil {
		t.Fatal(err)
	}
	prototype, err := compiler.Compile("cancel-test", block)
	if err != nil {
		t.Fatal(err)
	}
	func() {
		defer budget.Recover(&err)
		_, err = state.Run(prototype)
	}()
	if !errors.Is(err, context.DeadlineExceeded) || !state.GetGlobal("after").IsNil() {
		t.Fatal("cancellation lost its terminal cause", err)
	}
}
