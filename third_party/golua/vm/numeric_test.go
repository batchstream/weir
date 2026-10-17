package vm_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/vm"
)

func TestOwnedNumericParsingOnlyChargesAttemptedFallbacks(t *testing.T) {
	owner := newBudget(t, 1<<20)
	state := vm.New(vm.WithBudget(owner))
	defer state.Close(context.Background())
	text := strings.Repeat("0", 4096) + "1"
	paid := owner.UsedBytes()
	integer, ok := state.ToInt(vm.NewString(text))
	if !ok || integer != 1 {
		t.Fatal("integer conversion changed", integer, ok)
	}
	if charged := owner.UsedBytes() - paid; charged < len(text) || charged >= 2*len(text) {
		t.Fatal("successful integer conversion prepaid unused fallbacks", charged)
	}
	paid = owner.UsedBytes()
	integer, ok = state.ToInt(vm.NewInt(1))
	if !ok || integer != 1 || owner.UsedBytes() != paid {
		t.Fatal("numeric Value conversion charged string workspace", integer, ok, paid, owner.UsedBytes())
	}
}

func TestOwnedNumericParsingPreservesStickyCancellation(t *testing.T) {
	for operation := range 3 {
		ctx, cancel := context.WithCancel(t.Context())
		options := budget.Options{Context: ctx}
		owner := budget.New(options)
		state := vm.New(vm.WithBudget(owner))
		paid := owner.UsedBytes()
		text := strings.Repeat("0", 4096) + "1.0"
		cancel()
		var err error
		func() {
			defer budget.Recover(&err)
			switch operation {
			case 0:
				state.ToInt(vm.NewString(text))
			case 1:
				state.ToNumber(vm.NewString(text))
			case 2:
				state.StringToNumericValue(text)
			}
		}()
		if !errors.Is(err, context.Canceled) || !budget.IsExhausted(err) || owner.UsedBytes() != paid {
			t.Fatal("numeric conversion bypassed canceled ownership", operation, err, paid, owner.UsedBytes())
		}
		if err := state.Close(context.Background()); err != nil || owner.UsedBytes() != paid {
			t.Fatal("canceled numeric cleanup allocated", operation, err)
		}
	}
}
