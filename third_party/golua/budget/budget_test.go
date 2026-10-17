package budget

import (
	"context"
	"errors"
	"math"
	"testing"
)

func caught(t *testing.T, work func()) error {
	t.Helper()
	var result error
	func() {
		defer Recover(&result)
		work()
	}()
	if result == nil || !IsExhausted(result) {
		t.Fatalf("missing typed allocation termination: %v", result)
	}
	return result
}

func TestBudgetRefusalIsStickyAndPreservesCause(t *testing.T) {
	refused := errors.New("shared node capacity")
	calls := 0
	limit := math.MaxInt
	options := Options{Reserve: func(total int) error {
		calls++
		if total > limit {
			return refused
		}
		return nil
	}}
	owner := New(options)
	base := owner.UsedBytes()
	limit = base + 100
	calls = 0
	owner.Require(80)
	err := caught(t, func() { owner.Require(21) })
	if !errors.Is(err, refused) || owner.UsedBytes() != base+80 || calls != 2 {
		t.Fatal("refused allocation changed the paid total or lost cause", err, owner.UsedBytes(), calls)
	}
	caught(t, func() { owner.Require(1) })
	if calls != 2 || owner.Err() != err || owner.Check() != err {
		t.Fatal("failed scope resumed or called its allocator again")
	}
	for _, work := range []func(){
		func() { owner.RequireArray(1, -1) },
		func() { owner.RequireAllocation(math.MaxInt) },
	} {
		if later := caught(t, work); later != err || !errors.Is(later, refused) {
			t.Fatal("invalid later size replaced the initial refusal", later)
		}
	}
}

func TestBudgetCancellationAndOverflowTerminateBeforeAllocation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	options := Options{Context: ctx}
	owner := New(options)
	base := owner.UsedBytes()
	cancel()
	err := caught(t, func() { owner.RequireArray(1, 1024) })
	if !errors.Is(err, context.Canceled) || owner.UsedBytes() != base {
		t.Fatal(err, owner.UsedBytes())
	}
	for _, work := range []func(*Budget){
		func(b *Budget) { b.Require(-1) },
		func(b *Budget) { b.Require(math.MaxInt); b.Require(1) },
		func(b *Budget) { b.RequireArray(16, math.MaxInt) },
		func(b *Budget) { b.RequireAllocation(math.MaxInt) },
	} {
		empty := Options{}
		b := New(empty)
		caught(t, func() { work(b) })
	}
}

func TestControlledAppendAndBuilderOnlyChargeNewCapacity(t *testing.T) {
	options := Options{}
	owner := New(options)
	values := Append[int](owner, nil, 1)
	paid := owner.UsedBytes()
	for len(values) < cap(values) {
		values = Append(owner, values, 2)
	}
	if owner.UsedBytes() != paid {
		t.Fatal("existing slice capacity was charged again")
	}
	values = Append(owner, values, values...)
	if owner.UsedBytes() <= paid || len(values) != 16 {
		t.Fatal("new backing array was not prepaid", owner.UsedBytes(), len(values))
	}
	builder := NewBuilder(owner)
	builder.Grow(100)
	paid = owner.UsedBytes()
	builder.WriteString("a")
	original := builder.String()
	builder.WriteRune('é')
	if owner.UsedBytes() != paid || original != "a" || builder.String() != "aé" {
		t.Fatal("builder reused capacity incorrectly", original, builder.String())
	}
	owner.RequireArray(1, 0)
	if owner.UsedBytes() != paid {
		t.Fatal("empty backing storage consumed byte credit")
	}
}

func TestRefusedAppendCannotAllocateOrChangeTheOriginal(t *testing.T) {
	refused := errors.New("no allocation")
	calls := 0
	options := Options{Reserve: func(int) error {
		calls++
		if calls > 1 {
			return refused
		}
		return nil
	}}
	owner := New(options)
	base := owner.UsedBytes()
	original := []byte{1, 2, 3}
	err := caught(t, func() { _ = Append(owner, original[:3:3], byte(4)) })
	if !errors.Is(err, refused) || owner.UsedBytes() != base || string(original) != "\x01\x02\x03" {
		t.Fatal("failed prepayment changed ownership", err, original)
	}
}
