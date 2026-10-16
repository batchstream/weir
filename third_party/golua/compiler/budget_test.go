package compiler

import (
	"context"
	"errors"
	"fmt"
	"hash/maphash"
	"reflect"
	"strings"
	"testing"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/parser"
)

func TestBudgetCompilerPreservesBytecodeAndSmallAllocation(t *testing.T) {
	source := "return function(current, incoming) local s='x'; for i=1,29 do s=s..s end; return weir.keep() end"
	options := budget.Options{}
	memory := budget.New(options)
	block, err := parser.ParseWithBudget("same.lua", source, memory)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := Compile("same.lua", block)
	if err != nil {
		t.Fatal(err)
	}
	owned, err := Compile("same.lua", block, WithBudget(memory))
	if err != nil || !reflect.DeepEqual(plain, owned) {
		t.Fatalf("bytecode changed: %v", err)
	}
	if memory.UsedBytes() > 1<<20 {
		t.Fatalf("small source overcharged: %d", memory.UsedBytes())
	}
}

func TestBudgetCompilerRejectsWideConstantsAndDeepAST(t *testing.T) {
	var fields strings.Builder
	fields.WriteString("return {")
	for index := 0; index < 10000; index++ {
		fmt.Fprintf(&fields, "'value-%d',", index)
	}
	fields.WriteString("}")
	wide := fields.String()
	deep := "return " + strings.Repeat("1+", 4096) + "1"
	refused := errors.New("test Node allocation refusal")
	for _, source := range []string{wide, deep} {
		block, err := parser.Parse("large.lua", source)
		if err != nil {
			t.Fatal(err)
		}
		accepted := 0
		options := budget.Options{Reserve: func(next int) error {
			if next > 1<<20 {
				return refused
			}
			accepted = next
			return nil
		}}
		memory := budget.New(options)
		result, err := Compile("large.lua", block, WithBudget(memory))
		if result != nil || !errors.Is(err, refused) || memory.Err() != err || accepted > 1<<20 {
			t.Fatalf("Proto=%v error=%v accepted=%d", result != nil, err, accepted)
		}
	}
}

func TestBudgetCompilerKeepsDeepIndexChainAndRegisterRules(t *testing.T) {
	options := budget.Options{}
	memory := budget.New(options)
	source := "return obj" + strings.Repeat(".field", 500)
	block, err := parser.ParseWithBudget("chain.lua", source, memory)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Compile("chain.lua", block, WithBudget(memory)); err != nil {
		t.Fatal(err)
	}
	if memory.UsedBytes() > 16<<20 {
		t.Fatalf("chain allocation credit is unexpectedly large: %d", memory.UsedBytes())
	}
}

func TestBudgetCompilerCallerCancellationIsSticky(t *testing.T) {
	block, err := parser.Parse("cancel.lua", "return 1+2+3")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := budget.Options{Context: ctx, Reserve: func(next int) error {
		if next > 64<<10 {
			cancel()
		}
		return nil
	}}
	memory := budget.New(options)
	result, err := Compile("cancel.lua", block, WithBudget(memory))
	if result != nil || !errors.Is(err, context.Canceled) || memory.Err() != err {
		t.Fatalf("Proto=%v error=%v", result != nil, err)
	}
}

func TestBudgetCompilerMapCollisionsGrowthAndUpdates(t *testing.T) {
	options := budget.Options{}
	memory := budget.New(options)
	index := newCompilerMap[int, int](memory)
	var keys []int
	for candidate := 0; len(keys) < 80; candidate++ {
		if maphash.Comparable(index.seed, candidate)&15 == 0 {
			keys = append(keys, candidate)
		}
	}
	for ordinal, key := range keys {
		index.set(key, ordinal)
	}
	index.set(0, 123)
	before := memory.UsedBytes()
	index.set(0, 456)
	if memory.UsedBytes() != before {
		t.Fatal("updating existing storage consumed new allocation credit")
	}
	for ordinal, key := range keys {
		expected := ordinal
		if key == 0 {
			expected = 456
		}
		if actual, exists := index.get(key); !exists || actual != expected {
			t.Fatalf("collision lookup key=%d value=%d exists=%t", key, actual, exists)
		}
	}
	if actual, exists := index.get(0); !exists || actual != 456 {
		t.Fatal("zero key was not retained")
	}
	if _, exists := index.get(-1); exists {
		t.Fatal("absent key was found")
	}
}
