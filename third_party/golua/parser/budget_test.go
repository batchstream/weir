package parser

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/iceisfun/golua/budget"
)

func TestBudgetParserPreservesLanguageAndPositions(t *testing.T) {
	cases := []string{
		"return function(current, incoming) local s='x'; for i=1,29 do s=s..s end; return weir.keep() end",
		"local a<const> =3; local function f(x,...) return {a=a,x=x,v={...}} end; return f(1,2)",
		"for k,v in pairs({a=1}) do if v>0 then goto done end end; ::done:: return nil",
		"return {['\\x41']='\\u{1F600}', nested={true,false,nil}, value=0x1.8p2}",
	}
	for _, source := range cases {
		plain, err := Parse("same.lua", source)
		if err != nil {
			t.Fatal(err)
		}
		options := budget.Options{}
		memory := budget.New(options)
		owned, err := ParseWithBudget("same.lua", source, memory)
		if err != nil || !reflect.DeepEqual(plain, owned) {
			t.Fatalf("AST changed for %q: %v", source, err)
		}
		if memory.UsedBytes() > 512<<10 {
			t.Fatalf("small source overcharged: %d", memory.UsedBytes())
		}
	}
}

func TestBudgetParserBoundsWideASTAndPreservesRefusal(t *testing.T) {
	refused := errors.New("test Node allocation refusal")
	cases := []string{
		strings.Repeat(";", 20000),
		"return {" + strings.Repeat("'x',", 10000) + "}",
	}
	for _, source := range cases {
		accepted := 0
		options := budget.Options{Reserve: func(next int) error {
			if next > 128<<10 {
				return refused
			}
			accepted = next
			return nil
		}}
		memory := budget.New(options)
		result, err := ParseWithBudget("wide.lua", source, memory)
		if result != nil || !errors.Is(err, refused) || memory.Err() != err || accepted > 128<<10 {
			t.Fatalf("AST=%v error=%v accepted=%d", result != nil, err, accepted)
		}
	}
}

func TestBudgetParserKeepsNativeSyntaxDepthLimit(t *testing.T) {
	options := budget.Options{}
	memory := budget.New(options)
	source := "return " + strings.Repeat("(", 250) + "1" + strings.Repeat(")", 250)
	_, err := ParseWithBudget("deep.lua", source, memory)
	if err == nil || !strings.Contains(err.Error(), "C stack overflow") || memory.Err() != nil {
		t.Fatalf("native syntax limit changed: %v", err)
	}
}

func TestBudgetParserGuardsBareTableCallFields(t *testing.T) {
	// The NAME field shortcut must count the same subexpression entry as a
	// normal list field. Otherwise f{f{...}} can recurse without incDepth.
	source := "return " + strings.Repeat("f{", 250) + "1" + strings.Repeat("}", 250)
	if _, err := Parse("bare-table.lua", source); err == nil || !strings.Contains(err.Error(), "C stack overflow") {
		t.Fatalf("unguarded NAME list field recursion: %v", err)
	}

	refused := errors.New("test Node allocation refusal")
	accepted := 0
	options := budget.Options{Reserve: func(next int) error {
		if next > 160<<10 {
			return refused
		}
		accepted = next
		return nil
	}}
	memory := budget.New(options)
	result, err := ParseWithBudget("bare-table.lua", source, memory)
	if result != nil || !errors.Is(err, refused) || memory.Err() != err || accepted > 160<<10 {
		t.Fatalf("AST=%v error=%v accepted=%d", result != nil, err, accepted)
	}
}

func TestBudgetParserKeepsOrdinaryTableNestingDepth(t *testing.T) {
	// Adding a depth check to every table constructor would count these
	// expressions twice and reject a source inside Lua's existing limit.
	source := "return " + strings.Repeat("{a=", 150) + "1" + strings.Repeat("}", 150)
	plain, err := Parse("record-table.lua", source)
	if err != nil {
		t.Fatal(err)
	}
	options := budget.Options{}
	memory := budget.New(options)
	owned, err := ParseWithBudget("record-table.lua", source, memory)
	if err != nil || !reflect.DeepEqual(plain, owned) {
		t.Fatalf("ordinary table depth changed: %v", err)
	}
}

func TestBudgetParserCallerCancellationWinsOverSyntaxError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	options := budget.Options{Context: ctx, Reserve: func(next int) error {
		if next > 1000 {
			cancel()
		}
		return nil
	}}
	memory := budget.New(options)
	result, err := ParseWithBudget("canceled.lua", "return {'unfinished", memory)
	if result != nil || !errors.Is(err, context.Canceled) || memory.Err() != err {
		t.Fatalf("AST=%v error=%v", result != nil, err)
	}
}
