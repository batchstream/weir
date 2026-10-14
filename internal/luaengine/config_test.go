package luaengine

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
)

func TestDefaultExecutionHasNoFormerInstructionAndValueCaps(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	payload := value.Value{Kind: value.String, Text: strings.Repeat("x", 300<<10)}
	field := value.Field{Name: "payload", Value: payload}
	input := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	program := Program{Input: input, Source: `return function(_, incoming) local n = 0; for i=1,1100000 do n=n+1 end; return {payload=incoming.payload,n=n} end`}
	result, err := Evaluate(ctx, program)
	if err != nil {
		t.Fatal("default execution retained an old cap", err)
	}
	n, err := result.Value.Lookup("n")
	if err != nil || n.Integer != 1100000 {
		t.Fatal(result, err)
	}
	limits := DefaultLimits()
	limits.MaxInstructions = 100
	program.Limits = &limits
	if _, err := Evaluate(ctx, program); err == nil {
		t.Fatal("configured instruction budget did not apply")
	}
}

func TestDeepValuesAndLargeArraysHaveNoAdmissionCaps(t *testing.T) {
	program := Program{Source: `return function() local a={}; for i=1,10000 do a[i]=i end; table.sort(a); local nested={}; for i=1,5000 do nested={x=nested} end; return {items=a,nested=nested} end`}
	if _, err := Evaluate(t.Context(), program); err != nil {
		t.Fatal("former data depth or node admission remained", err)
	}
}

func TestDeepCurrentAndInputRoundTripWithoutRecursiveConversion(t *testing.T) {
	current := value.Value{Kind: value.Int32, Integer: 7}
	for depth := 0; depth < 5000; depth++ {
		field := value.Field{Name: "nested", Value: current}
		current = value.Value{Kind: value.Object, Fields: []value.Field{field}}
	}
	program := Program{Current: current, Input: current,
		Source: `return function(current, incoming) return {current=current,incoming=incoming} end`,
	}
	result, err := Evaluate(t.Context(), program)
	if err != nil {
		t.Fatal("deep input conversion failed", err)
	}
	for _, name := range []string{"current", "incoming"} {
		item, err := result.Value.Lookup(name)
		if err != nil {
			t.Fatal(err)
		}
		for depth := 0; depth < 5000; depth++ {
			if item.Kind != value.Object || len(item.Fields) != 1 || item.Fields[0].Name != "nested" {
				t.Fatal("deep container changed", name, depth)
			}
			item = item.Fields[0].Value
		}
		if item.Kind != value.Int32 || item.Integer != 7 {
			t.Fatal("deep scalar lost its integer width", name, item)
		}
	}
}
