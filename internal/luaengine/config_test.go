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

func TestConfiguredValueDepthAndNodesReachBridgeAndLibraries(t *testing.T) {
	limits := DefaultLimits()
	limits.Values.MaxDepth = 64
	limits.Values.MaxNodes = 10000
	program := Program{Limits: &limits, Source: `return function() local a={}; for i=1,5000 do a[i]=i end; table.sort(a); local nested={}; for i=1,40 do nested={x=nested} end; return {items=a,nested=nested} end`}
	if _, err := Evaluate(t.Context(), program); err != nil {
		t.Fatal("custom tree budget was ignored", err)
	}
	limits.Values.MaxDepth = 32
	if _, err := Evaluate(t.Context(), program); err == nil {
		t.Fatal("configured depth was ignored")
	}
}
