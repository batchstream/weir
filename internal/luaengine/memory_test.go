package luaengine

import (
	"errors"
	"strings"
	"testing"
	"unsafe"

	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/value"
)

func TestEvaluateMemoryRefusalCannotBeCaughtOrReturnPartialOutput(t *testing.T) {
	sources := []string{
		`return function() local s='x'; for i=1,29 do s=s..s end; return {s=s} end`,
		`return function() local ok=pcall(function() local s='x'; for i=1,29 do s=s..s end end); return weir.keep() end`,
		`return function() xpcall(function() local t={}; for i=1,1000000 do t[i]=i end end,function() return 'caught' end); return weir.keep() end`,
		`return function() local s='x'; for i=1,19 do s=s..s end; return {s=string.format('%s%s',s,s)} end`,
		`return function() local t={x=1}; for i=1,40 do t={a=t,b=t} end; return t end`,
	}
	for _, source := range sources {
		t.Run(source[:min(60, len(source))], func(t *testing.T) {
			last, calls := 0, 0
			reserve := func(bytes int) error {
				calls++
				if bytes < last {
					t.Fatal("evaluation released a live allocation", last, bytes)
				}
				if bytes > 2<<20 {
					return execution.ErrExecutionMemory
				}
				last = bytes
				return nil
			}
			program := Program{Source: source, Reserve: reserve}
			result, err := Evaluate(t.Context(), program)
			if !errors.Is(err, execution.ErrExecutionMemory) || result.Action != "" || result.Value.Kind != value.Missing || result.Allocations != nil {
				t.Fatalf("allocation refusal escaped or produced output: result=%+v err=%v", result, err)
			}
			if calls == 0 || last == 0 || last > 2<<20 {
				t.Fatal("allocation callback was skipped or exceeded its admission", calls, last)
			}
		})
	}
}

func TestEvaluateRetainsOwnedResultAndSharedScope(t *testing.T) {
	message := strings.Repeat("retained", 4096)
	text := value.Value{Kind: value.String, Text: message}
	field := value.Field{Name: "message", Value: text}
	input := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	last := 0
	reserve := func(bytes int) error {
		if bytes < last {
			t.Fatal("live scope was reset", last, bytes)
		}
		last = bytes
		return nil
	}
	program := Program{Source: `return function(_, input) return weir.reject(input.message) end`, Input: input, Reserve: reserve}
	result, err := Evaluate(t.Context(), program)
	if err != nil || result.Action != Reject || result.Message != message || result.Allocations == nil || result.Allocations.UsedBytes() != last {
		t.Fatal("result lost its allocation scope", result.Action, err, last)
	}
	if unsafe.StringData(result.Message) == unsafe.StringData(message) {
		t.Fatal("rejection still borrows the VM/input string backing")
	}
	before := last
	bytes, err := result.ValueBytes(t.Context(), input, true)
	if err != nil || bytes != 64+6*len(field.Name)+6*len(message) || last <= before {
		t.Fatal("post-evaluation encoding estimate lost the original scope", bytes, last, before, err)
	}
}

func TestEvaluateValidationReservesBeforeWideInputMaps(t *testing.T) {
	fields := make([]value.Field, 10000)
	for index := range fields {
		fields[index].Name = "duplicate"
		fields[index].Value.Kind = value.Null
	}
	input := value.Value{Kind: value.Object, Fields: fields}
	reserve := func(bytes int) error {
		if bytes > 32<<10 {
			return execution.ErrExecutionMemory
		}
		return nil
	}
	program := Program{Source: `return function() return weir.keep() end`, Input: input, Reserve: reserve}
	_, err := Evaluate(t.Context(), program)
	if !errors.Is(err, execution.ErrExecutionMemory) {
		t.Fatal("wide validation ran before its allocation admission", err)
	}
}

func TestEvaluateTinyProgramOnlyReservesItsAllocations(t *testing.T) {
	last := 0
	reserve := func(bytes int) error {
		last = bytes
		if bytes > 1<<20 {
			return execution.ErrExecutionMemory
		}
		return nil
	}
	program := Program{Source: `return function() return weir.keep() end`, Reserve: reserve}
	result, err := Evaluate(t.Context(), program)
	if err != nil || result.Action != Keep || last == 0 || last >= 1<<20 {
		t.Fatal("small Lua prepaid a large fixed window", last, err)
	}
}
