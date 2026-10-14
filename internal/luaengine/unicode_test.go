package luaengine

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/vm"
)

func TestUnicodeCasingMatchesGoSimpleCasing(t *testing.T) {
	texts := []string{"", "ASCII 123", "straße café", "Μικτό σς", "İı I i", "ȺȿⱾⱥ", "кириллица", "中文 👩🏽‍💻", "e\u0301", "a\x00é"}
	for _, text := range texts {
		input := value.Value{Kind: value.String, Text: text}
		program := Program{Source: `return function(_, incoming) return {upper=utf8.upper(incoming), lower=utf8.lower(incoming), ascii=string.upper(incoming)} end`, Input: input}
		result, err := Evaluate(t.Context(), program)
		if err != nil {
			t.Fatal(text, err)
		}
		upper, _ := result.Value.Lookup("upper")
		lower, _ := result.Value.Lookup("lower")
		if upper.Text != strings.ToUpper(text) || lower.Text != strings.ToLower(text) {
			t.Fatal("Unicode casing changed Go business semantics", text, upper.Text, lower.Text)
		}
	}
}

func TestUnicodeCasingValidatesInputAndPreservesExpansion(t *testing.T) {
	for _, call := range []string{`utf8.upper()`, `utf8.lower(1)`, `utf8.upper("a", "b")`, `utf8.upper(string.char(255))`} {
		program := Program{Source: `return function() ` + call + `; return weir.keep() end`}
		if _, err := Evaluate(t.Context(), program); err == nil {
			t.Fatal("invalid Unicode input accepted", call)
		}
	}
	program := Program{Source: `return function()
		local input = "ȺȺȺȺȺȺȺȺ"
		assert(utf8.lower(input) == "ⱥⱥⱥⱥⱥⱥⱥⱥ")
		assert(utf8.upper("café") == "CAFÉ")
		return weir.keep()
	end`}
	if result, err := Evaluate(t.Context(), program); err != nil || result.Action != Keep {
		t.Fatal("Unicode expansion changed valid string data", result, err)
	}
}

func TestUnicodeNativeCallHonorsVMExecutionBudget(t *testing.T) {
	limits := vm.Limits{MaxInstructions: 3}
	state := vm.New(vm.WithContext(t.Context()), vm.WithLimits(limits))
	defer state.Close(context.Background())
	function := vm.NewNativeFunc(func(state *vm.VM) int { return luaUnicodeCase(state, true) })
	arguments := []vm.Value{vm.NewString(strings.Repeat("café", 9000))}
	if _, err := state.ProtectedCall(function, arguments); err == nil || !strings.Contains(err.Error(), "instruction limit") {
		t.Fatal("long native Unicode conversion skipped VM checkpoints", err)
	}
}

func TestUnicodeLibraryIsIndependentAcrossConcurrentEvaluations(t *testing.T) {
	program := Program{Source: `return function()
		assert(utf8.custom == nil)
		assert(utf8.upper("café") == "CAFÉ" and utf8.lower("Σ") == "σ")
		utf8.custom = true
		utf8.upper = nil
		return weir.keep()
	end`}
	var calls sync.WaitGroup
	for range 32 {
		calls.Go(func() {
			if _, err := Evaluate(t.Context(), program); err != nil {
				t.Error(err)
			}
		})
	}
	calls.Wait()
}
