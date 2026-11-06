package luaengine

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
)

func TestProgramCacheReusesCompiledPrototype(t *testing.T) {
	cache := newProgramCache(maxCachedPrograms, maxCachedSourceBytes)
	first, err := cache.compile("return function() return {} end")
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.compile("return function() return {} end")
	if err != nil {
		t.Fatal(err)
	}
	other, err := cache.compile("return function() return {a = 1} end")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first == other {
		t.Fatal("cache did not key prototypes by source text")
	}
}

func TestProgramCacheDoesNotStoreCompileErrors(t *testing.T) {
	cache := newProgramCache(maxCachedPrograms, maxCachedSourceBytes)
	for range 2 {
		if _, err := cache.compile("return function("); err == nil {
			t.Fatal("invalid source compiled")
		}
	}
	if len(cache.programs) != 0 || cache.sourceBytes != 0 {
		t.Fatalf("compile error was cached: programs=%d bytes=%d", len(cache.programs), cache.sourceBytes)
	}
}

func TestProgramCacheBoundsProgramsAndSourceBytes(t *testing.T) {
	cache := newProgramCache(3, 200)
	for index := range 10 {
		source := fmt.Sprintf("return function() return {n = %d} end", index)
		if _, err := cache.compile(source); err != nil {
			t.Fatal(err)
		}
		checkProgramCacheBounds(t, cache)
	}
	if len(cache.programs) != 3 {
		t.Fatalf("cache holds %d programs, want 3", len(cache.programs))
	}
	padded := "return function() return {} end --" + strings.Repeat("x", 120)
	if _, err := cache.compile(padded); err != nil {
		t.Fatal(err)
	}
	checkProgramCacheBounds(t, cache)
	if _, ok := cache.programs[padded]; !ok {
		t.Fatal("source within the byte budget was not cached")
	}
	oversized := "return function() return {} end --" + strings.Repeat("x", 200)
	prototype, err := cache.compile(oversized)
	if err != nil || prototype == nil {
		t.Fatal("oversized source did not compile", err)
	}
	if _, ok := cache.programs[oversized]; ok {
		t.Fatal("source larger than the byte budget was cached")
	}
	if _, ok := cache.programs[padded]; !ok {
		t.Fatal("oversized source evicted cached programs")
	}
}

func checkProgramCacheBounds(t *testing.T, cache *programCache) {
	t.Helper()
	total := 0
	for source := range cache.programs {
		total += len(source)
	}
	if len(cache.programs) > cache.maxPrograms || total != cache.sourceBytes || total > cache.maxBytes {
		t.Fatalf("cache bounds broken: programs=%d bytes=%d counted=%d", len(cache.programs), total, cache.sourceBytes)
	}
}

// A shared prototype must not share state: top-level code, globals and
// upvalues start fresh in every evaluation.
func TestEvaluateCachedProgramRunsTopLevelPerEvaluation(t *testing.T) {
	source := `
		chunks = (chunks or 0) + 1
		local calls = 0
		return function(current, incoming)
			calls = calls + 1
			return {chunks = chunks, calls = calls, n = incoming.n}
		end`
	var evaluations sync.WaitGroup
	for index := range 64 {
		evaluations.Go(func() {
			integer, err := value.Integer(value.Int64, int64(index))
			if err != nil {
				t.Error(err)
				return
			}
			field := value.Field{Name: "n", Value: integer}
			input := value.Value{Kind: value.Object, Fields: []value.Field{field}}
			program := Program{Source: source, Input: input, ObservedAt: time.Unix(1, 0)}
			result, err := Evaluate(t.Context(), program)
			if err != nil {
				t.Error(err)
				return
			}
			for name, want := range map[string]int64{"chunks": 1, "calls": 1, "n": int64(index)} {
				got, err := result.Value.Lookup(name)
				if err != nil || got.Integer != want {
					t.Errorf("evaluation %d: %s = %#v (%v), want %d", index, name, got, err, want)
				}
			}
		})
	}
	evaluations.Wait()
}

// Concurrent evaluations of more distinct sources than the cache holds keep
// returning each source's own result while entries are evicted.
func TestEvaluateConcurrentlyWithEviction(t *testing.T) {
	previous := compiledPrograms
	compiledPrograms = newProgramCache(4, maxCachedSourceBytes)
	t.Cleanup(func() { compiledPrograms = previous })
	var evaluations sync.WaitGroup
	for worker := range 16 {
		evaluations.Go(func() {
			for round := range 50 {
				variant := (worker + round) % 12
				source := fmt.Sprintf("return function() return {variant = %d} end", variant)
				program := Program{Source: source, ObservedAt: time.Unix(1, 0)}
				result, err := Evaluate(t.Context(), program)
				if err != nil {
					t.Error(err)
					return
				}
				got, err := result.Value.Lookup("variant")
				if err != nil || got.Integer != int64(variant) {
					t.Errorf("source %d returned %#v (%v)", variant, got, err)
					return
				}
			}
		})
	}
	evaluations.Wait()
	checkProgramCacheBounds(t, compiledPrograms)
}
