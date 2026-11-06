package luaengine

import (
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
)

func TestProgramCacheReusesCompiledPrototype(t *testing.T) {
	cache := newProgramCache(maxCachedPrograms, maxCachedBytes)
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
	cache := newProgramCache(maxCachedPrograms, maxCachedBytes)
	for range 2 {
		if _, err := cache.compile("return function("); err == nil {
			t.Fatal("invalid source compiled")
		}
	}
	if len(cache.programs) != 0 || cache.bytes != 0 {
		t.Fatalf("compile error was cached: programs=%d bytes=%d", len(cache.programs), cache.bytes)
	}
}

func TestProgramCacheBoundsProgramsAndBytes(t *testing.T) {
	cache := newProgramCache(3, maxCachedBytes)
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
	first := "return function() return {n = 0} end"
	if _, ok := cache.programs[first]; ok {
		t.Fatal("first program survived seven later insertions into three slots")
	}
	cost := cache.programs["return function() return {n = 9} end"].bytes
	cache = newProgramCache(maxCachedPrograms, 2*cost+cost/2)
	for index := range 5 {
		source := fmt.Sprintf("return function() return {n = %d} end", index)
		if _, err := cache.compile(source); err != nil {
			t.Fatal(err)
		}
		checkProgramCacheBounds(t, cache)
	}
	if len(cache.programs) != 2 {
		t.Fatalf("cache holds %d programs, want the 2 that fit its bytes", len(cache.programs))
	}
	oversized := "return function() return {" + strings.Repeat("function() end, ", 50) + "} end"
	prototype, err := cache.compile(oversized)
	if err != nil || prototype == nil {
		t.Fatal("oversized program did not compile", err)
	}
	if _, ok := cache.programs[oversized]; ok || len(cache.programs) != 2 {
		t.Fatal("program larger than the byte budget was cached or evicted others")
	}
}

func checkProgramCacheBounds(t *testing.T, cache *programCache) {
	t.Helper()
	total := 0
	for source, entry := range cache.programs {
		if entry.bytes != len(source)+prototypeBytes(entry.prototype) {
			t.Fatal("cached program size changed")
		}
		total += entry.bytes
	}
	if len(cache.programs) > cache.maxPrograms || total != cache.bytes || total > cache.maxBytes {
		t.Fatalf("cache bounds broken: programs=%d bytes=%d counted=%d", len(cache.programs), total, cache.bytes)
	}
}

// The estimate must follow the heap a prototype actually retains, including
// code whose prototypes are many times larger than its source.
func TestPrototypeBytesTracksRetainedHeap(t *testing.T) {
	var names []string
	for index := range 50 {
		names = append(names, fmt.Sprintf("v%d", index))
	}
	captures := "function() return " + strings.Join(names, "+") + " end, "
	sources := map[string]string{
		"catalog product": catalogCommonSource + catalogProductSource,
		"tiny closures":   "local t = {" + strings.Repeat("function() end, ", 20000) + "}\nreturn function() return {} end",
		"upvalues":        "local " + strings.Join(names, ", ") + " = 1\nlocal t = {" + strings.Repeat(captures, 1000) + "}\nreturn function() return {} end",
	}
	for name, source := range sources {
		cache := newProgramCache(maxCachedPrograms, 1<<30)
		before := retainedHeap()
		prototype, err := cache.compile(source)
		if err != nil {
			t.Fatal(name, err)
		}
		retained := retainedHeap() - before
		estimate := prototypeBytes(prototype)
		ratio := float64(retained) / float64(estimate)
		t.Logf("%s: source=%d estimate=%d retained=%d ratio=%.2f", name, len(source), estimate, retained, ratio)
		if ratio < 0.75 || ratio > 1.5 {
			t.Errorf("%s: retained heap is %.2f times the estimate", name, ratio)
		}
		runtime.KeepAlive(cache)
	}
}

func retainedHeap() int64 {
	runtime.GC()
	runtime.GC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapAlloc)
}

func TestEvaluateCachesCompiledSource(t *testing.T) {
	source := fmt.Sprintf("return function() return {test = %q} end", t.Name())
	program := Program{Source: source, ObservedAt: time.Unix(1, 0)}
	if _, err := Evaluate(t.Context(), program); err != nil {
		t.Fatal(err)
	}
	compiledPrograms.mu.RLock()
	_, ok := compiledPrograms.programs[source]
	compiledPrograms.mu.RUnlock()
	if !ok {
		t.Fatal("Evaluate did not cache the compiled source")
	}
}

// Evaluation runs the prototype cached for its source rather than compiling
// the source again.
func TestEvaluateRunsCachedPrototype(t *testing.T) {
	cache := newProgramCache(maxCachedPrograms, maxCachedBytes)
	substitute, err := cache.compile("return function() return {source = 'cached'} end")
	if err != nil {
		t.Fatal(err)
	}
	source := "return function() return {source = 'compiled'} end"
	cache.store(source, substitute)
	program := Program{Source: source, ObservedAt: time.Unix(1, 0)}
	result, err := evaluate(t.Context(), cache, program)
	if err != nil {
		t.Fatal(err)
	}
	got, err := result.Value.Lookup("source")
	if err != nil || got.Text != "cached" {
		t.Fatalf("evaluation compiled its source instead of using the cache: %#v (%v)", got, err)
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
	cache := newProgramCache(maxCachedPrograms, maxCachedBytes)
	shared, err := cache.compile(source)
	if err != nil {
		t.Fatal(err)
	}
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
			result, err := evaluate(t.Context(), cache, program)
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
	if cache.programs[source].prototype != shared || len(cache.programs) != 1 {
		t.Fatal("evaluations did not share the cached prototype")
	}
}

// Concurrent evaluations of one large cached transform match a sequential
// evaluation of the same input.
func TestEvaluateSharedCatalogProgramConcurrently(t *testing.T) {
	cache := newProgramCache(maxCachedPrograms, maxCachedBytes)
	product := catalogCommonSource + catalogProductSource
	offer := catalogCommonSource + catalogOfferSource
	current := bridgeDecodeJSON(t, `{"uid":"old","uids":["old","shared"],"title":"keep","brand":"café","first_found_at":"first","comment_count":12,"offers":[{"uid":"seller","id":"s","name":"old"}]}`)
	var programs []Program
	var expected []string
	for worker := range 8 {
		document := map[string]any{"uid": fmt.Sprintf("new-%d", worker), "uids": []string{"shared"}, "title": fmt.Sprintf("title %d", worker), "brand": "straße", "comment_count": worker}
		incoming := bridgeDecodeJSON(t, catalogEnvelope(t, document))
		source := product
		if worker%2 == 1 {
			source = offer
		}
		program := Program{Source: source, Current: current, Input: incoming, ObservedAt: time.Unix(1, 0)}
		baseline, err := evaluate(t.Context(), cache, program)
		if err != nil {
			t.Fatal(err)
		}
		encoded, err := value.EncodeJSON(t.Context(), baseline.Value)
		if err != nil {
			t.Fatal(err)
		}
		programs = append(programs, program)
		expected = append(expected, string(encoded))
	}
	var evaluations sync.WaitGroup
	for worker := range 32 {
		evaluations.Go(func() {
			for round := range 20 {
				index := (worker + round) % len(programs)
				result, err := evaluate(t.Context(), cache, programs[index])
				if err != nil {
					t.Error(err)
					return
				}
				encoded, err := value.EncodeJSON(t.Context(), result.Value)
				if err != nil || string(encoded) != expected[index] {
					t.Errorf("program %d diverged under concurrency: %s (%v)", index, encoded, err)
					return
				}
			}
		})
	}
	evaluations.Wait()
	if len(cache.programs) != 2 {
		t.Fatalf("cache holds %d programs, want the two catalog sources", len(cache.programs))
	}
}

// Concurrent evaluations of more distinct sources than the cache holds keep
// returning each source's own result while entries are evicted.
func TestEvaluateConcurrentlyWithEviction(t *testing.T) {
	cache := newProgramCache(4, maxCachedBytes)
	var evaluations sync.WaitGroup
	for worker := range 16 {
		evaluations.Go(func() {
			for round := range 50 {
				variant := (worker + round) % 12
				source := fmt.Sprintf("return function() return {variant = %d} end", variant)
				program := Program{Source: source, ObservedAt: time.Unix(1, 0)}
				result, err := evaluate(t.Context(), cache, program)
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
	checkProgramCacheBounds(t, cache)
}
