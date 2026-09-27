package goluaprobe

import (
	"context"
	"errors"
	"io"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/arnodel/golua/ast"
	"github.com/arnodel/golua/code"
	"github.com/arnodel/golua/lib/base"
	"github.com/arnodel/golua/lib/stringlib"
	rt "github.com/arnodel/golua/runtime"
)

func TestParseAllocationGap(t *testing.T) {
	if !child(t) {
		return
	}
	// 4 KiB of source, 4 KiB + 1 of claimed memory. AST stays live at measurement.
	source := []byte(strings.Repeat("a=1;", 1024))
	r := rt.New(io.Discard)
	defer r.Close(nil)
	limits := rt.RuntimeResources{Cpu: maxFuel, Memory: uint64(len(source) + 1), Millis: maxMillis}
	def := rt.RuntimeContextDef{HardLimits: limits}
	var tree *ast.BlockStat
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	ctx, err := r.MainThread().CallContext(def, func() error {
		var err error
		tree, _, err = r.ParseLuaChunk("parse", source)
		return err
	})
	runtime.ReadMemStats(&after)
	allocated := after.TotalAlloc - before.TotalAlloc
	if err != nil || tree == nil || allocated <= limits.Memory {
		t.Fatalf("counterexample not reproduced: alloc=%d limit=%d err=%v", allocated, limits.Memory, err)
	}
	// Even just the live Stats backing slice exceeds the hard quota. This
	// lower bound excludes every AST node/token and does not depend on GC.
	retainedMinimum := uint64(len(tree.Stats)) * uint64(unsafe.Sizeof(tree.Stats[0]))
	if retainedMinimum <= limits.Memory {
		t.Fatal("AST structural lower bound")
	}
	runtime.KeepAlive(tree)
	t.Logf("NO-GO parse: source=%d logical=%+v hard=%d live AST minimum=%d Go TotalAlloc delta=%d", len(source), ctx.UsedResources(), limits.Memory, retainedMinimum, allocated)
}

func TestReturnedTableAndClosureAreUncharged(t *testing.T) {
	for _, source := range []string{`return {}`, `return function() return 1 end`} {
		r := rt.New(io.Discard)
		// Separate compilation from the VM-only observation; not a product path.
		closure, err := r.CompileAndLoadLuaChunk("vm", []byte(source), rt.TableValue(r.GlobalEnv()))
		if err != nil {
			r.Close(nil)
			t.Fatal(err)
		}
		def := profile()
		var result rt.Value
		ctx, err := r.MainThread().CallContext(def, func() error {
			var err error
			result, err = rt.Call1(r.MainThread(), rt.FunctionValue(closure))
			return err
		})
		r.Close(nil)
		if err != nil || result.IsNil() || ctx.UsedResources().Memory != 0 {
			t.Fatal("VM allocation counterexample changed", err, ctx.UsedResources())
		}
		t.Logf("NO-GO VM: %s retains result type=%v with logical=%+v", source, result.Type(), ctx.UsedResources())
	}
}

func TestCompileShapesAndCancellationGap(t *testing.T) {
	if !child(t) {
		return
	}
	cases := []struct {
		name, source string
		invalid      bool
	}{
		{"normal", `local a=2; if a>1 then return a+3 end`, false},
		{"tokens", strings.Repeat("a=1;", 4000), false},
		{"constants", `return {` + strings.Repeat(`"abcdef",`, 1500) + `}`, false},
		{"nested", `return ` + strings.Repeat("(", 1024) + "1" + strings.Repeat(")", 1024), false},
		{"literal", `return "` + strings.Repeat("x", 16_000) + `"`, false},
		{"invalid", `return "` + strings.Repeat("x", 16_000), true},
	}
	for _, tc := range cases {
		r := rt.New(io.Discard)
		def := profile()
		caller, cancel := context.WithCancel(context.Background())
		cancel() // No caller-context parameter exists on parse/compile.
		var unit *code.Unit
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		start := time.Now()
		ctx, err := r.MainThread().CallContext(def, func() error {
			r.RequireBytes(len(tc.source))
			var err error
			unit, _, err = r.CompileLuaChunk("compile", []byte(tc.source))
			return err
		})
		elapsed := time.Since(start)
		runtime.ReadMemStats(&after)
		r.Close(nil)
		if (err != nil) != tc.invalid || caller.Err() != context.Canceled {
			t.Fatalf("%s: %v", tc.name, err)
		}
		runtime.KeepAlive(unit)
		t.Logf("NO-GO compile caller ignored: %s source=%d used=%+v Go TotalAlloc=%d elapsed=%s rejected=%t", tc.name, len(tc.source), ctx.UsedResources(), after.TotalAlloc-before.TotalAlloc, elapsed, err != nil)
	}
	// Cancellation races only with context.Context, never with VM fields/Close.
	r := rt.New(io.Discard)
	defer r.Close(nil)
	caller, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancelled := make(chan time.Time, 1)
	go func() { time.Sleep(100 * time.Microsecond); cancel(); cancelled <- time.Now() }()
	def := profile()
	source := []byte(strings.Repeat("a=1;", 4000))
	_, err := r.MainThread().CallContext(def, func() error {
		_, _, err := r.CompileLuaChunk("during", source)
		return err
	})
	end := time.Now()
	when := <-cancelled // Join the sole cancellation worker before Close.
	if err != nil || caller.Err() != context.Canceled {
		t.Fatal(err)
	}
	t.Logf("compile cancellation observation: cancelledBeforeReturn=%t; success despite cancelled caller when true", when.Before(end))
}

func TestTableAllocatesBeforeQuotaCheck(t *testing.T) {
	r := rt.New(io.Discard)
	defer r.Close(nil)
	table := rt.NewTable()
	limits := rt.RuntimeResources{Cpu: maxFuel, Memory: 1}
	def := rt.RuntimeContextDef{HardLimits: limits}
	ctx, err := r.MainThread().CallContext(def, func() error {
		r.SetTable(table, rt.IntValue(1), rt.IntValue(42))
		return nil
	})
	var termination rt.ContextTerminationError
	if !errors.As(err, &termination) || table.Get(rt.IntValue(1)).AsInt() != 42 {
		t.Fatal("expected mutation before quota panic", err)
	}
	t.Logf("NO-GO table: hard memory=1, table[1]=42 already stored; logical=%+v error=%v", ctx.UsedResources(), err)
}

func TestVMConcatAndHostHelperFuelGaps(t *testing.T) {
	if !child(t) {
		return
	}
	for _, operation := range []string{`return x..x`, `return string.rep(x, 2)`, `return string.reverse(x)`} {
		var previous uint64
		for _, size := range []int{8, 128 << 10} {
			r := rt.New(io.Discard)
			// Diagnostic-only: inspect upstream helpers with their OWN compliance
			// declarations. These functions are never part of the minimal env.
			lib, _ := stringlib.LibLoader.Load(r)
			r.SetEnv(r.GlobalEnv(), "string", lib)
			r.SetEnv(r.GlobalEnv(), "x", rt.StringValue(strings.Repeat("x", size)))
			def := profile()
			var result rt.Value
			ctx, err := r.MainThread().CallContext(def, func() error {
				var err error
				result, err = runChunk(r, operation, r.GlobalEnv())
				return err
			})
			r.Close(nil)
			if err != nil {
				t.Fatal(err)
			}
			used := ctx.UsedResources().Cpu
			if previous != 0 && used != previous {
				t.Fatal("fixed-version fuel counterexample changed")
			}
			previous = used
			t.Logf("NO-GO byte work: %s input=%d output=%d fuel=%d", operation, size, len(result.AsString()), used)
		}
	}
}

func TestHostFormatAllocatesPastBudget(t *testing.T) {
	if !child(t) {
		return
	}
	r := rt.New(io.Discard)
	defer r.Close(nil)
	// 512 bounded width specifiers expand to 50,688 bytes. Source and arguments
	// are owned outside the measured helper. Format's output is not precharged.
	format := strings.Repeat("%99d", 512)
	args := make([]rt.Value, 512)
	for i := range args {
		args[i] = rt.IntValue(1)
	}
	limits := rt.RuntimeResources{Cpu: maxFuel, Memory: 32 << 10, Millis: maxMillis}
	def := rt.RuntimeContextDef{HardLimits: limits}
	var output string
	ctx, err := r.MainThread().CallContext(def, func() error {
		var err error
		output, err = stringlib.Format(r.MainThread(), format, args)
		return err
	})
	if err != nil || len(output) != 512*99 || uint64(len(output)) <= limits.Memory {
		t.Fatal("format counterexample", err)
	}
	t.Logf("NO-GO format: hard=%d returned bytes=%d logical=%+v (Lua wrapper charges after Format)", limits.Memory, len(output), ctx.UsedResources())
}

func TestUnreviewedHostRejected(t *testing.T) {
	r := rt.New(io.Discard)
	defer r.Close(nil)
	called := false
	r.SetEnvGoFunc(r.GlobalEnv(), "host", func(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
		called = true
		return c.Next(), nil
	}, 0, false)
	def := profile()
	_, err := r.MainThread().CallContext(def, func() error {
		_, err := runChunk(r, `host()`, r.GlobalEnv())
		return err
	})
	if err == nil || called {
		t.Fatal("unreviewed helper executed", err)
	}
}

func TestMillisPollingAndPcallQuotaResult(t *testing.T) {
	if !child(t) {
		return
	}
	r := rt.New(io.Discard)
	defer r.Close(nil)
	limits := rt.RuntimeResources{Cpu: maxFuel, Memory: maxMemory, Millis: 1}
	def := rt.RuntimeContextDef{HardLimits: limits}
	ctx, err := r.MainThread().CallContext(def, func() error {
		r.RequireCPU(1)                   // Establish the next clock sample at 10,001 ticks.
		time.Sleep(20 * time.Millisecond) // finite host-side work, not a safe helper
		r.RequireCPU(1)
		return nil
	})
	if err != nil {
		t.Fatal("expected no clock poll below threshold", err)
	}
	t.Logf("NO-GO watchdog: 20ms host work returned success under 1ms limit; used=%+v", ctx.UsedResources())
	// Diagnostic-only base loading checks panic propagation; it is NOT the env.
	base.Load(r)
	def = profile()
	var result rt.Value
	ctx, err = r.MainThread().CallContext(def, func() error {
		var err error
		result, err = runChunk(r, `return pcall(function() while true do end end)`, r.GlobalEnv())
		return err
	})
	if err != nil || result != rt.BoolValue(false) || ctx.Status() != rt.StatusDone || ctx.UsedResources().Cpu != maxFuel-1 {
		t.Fatal("fixed-version pcall quota conversion changed", err)
	}
	t.Logf("pcall converts child quota to false with nil outer error; used=%+v. No proof of extra fuel; pcall excluded", ctx.UsedResources())
}
