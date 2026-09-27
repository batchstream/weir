// Package goluaprobe records bounded counterexamples, not an approved runtime.
// No production package imports this test-only experiment.
package goluaprobe

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"

	rt "github.com/arnodel/golua/runtime"
)

// Frozen before running probes. These are attempted logical limits, not claims
// about Go allocation or RSS. The child fuse protects the experiment only.
const (
	maxSource = 16 << 10
	maxFuel   = 100_000
	maxMemory = 1 << 20
	maxMillis = 50
	childFuse = 5 * time.Second
)

func profile() rt.RuntimeContextDef {
	limits := rt.RuntimeResources{Cpu: maxFuel, Memory: maxMemory, Millis: maxMillis}
	def := rt.RuntimeContextDef{HardLimits: limits, RequiredFlags: rt.ComplyIoSafe}
	return def
}

// Hazardous upstream paths run synchronously in an owned child. Run always Waits,
// including after the fuse kills it. No compiler is abandoned in a goroutine.
func child(t *testing.T) bool {
	t.Helper()
	if os.Getenv("WEIR_GOLUA_PROBE_CHILD") == "1" {
		return true
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), childFuse)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^"+regexp.QuoteMeta(t.Name())+"$", "-test.v", "-test.timeout=4s")
	cmd.Env = append(os.Environ(), "WEIR_GOLUA_PROBE_CHILD=1", "GORACE=atexit_sleep_ms=0")
	cmd.WaitDelay = time.Second
	out, err := cmd.CombinedOutput()
	t.Logf("owned child (joined):\n%s", out)
	if err != nil {
		t.Fatalf("experiment failed (outer termination is NOT runtime safety): %v; deadline=%v", err, ctx.Err())
	}
	return false
}

func runChunk(r *rt.Runtime, source string, env *rt.Table) (rt.Value, error) {
	if len(source) > maxSource {
		return rt.NilValue, errors.New("probe source limit")
	}
	// The caller owns the original string; charge before the source copy.
	r.RequireBytes(len(source))
	closure, err := r.CompileAndLoadLuaChunk("probe", []byte(source), rt.TableValue(env))
	if err != nil {
		return rt.NilValue, err
	}
	return rt.Call1(r.MainThread(), rt.FunctionValue(closure))
}

var errGlobalWrite = errors.New("global writes disabled")

func rejectGlobal(t *rt.Thread, c *rt.GoCont) (rt.Cont, error) {
	return nil, errGlobalWrite
}

func readonlyEnv(r *rt.Runtime, bindings *rt.Table) *rt.Table {
	env, meta := rt.NewTable(), rt.NewTable()
	r.SetEnv(meta, "__index", rt.TableValue(bindings))
	f := r.SetEnvGoFunc(meta, "__newindex", rejectGlobal, 3, false)
	// Audited constant-work helper: no arguments inspected, no IO, fixed error.
	f.SolemnlyDeclareCompliance(rt.ComplyCpuSafe | rt.ComplyMemSafe | rt.ComplyTimeSafe | rt.ComplyIoSafe)
	env.SetMetatable(meta)
	return env
}

func TestMinimalEnvironment(t *testing.T) {
	scripts := []string{
		`return os.execute("unused")`, `return io.open("unused")`, `return require("unused")`,
		`return package.loadlib("unused")`, `return debug.getinfo(1)`, `return math.random()`,
		`return string.format("%p", {})`, `return tostring({})`, `return pairs({})`, `return next({})`,
		`return coroutine.create(function() end)`, `return load("return 1")`, `return dofile("unused")`,
		`return loadfile("unused")`, `return rawset(_ENV, "x", 1)`, `return setmetatable({}, {})`,
		`return getmetatable({})`, `return collectgarbage()`, `return pcall(function() end)`,
		`return _G.x`, `return go.import("os")`, `x = 1`, "\x1bLua\x00",
	}
	for _, source := range scripts {
		r := rt.New(io.Discard)
		env := readonlyEnv(r, rt.NewTable())
		def := profile()
		_, err := r.MainThread().CallContext(def, func() error {
			_, err := runChunk(r, source, env)
			return err
		})
		r.Close(nil)
		if err == nil {
			t.Fatalf("unexpectedly allowed %q", source)
		}
	}
}

func TestReadonlyEnvironmentIsNotALanguageRestriction(t *testing.T) {
	r := rt.New(io.Discard)
	defer r.Close(nil)
	bindings := rt.NewTable()
	env := readonlyEnv(r, bindings)
	def := profile()
	var result rt.Value
	_, err := r.MainThread().CallContext(def, func() error {
		var err error
		result, err = runChunk(r, `_ENV={}; x=1; return x`, env)
		return err
	})
	if err != nil || result != rt.IntValue(1) || !bindings.Get(rt.StringValue("x")).IsNil() {
		t.Fatal("environment rebind counterexample", err)
	}
	t.Log("readonly host bindings survive, but guest can rebind _ENV; no approved immutable-global language profile")
}

func TestVMFuelMemoryAndFreshInvocation(t *testing.T) {
	if !child(t) {
		return
	}
	cases := []struct{ name, source string }{
		{"loop", `while true do end`},
		{"recursion", `local function f() return 1+f() end; return f()`},
		{"tables", `local t={}; for i=1,100000 do t[i]={i} end; return t`},
	}
	for _, tc := range cases {
		var previous uint64
		for i := 0; i < 3; i++ {
			r := rt.New(io.Discard)
			def := profile()
			ctx, err := r.MainThread().CallContext(def, func() error {
				_, err := runChunk(r, tc.source, r.GlobalEnv())
				return err
			})
			r.Close(nil)
			var termination rt.ContextTerminationError
			if !errors.As(err, &termination) || ctx.Status() != rt.StatusKilled {
				t.Fatalf("%s did not terminate: %v", tc.name, err)
			}
			if i > 0 && previous != ctx.UsedResources().Cpu {
				t.Fatal("nondeterministic logical fuel")
			}
			previous = ctx.UsedResources().Cpu
			t.Logf("%s run=%d used=%+v error=%v", tc.name, i, ctx.UsedResources(), err)
			fresh := rt.New(io.Discard)
			_, err = fresh.MainThread().CallContext(def, func() error {
				v, err := runChunk(fresh, `local t={}; for i=1,10 do t[i]=i end; return t[10]`, fresh.GlobalEnv())
				if err == nil && v.AsInt() != 10 {
					return errors.New("fresh result")
				}
				return err
			})
			fresh.Close(nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func TestContextLimitsAndAggregateFuel(t *testing.T) {
	r := rt.New(io.Discard)
	defer r.Close(nil)
	soft := rt.RuntimeResources{Cpu: 2}
	def := rt.RuntimeContextDef{SoftLimits: soft}
	ctx, err := r.MainThread().CallContext(def, func() error {
		r.RequireCPU(3)
		if !r.Due() {
			t.Fatal("soft quota did not become due")
		}
		return nil
	})
	if err != nil || ctx.Status() != rt.StatusDone {
		t.Fatal("soft quota should not kill", err)
	}
	zero := rt.RuntimeContextDef{}
	_, err = r.MainThread().CallContext(zero, func() error { r.RequireCPU(1_000_000); return nil })
	if err != nil {
		t.Fatal("zero means unlimited", err)
	}
	limits := rt.RuntimeResources{Cpu: 100, Millis: maxMillis}
	outer := rt.RuntimeContextDef{HardLimits: limits}
	attempts := 0
	ctx, err = r.MainThread().CallContext(outer, func() error {
		for attempts < 5 {
			attempts++
			// Each child requests unlimited resources; parent remainder wins.
			_, err := r.MainThread().CallContext(zero, func() error { r.RequireCPU(30); return nil })
			if err != nil {
				return err
			}
		}
		return nil
	})
	var termination rt.ContextTerminationError
	if !errors.As(err, &termination) || attempts != 4 || ctx.UsedResources().Cpu != 90 {
		t.Fatalf("aggregate budget: attempts=%d used=%+v err=%v", attempts, ctx.UsedResources(), err)
	}
	t.Logf("aggregate logical charge: attempts=%d used=%+v err=%v", attempts, ctx.UsedResources(), err)
}

func TestFreshRecomputationsShareHostBudget(t *testing.T) {
	// This only demonstrates the remaining-budget bookkeeping, not complete
	// fuel/cancellation isolation or any database conflict behavior.
	caller, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	deadline, _ := caller.Deadline()
	remaining := uint64(100)
	completed := 0
	var lastErr error
	for attempt := 0; attempt < 5; attempt++ {
		if caller.Err() != nil || remaining <= 1 {
			break
		}
		r := rt.New(io.Discard)
		def := profile()
		def.HardLimits.Cpu = remaining
		millis := time.Until(deadline).Milliseconds()
		if millis <= 0 {
			r.Close(nil)
			break
		}
		def.HardLimits.Millis = uint64(millis)
		ctx, err := r.MainThread().CallContext(def, func() error {
			got, err := runChunk(r, `local n=1; if n>0 then return n+1 end`, r.GlobalEnv())
			if err == nil && got.AsInt() != 2 {
				return errors.New("recomputation changed")
			}
			return err
		})
		r.Close(nil)
		used := ctx.UsedResources().Cpu
		if used > remaining {
			t.Fatal("logical fuel exceeded remaining")
		}
		remaining -= used
		lastErr = err
		if err != nil {
			break
		}
		completed++
	}
	if completed == 0 || completed >= 5 || lastErr == nil {
		t.Fatalf("aggregate did not stop recomputations: completed=%d remaining=%d err=%v", completed, remaining, lastErr)
	}
	t.Logf("fresh recomputations: completed=%d remaining=%d original deadline retained; stop=%v", completed, remaining, lastErr)
}

func TestCancelAndOwnerClose(t *testing.T) {
	for i := 0; i < 30; i++ {
		caller, cancel := context.WithCancel(context.Background())
		r := rt.New(io.Discard)
		joined := make(chan struct{})
		go func() { cancel(); close(joined) }()
		// The owner serializes all VM access and Close. The cancelling worker
		// only touches context.Context; unsafe concurrent SetStopLevel is absent.
		if caller.Err() == nil {
			def := profile()
			_, err := r.MainThread().CallContext(def, func() error {
				_, err := runChunk(r, `return 1`, r.GlobalEnv())
				return err
			})
			if err != nil {
				r.Close(nil)
				<-joined
				t.Fatal(err)
			}
		}
		r.Close(nil)
		<-joined
		if caller.Err() != context.Canceled {
			t.Fatal("cancellation not joined")
		}
	}
}
