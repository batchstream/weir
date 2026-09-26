// This candidate is deliberately test-only: allocation/compile/helper isolation fails.
package luaprobe

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/mongostore"
	"github.com/batchstream/weir/internal/value"
	lua "github.com/yuin/gopher-lua"
	"go.mongodb.org/mongo-driver/v2/bson"
)

func push(L *lua.LState, v value.Value) { u := L.NewUserData(); u.Value = v; L.Push(u) }
func typed(L *lua.LState, n int) value.Value {
	u := L.CheckUserData(n)
	v, ok := u.Value.(value.Value)
	if !ok {
		L.RaiseError("typed value required")
	}
	return v
}
func newCandidate(ctx context.Context) *lua.LState {
	options := lua.Options{SkipOpenLibs: true, CallStackSize: 128, RegistrySize: 128, RegistryMaxSize: 4096, RegistryGrowStep: 128}
	L := lua.NewState(options)
	L.SetContext(ctx)
	L.SetGlobal("i32", L.NewFunction(func(L *lua.LState) int {
		n, err := strconv.ParseInt(L.CheckString(1), 10, 32)
		if err != nil {
			L.RaiseError("int32 overflow")
		}
		v, _ := value.Integer(value.Int32, n)
		push(L, v)
		return 1
	}))
	L.SetGlobal("i64", L.NewFunction(func(L *lua.LState) int {
		n, err := strconv.ParseInt(L.CheckString(1), 10, 64)
		if err != nil {
			L.RaiseError("int64 overflow")
		}
		v, _ := value.Integer(value.Int64, n)
		push(L, v)
		return 1
	}))
	L.SetGlobal("add", L.NewFunction(func(L *lua.LState) int {
		a, b := typed(L, 1), typed(L, 2)
		v, err := value.Arithmetic(a, b, '+')
		if err != nil {
			L.RaiseError("%v", err)
		}
		push(L, v)
		return 1
	}))
	L.SetGlobal("to32", L.NewFunction(func(L *lua.LState) int {
		v, err := value.Convert(typed(L, 1), value.Int32)
		if err != nil {
			L.RaiseError("%v", err)
		}
		push(L, v)
		return 1
	}))
	L.SetGlobal("get", L.NewFunction(func(L *lua.LState) int {
		v, err := typed(L, 1).Lookup(L.CheckString(2))
		if err != nil {
			L.RaiseError("%v", err)
		}
		push(L, v)
		return 1
	}))
	L.SetGlobal("set", L.NewFunction(func(L *lua.LState) int {
		v := typed(L, 1)
		name := L.CheckString(2)
		child := typed(L, 3)
		if _, err := v.Lookup(name); err != nil {
			L.RaiseError("%v", err)
		}
		for i := range v.Fields {
			if v.Fields[i].Name == name {
				v.Fields[i].Value = child
				push(L, v)
				return 1
			}
		}
		L.RaiseError("unknown field")
		return 0
	}))
	return L
}
func TestExactArithmeticCodecAndDeterminism(t *testing.T) {
	scripts := []struct {
		script string
		want   int64
		fail   bool
	}{
		{`return add(i64("9007199254740993"), i64("1"))`, 9007199254740994, false},
		{`return add(i32("2147483647"),i32("1"))`, 0, true},
		{`return add(i64("9223372036854775807"),i64("1"))`, 0, true},
		{`return add(i32("1"),i64("1"))`, 0, true},
		{`return add(i32("1"),to32(i64("2")))`, 3, false},
	}
	for _, c := range scripts {
		L := newCandidate(context.Background())
		err := L.DoString(c.script)
		if (err != nil) != c.fail {
			t.Fatal(c.script, err)
		}
		if err == nil && typedResult(L).Integer != c.want {
			t.Fatal("precision")
		}
		L.Close()
	}
	decimal, err := bson.ParseDecimal128("123456789.0123456789")
	if err != nil {
		t.Fatal(err)
	}
	binary := bson.Binary{Subtype: 0x80, Data: []byte{0, 1, 255}}
	rawDoc := bson.D{{Key: "_id", Value: bson.NewObjectID()}, {Key: "n", Value: int32(0)}, {Key: "wide", Value: int64(1)}, {Key: "null", Value: nil}, {Key: "decimal", Value: decimal}, {Key: "binary", Value: binary}}
	raw, _ := bson.Marshal(rawDoc)
	var expected string
	for i := 0; i < 20; i++ {
		decoded, err := mongostore.Decode(raw)
		if err != nil {
			t.Fatal(err)
		}
		L := newCandidate(context.Background())
		push(L, decoded)
		L.SetGlobal("current", L.Get(-1))
		L.Pop(1)
		if err := L.DoString(`return get(current,"absent")`); err != nil || typedResult(L).Kind != value.Missing {
			t.Fatal("runtime missing", err)
		}
		L.Pop(1)
		if err := L.DoString(`return get(current,"null")`); err != nil || typedResult(L).Kind != value.Null {
			t.Fatal("runtime null", err)
		}
		L.Pop(1)
		err = L.DoString(`return set(current,"n",add(get(current,"n"),i32("1")))`)
		if err != nil {
			t.Fatal(err)
		}
		out, err := mongostore.Encode(typedResult(L))
		L.Close()
		if err != nil {
			t.Fatal(err)
		}
		if out.Lookup("n").Type != bson.TypeInt32 || out.Lookup("wide").Type != bson.TypeInt64 {
			t.Fatal("width changed")
		}
		if i == 0 {
			expected = string(out)
		} else if string(out) != expected {
			t.Fatal("nondeterministic")
		}
	}
}
func typedResult(L *lua.LState) value.Value { return L.Get(-1).(*lua.LUserData).Value.(value.Value) }
func TestCandidateLoopCancellationAndStack(t *testing.T) {
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
		L := newCandidate(ctx)
		start := time.Now()
		err := L.DoString(`while true do end`)
		L.Close()
		cancel()
		if err == nil || time.Since(start) > time.Second {
			t.Fatal("loop did not stop", err)
		}
	}
	L := newCandidate(context.Background())
	defer L.Close()
	if err := L.DoString(`function f() return 1+f() end; return f()`); err == nil {
		t.Fatal("unbounded recursion")
	}
}
func TestCandidateIsolationGapsAreReleaseBlocking(t *testing.T) {
	// Bounded probes demonstrate missing controls without risking an OOM or endless helper.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	L := newCandidate(ctx)
	defer L.Close()
	source := strings.Repeat("local x = 1\n", 100)
	start := time.Now()
	_, err := L.LoadString(source)
	if err != nil {
		t.Fatalf("expected proof that compile ignores cancelled context: %v", err)
	}
	t.Logf("UNAPPROVED: compile returned success with cancelled context (%s)", time.Since(start))
	alloc := newCandidate(context.Background())
	defer alloc.Close()
	lua.OpenString(alloc)
	err = alloc.DoString(`return string.rep("x", 8*1024*1024)`)
	if err != nil || len(alloc.Get(-1).String()) != 8<<20 {
		t.Fatal("allocation probe", err)
	}
	t.Log("UNAPPROVED: 8 MiB standard helper allocation exceeds a 1 MiB invocation target; no per-state allocation hook")
	helperCtx, stop := context.WithTimeout(context.Background(), time.Millisecond)
	defer stop()
	helper := newCandidate(helperCtx)
	defer helper.Close()
	helper.SetGlobal("host", helper.NewFunction(func(L *lua.LState) int { time.Sleep(40 * time.Millisecond); return 0 }))
	start = time.Now()
	_ = helper.DoString(`host(); return 1`)
	if time.Since(start) < 35*time.Millisecond {
		t.Fatal("host probe did not run")
	}
	t.Log(fmt.Sprintf("UNAPPROVED: host helper outlived 1ms cancellation (%s); context only checks VM dispatch", time.Since(start)))
}
