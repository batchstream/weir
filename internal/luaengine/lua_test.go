package luaengine_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
)

func TestEvaluateBusinessFieldsAndNativeIntegerArithmetic(t *testing.T) {
	program := testProgram(`return function(current, incoming)
		assert(type(current) == "table" and current.large > 9007199254740992.0)
		current.n = current.n + incoming.step
		current.large = current.large + 1
		current.removed = nil
		current.null = weir.null()
		return current
	end`)
	program.Current = ValueObject(
		Field("n", ValueInt32(4)),
		Field("large", ValueInt64(9007199254740993)),
		Field("removed", ValueString("old")),
	)
	program.Input = ValueObject(Field("step", ValueInt32(2)))
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(Field("n", ValueInt32(6)), Field("large", ValueInt64(9007199254740994)), Field("null", ValueNull()))
	if got.Action != luaengine.Replace || !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("unexpected result: %#v", got)
	}
	if program.Current.Fields[0].Value.Integer != 4 || len(program.Current.Fields) != 3 {
		t.Fatal("Lua mutation changed the caller's current document")
	}
}

func TestEvaluateRequiresFunctionAndExplicitResult(t *testing.T) {
	cases := []struct {
		source string
		want   luaengine.Action
	}{
		{source: "return function() return weir.keep() end", want: luaengine.Keep},
		{source: "return function() return weir.delete() end", want: luaengine.Delete},
		{source: "return function() return weir.reject('invalid') end", want: luaengine.Reject},
		{source: "return function() return {n = 1} end", want: luaengine.Replace},
	}
	for _, test := range cases {
		program := testProgram(test.source)
		got, err := luaengine.Evaluate(context.Background(), program)
		if err != nil || got.Action != test.want {
			t.Fatalf("source %q: got %#v, err %v", test.source, got, err)
		}
	}
	invalid := []string{
		"return weir.keep()", "return nil", "local ignored = 1", "return function() end, function() end",
		"return function() end", "return function() return nil end", "return function() return 1 end",
		"return function() return {}, weir.delete() end", "return function() return weir.array() end",
		"return function() return weir.keep(1) end", "return function() return weir.delete(1) end",
		"return function() return weir.reject(1) end", "return function() return weir.time.now(1) end",
		"return function( end", "error('chunk failed')", "return function() error('callback failed') end",
	}
	for _, source := range invalid {
		program := testProgram(source)
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatalf("accepted invalid source/result %q", source)
		}
	}
}

func TestEvaluateMissingDocumentsAreNilCallbackArguments(t *testing.T) {
	for _, missingCurrent := range []bool{false, true} {
		for _, missingInput := range []bool{false, true} {
			for _, action := range []luaengine.Action{luaengine.Keep, luaengine.Delete, luaengine.Reject, luaengine.Replace} {
				source := `return function(current, incoming)
					assert((current == nil) == ` + fmt.Sprint(missingCurrent) + `)
					assert((incoming == nil) == ` + fmt.Sprint(missingInput) + `)
					if current ~= nil then assert(current.n == 1) end
					if incoming ~= nil then assert(weir.kind(incoming) == "object") end
					return `
				if action == luaengine.Replace {
					source += "weir.object()"
				} else if action == luaengine.Reject {
					source += "weir.reject('rejected')"
				} else {
					source += "weir." + string(action) + "()"
				}
				source += " end"
				program := testProgram(source)
				if missingCurrent {
					program.Current = value.Value{}
				}
				if missingInput {
					program.Input = value.Value{}
				}
				got, err := luaengine.Evaluate(context.Background(), program)
				if err != nil || got.Action != action || action == luaengine.Reject && got.Message != "rejected" {
					t.Fatalf("current missing=%t, input missing=%t, action=%s: %#v (%v)", missingCurrent, missingInput, action, got, err)
				}
			}
		}
	}
}

func TestEvaluateLua54LanguageContract(t *testing.T) {
	program := testProgram(`return function()
		assert(_VERSION == "Lua 5.4")
		assert(math.type(7) == "integer" and math.type(7.0) == "float")
		assert(math.maxinteger == 9223372036854775807)
		assert(math.maxinteger + 1 == math.mininteger)
		assert(7 // 2 == 3 and (5 & 3) == 1 and (1 << 4) == 16)
		return weir.keep()
	end`)
	if _, err := luaengine.Evaluate(context.Background(), program); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateRestrictsHostCapabilitiesAndOldInterface(t *testing.T) {
	program := testProgram(`
		assert(current == nil and input == nil)
		return function()
			for _, name in ipairs({"io", "os", "package", "debug", "coroutine", "require", "load",
				"loadfile", "dofile", "loadstring", "collectgarbage", "getfenv", "setfenv", "getmetatable",
				"setmetatable", "rawset", "print", "warn", "time", "exec", "chan", "glob", "_G"}) do
				assert(_ENV[name] == nil, name)
			end
			assert(math.random == nil and math.randomseed == nil)
			assert(string.dump == nil and string.pack == nil and string.rep == nil)
			assert(string.find == nil and string.match == nil and string.gmatch == nil and string.gsub == nil)
			assert(weir.replace == nil and weir.get == nil and weir.set == nil and weir.merge == nil)
			assert(weir.i32 == nil and weir.i64 == nil and weir.add == nil)
			return weir.keep()
		end
	`)
	if _, err := luaengine.Evaluate(context.Background(), program); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateControlledLibraries(t *testing.T) {
	program := testProgram(`return function(current, incoming)
		local numbers = {3, 1, 2}
		table.sort(numbers)
		table.insert(numbers, 4)
		assert(table.remove(numbers, 2) == 2)
		local names = {"b", "c", "a"}
		table.sort(names, function(a, b) return a > b end)
		local sum = 0
		for _, number in ipairs(numbers) do sum = sum + number end
		local fields = 0
		for _ in pairs(current) do fields = fields + 1 end
		return {
			title = string.upper(string.sub(incoming.title, 2, -2)),
		quoted = string.format("%q %% %04d %.2f", "a\"b", 3, 1.25),
			names = table.concat(names, ","),
			sum = sum,
			fields = fields,
			integer = math.floor(math.max(1, 2.9)),
			exact = tostring(9007199254740993),
		}
	end`)
	program.Input = ValueObject(Field("title", ValueString("[hello]")))
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	checks := map[string]string{"title": "HELLO", "names": "c,b,a", "exact": "9007199254740993", "quoted": "\"a\\\"b\" % 0003 1.25"}
	for name, want := range checks {
		field, err := got.Value.Lookup(name)
		if err != nil || field.Text != want {
			t.Fatalf("%s = %#v, expected %q (%v)", name, field, want, err)
		}
	}
	for name, want := range map[string]int64{"sum": 8, "fields": 1, "integer": 2} {
		field, err := got.Value.Lookup(name)
		if err != nil || field.Integer != want {
			t.Fatalf("%s = %#v, expected %d (%v)", name, field, want, err)
		}
	}
}

func TestEvaluateDeadlineCoversChunkCallbackAndProtectedCalls(t *testing.T) {
	sources := []string{
		"while true do end; return function() return weir.keep() end",
		"return function() while true do end end",
		"return function() while true do pcall(function() while true do end end) end end",
		"return function() table.sort({3,2,1}, function() while true do end end); return {} end",
	}
	for _, source := range sources {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		program := testProgram(source)
		started := time.Now()
		_, err := luaengine.Evaluate(ctx, program)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
			t.Fatalf("execution did not honor the caller deadline: %q elapsed=%s err=%v", source, time.Since(started), err)
		}
		if _, err := luaengine.Evaluate(context.Background(), testProgram("return function() return weir.keep() end")); err != nil {
			t.Fatalf("execution failed after cancellation: %v", err)
		}
	}
}

func TestEvaluateBoundsExecutionWorkAndCompilation(t *testing.T) {
	sources := []string{
		"while true do end",
		"return function() while true do end end",
		"return function() local function recurse() return 1 + recurse() end; return recurse() end",
		"return function() local t = {}; for i = 1, 5000 do t[i] = 1 end; table.sort(t); return {} end",
	}
	locals := make([]string, 201)
	for index := range locals {
		locals[index] = "var" + strconv.Itoa(index)
	}
	sources = append(sources, "local "+strings.Join(locals, ",")+"; return function() return {} end")
	for _, source := range sources {
		started := time.Now()
		program := testProgram(source)
		limits := luaengine.DefaultLimits()
		limits.MaxInstructions = 1_000_000
		program.Limits = &limits
		_, err := luaengine.Evaluate(context.Background(), program)
		if err == nil || time.Since(started) > 2*time.Second {
			t.Fatalf("execution bound not enforced: elapsed=%s err=%v", time.Since(started), err)
		}
	}
}

func TestEvaluateCannotHideWorkExhaustionBehindAction(t *testing.T) {
	for _, action := range []string{"keep", "delete"} {
		program := testProgram("return function() local result = weir." + action + "(); pcall(function() while true do end end); return result end")
		limits := luaengine.DefaultLimits()
		limits.MaxInstructions = 1_000_000
		program.Limits = &limits
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatalf("pcall hid work exhaustion behind %s", action)
		}
	}
}

func TestEvaluateSortComparatorObservesInPlaceArray(t *testing.T) {
	program := testProgram(`return function()
		local items = {4, 3, 2, 1}
		local observed = false
		table.sort(items, function(a, b)
			if table.concat(items, ",") ~= "4,3,2,1" then observed = true end
			return a < b
		end)
		assert(observed and table.concat(items, ",") == "1,2,3,4")
		return weir.keep()
	end`)
	if _, err := luaengine.Evaluate(context.Background(), program); err != nil {
		t.Fatal(err)
	}
}

func TestEvaluateSourceValueAndLibraryResultBounds(t *testing.T) {
	for _, source := range []string{"", strings.Repeat(" ", luaengine.MaxSourceBytes+1), "return\x00", "\x1bLua", string([]byte{255})} {
		program := testProgram(source)
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatal("accepted invalid or oversized source")
		}
	}
	program := testProgram("return function(current) return current end")
	program.Current = ValueObject(Field("large", ValueString(strings.Repeat("x", value.MaxBytes))))
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized current value")
	}
	program.Input, program.Current = program.Current, ValueObject()
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized input value")
	}
	program = testProgram("return function(_, incoming) return {v = string.format('%s%s', incoming.large, incoming.large)} end")
	program.Input = ValueObject(Field("large", ValueString(strings.Repeat("x", value.MaxBytes/2+1))))
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized format result")
	}
	program.Source = "return function(_, incoming) return {v = table.concat({incoming.large, incoming.large})} end"
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized concat result")
	}
	program.Source = "return function(_, incoming) return weir.reject(incoming.large) end"
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized rejection message")
	}
}

func TestEvaluateRejectsInvalidValuesBeforeRunningSource(t *testing.T) {
	noncanonical := value.Value{Kind: value.Bool, Text: "unexpected payload"}
	overflow := value.Value{Kind: value.Int32, Integer: 2147483648}
	duplicate := ValueObject(Field("same", ValueInt32(1)), Field("same", ValueInt32(2)))
	invalidUTF8 := ValueString(string([]byte{255}))
	for _, invalid := range []value.Value{noncanonical, overflow, duplicate, invalidUTF8} {
		for _, name := range []string{"current", "input"} {
			program := testProgram(`error("source executed")`)
			if name == "current" {
				program.Current = invalid
			} else {
				program.Input = invalid
			}
			_, err := luaengine.Evaluate(context.Background(), program)
			if err == nil || !strings.HasPrefix(err.Error(), "invalid "+name+" value:") {
				t.Fatalf("invalid %s kind %v reached Lua source: %v", name, invalid.Kind, err)
			}
		}
	}
}

func TestEvaluateProductExampleUsesStableObservationTime(t *testing.T) {
	source, err := os.ReadFile("../../examples/lua/product.lua")
	if err != nil {
		t.Fatal(err)
	}
	program := testProgram(string(source))
	program.ObservedAt = time.Date(2026, 10, 7, 13, 2, 3, 123456789, time.FixedZone("local", 8*60*60))
	currentTags := value.Value{Kind: value.Array, Items: []value.Value{ValueString("a"), ValueString("b")}}
	incomingTags := value.Value{Kind: value.Array, Items: []value.Value{ValueString("b"), ValueString("c"), ValueString("")}}
	program.Current = ValueObject(Field("title", ValueString("old")), Field("tags", currentTags), Field("untouched", ValueInt32(4)))
	program.Input = ValueObject(Field("title", ValueString("new")), Field("tags", incomingTags))
	for range 2 {
		got, err := luaengine.Evaluate(context.Background(), program)
		if err != nil {
			t.Fatal(err)
		}
		stamp, _ := got.Value.Lookup("updated_at")
		title, _ := got.Value.Lookup("title")
		tags, _ := got.Value.Lookup("tags")
		untouched, _ := got.Value.Lookup("untouched")
		wantTags := value.Value{Kind: value.Array, Items: []value.Value{ValueString("a"), ValueString("b"), ValueString("c")}}
		if stamp.Text != "2026-10-07T05:02:03.123456789Z" || title.Text != "new" || !reflect.DeepEqual(tags, wantTags) || untouched.Integer != 4 {
			t.Fatalf("product merge differs: %#v", got)
		}
	}
	program.Source = "return function() assert(weir.time.now() == weir.time.now()); return {now = weir.time.now()} end"
	program.ObservedAt = time.Time{}
	before := time.Now()
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	stamp, _ := got.Value.Lookup("now")
	observed, err := time.Parse(time.RFC3339Nano, stamp.Text)
	if err != nil || observed.Before(before) || observed.After(time.Now()) {
		t.Fatalf("default observation time is invalid: %q (%v)", stamp.Text, err)
	}
}

func TestEvaluateConcurrentCallsHaveFreshGlobalsAndLibraries(t *testing.T) {
	program := testProgram(`return function(current)
		assert(leaked == nil and string.upper ~= nil and math.floor ~= nil and table.concat ~= nil and weir.time.now ~= nil)
		leaked = true
		string.upper = nil
		math.floor = nil
		table.concat = nil
		weir.time.now = nil
		current.n = current.n + 1
		return current
	end`)
	var calls sync.WaitGroup
	for range 24 {
		calls.Go(func() {
			got, err := luaengine.Evaluate(context.Background(), program)
			field, lookupErr := got.Value.Lookup("n")
			if err != nil || lookupErr != nil || field.Integer != 2 {
				t.Errorf("state leaked: result=%#v err=%v lookup=%v", got, err, lookupErr)
			}
		})
	}
	calls.Wait()
	if program.Current.Fields[0].Value.Integer != 1 {
		t.Fatal("concurrent execution mutated the shared input")
	}
}

func BenchmarkEvaluateMerge(b *testing.B) {
	program := testProgram("return function(current, incoming) current.n = incoming.n; return current end")
	program.Input = ValueObject(Field("n", ValueInt32(2)))
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, err := luaengine.Evaluate(ctx, program); err != nil {
			b.Fatal(err)
		}
	}
}

func testProgram(source string) luaengine.Program {
	current := ValueObject(Field("n", ValueInt32(1)))
	input := ValueObject()
	program := luaengine.Program{Source: source, Current: current, Input: input}
	return program
}

func ValueObject(fields ...value.Field) value.Value {
	result := value.Value{Kind: value.Object, Fields: fields}
	return result
}

func Field(name string, v value.Value) value.Field {
	result := value.Field{Name: name, Value: v}
	return result
}

func ValueString(text string) value.Value {
	result := value.Value{Kind: value.String, Text: text}
	return result
}

func ValueInt32(n int64) value.Value {
	result, err := value.Integer(value.Int32, n)
	if err != nil {
		panic(err)
	}
	return result
}

func ValueInt64(n int64) value.Value {
	result, err := value.Integer(value.Int64, n)
	if err != nil {
		panic(err)
	}
	return result
}

func ValueNull() value.Value {
	result := value.Value{Kind: value.Null}
	return result
}
