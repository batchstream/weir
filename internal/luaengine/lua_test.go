package luaengine_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/luaengine"
	"github.com/batchstream/weir/internal/value"
)

func TestEvaluateMergesTypedValues(t *testing.T) {
	current := ValueObject(
		Field("count", ValueInt32(4)),
		Field("nested", ValueObject(Field("keep", ValueString("yes")), Field("replace", ValueString("old")))),
		Field("remove", ValueNull()),
	)
	patch := ValueObject(
		Field("count", ValueInt32(5)),
		Field("nested", ValueObject(Field("replace", ValueString("new")), Field("add", ValueInt64(9007199254740993)))),
		Field("remove", ValueMissing()),
	)
	program := luaengine.Program{Source: "return weir.replace(weir.merge(current, input))", Current: current, Input: patch}
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(
		Field("count", ValueInt32(5)),
		Field("nested", ValueObject(Field("keep", ValueString("yes")), Field("replace", ValueString("new")), Field("add", ValueInt64(9007199254740993)))),
	)
	if got.Action != luaengine.Replace || !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestEvaluateMergeCreatesMissingRecordAndDropsMissingFields(t *testing.T) {
	program := luaengine.Program{
		Source:  "return weir.replace(weir.merge(current, input))",
		Current: ValueMissing(),
		Input: ValueObject(
			Field("count", ValueInt32(1)),
			Field("profile", ValueObject(Field("name", ValueString("new")), Field("remove", ValueMissing()))),
		),
	}
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(
		Field("count", ValueInt32(1)),
		Field("profile", ValueObject(Field("name", ValueString("new")))),
	)
	if got.Action != luaengine.Replace || !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("unexpected missing-record merge: %#v", got)
	}
}

func TestEvaluateActionsAndRestrictedEnvironment(t *testing.T) {
	cases := []struct {
		source string
		want   luaengine.Action
	}{
		{source: "return weir.keep()", want: luaengine.Keep},
		{source: "return nil", want: luaengine.Keep},
		{source: "local ignored = 1", want: luaengine.Keep},
		{source: `return weir.object("n", weir.i32("1"))`, want: luaengine.Replace},
		{source: "return weir.delete()", want: luaengine.Delete},
		{source: "return weir.reject('invalid')", want: luaengine.Reject},
	}
	for _, tc := range cases {
		program := testProgram(tc.source)
		got, err := luaengine.Evaluate(context.Background(), program)
		if err != nil || got.Action != tc.want {
			t.Fatalf("source %q: got %#v, err %v", tc.source, got, err)
		}
	}
	for _, source := range []string{"return print('not available')", "return require('os')"} {
		program := testProgram(source)
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatalf("standard library was available: %q", source)
		}
	}
}

func TestEvaluateChecksTypedArithmetic(t *testing.T) {
	program := testProgram(`return weir.replace(weir.object("n", weir.add(weir.i64("9007199254740993"), weir.i64("1"))))`)
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(Field("n", ValueInt64(9007199254740994)))
	if !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("got %#v, want %#v", got.Value, want)
	}
	program = testProgram(`return weir.add(weir.i64("9223372036854775807"), weir.i64("1"))`)
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted overflowing integer arithmetic")
	}
}

func TestEvaluateDeadlineStopsExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	program := testProgram("while true do end")
	_, err := luaengine.Evaluate(ctx, program)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("Lua evaluation was not stopped at the deadline: elapsed=%s err=%v", time.Since(started), err)
	}
}

func TestEvaluateEnforcesOwnDeadlineAndRecovers(t *testing.T) {
	program := testProgram("while true do end")
	started := time.Now()
	_, err := luaengine.Evaluate(context.Background(), program)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 2*time.Second {
		t.Fatalf("execution deadline not enforced: elapsed=%s err=%v", time.Since(started), err)
	}
	program.Source = "return weir.keep()"
	if _, err := luaengine.Evaluate(context.Background(), program); err != nil {
		t.Fatalf("evaluation failed after timeout: %v", err)
	}
}

func TestEvaluateEnforcesSourceValueAndResultLimits(t *testing.T) {
	for _, source := range []string{"", strings.Repeat(" ", luaengine.MaxSourceBytes+1), "return\x00", "\x1bLua", string([]byte{255})} {
		program := testProgram(source)
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatal("accepted invalid or oversized source")
		}
	}
	for _, source := range []string{
		"return weir.i32('1')",
		"return weir.replace(weir.array())",
		"return weir.replace(weir.object('missing', weir.missing()))",
		"return nil, weir.delete()",
		"return weir.reject('" + strings.Repeat("x", luaengine.MaxMessageBytes+1) + "')",
		"local function recurse() return 1 + recurse() end; return recurse()",
	} {
		program := testProgram(source)
		if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
			t.Fatalf("accepted invalid or unbounded result: %q", source)
		}
	}
	program := testProgram("return weir.replace(current)")
	program.Current = ValueObject(Field("large", ValueString(strings.Repeat("x", value.MaxBytes))))
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized current value")
	}
	program.Input = program.Current
	program.Current = ValueObject()
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized input value")
	}
	program.Source = "return weir.replace(weir.merge(current, input))"
	program.Current = ValueObject(Field("a", ValueString(strings.Repeat("x", value.MaxBytes/2))))
	program.Input = ValueObject(Field("b", ValueString(strings.Repeat("x", value.MaxBytes/2))))
	if _, err := luaengine.Evaluate(context.Background(), program); err == nil {
		t.Fatal("accepted oversized merged output")
	}
}

func TestEvaluateAcceptsBoundedValuesWithoutIPCEscapingLimits(t *testing.T) {
	program := testProgram("return weir.replace(current)")
	program.Current = ValueObject(Field("large", ValueString(strings.Repeat("\x00", 100<<10))))
	got, err := luaengine.Evaluate(context.Background(), program)
	if err != nil || !reflect.DeepEqual(got.Value, program.Current) {
		t.Fatalf("bounded value failed direct evaluation: %v", err)
	}
	got.Value.Fields[0].Value.Text = "changed"
	if program.Current.Fields[0].Value.Text == "changed" {
		t.Fatal("result shares mutable fields with the input")
	}
}

func TestEvaluateConcurrentCallsHaveIndependentStates(t *testing.T) {
	program := testProgram(`
        if leaked ~= nil or weir.keep == nil then return weir.reject('state leaked') end
        leaked = true
        weir.keep = nil
        return weir.replace(weir.set(current, 'n', weir.i32('2')))
    `)
	want := ValueObject(Field("n", ValueInt32(2)))
	var calls sync.WaitGroup
	for range 24 {
		calls.Go(func() {
			got, err := luaengine.Evaluate(context.Background(), program)
			if err != nil || got.Action != luaengine.Replace || !reflect.DeepEqual(got.Value, want) {
				t.Errorf("evaluation leaked state: result=%#v err=%v", got, err)
			}
		})
	}
	calls.Wait()
	if program.Current.Fields[0].Value.Integer != 1 {
		t.Fatal("evaluation mutated the shared current record")
	}
}

func BenchmarkEvaluateMerge(b *testing.B) {
	program := testProgram("return weir.replace(weir.merge(current, input))")
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

func ValueMissing() value.Value {
	result := value.Value{Kind: value.Missing}
	return result
}
