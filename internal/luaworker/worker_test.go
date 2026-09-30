package luaworker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/luaengine"
	worker "github.com/batchstream/weir/internal/luaworker"
	"github.com/batchstream/weir/internal/value"
)

func TestMain(m *testing.M) {
	if len(os.Args) == 2 && os.Args[1] == "--weir-lua-worker" {
		if err := luaengine.Serve(os.Stdin, os.Stdout); err != nil {
			fmt.Fprintln(os.Stderr, "worker failed")
			os.Exit(1)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func TestRunnerMergesTypedValues(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
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
	program := worker.Program{Source: "return weir.replace(weir.merge(current, input))", Current: current, Input: patch}
	got, err := runner.Run(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(
		Field("count", ValueInt32(5)),
		Field("nested", ValueObject(Field("keep", ValueString("yes")), Field("replace", ValueString("new")), Field("add", ValueInt64(9007199254740993)))),
	)
	if got.Action != worker.Replace || !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("unexpected result: %#v", got)
	}
}

func TestRunnerMergeCreatesMissingRecordAndDropsMissingFields(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
	program := worker.Program{
		Source:  "return weir.replace(weir.merge(current, input))",
		Current: ValueMissing(),
		Input: ValueObject(
			Field("count", ValueInt32(1)),
			Field("profile", ValueObject(Field("name", ValueString("new")), Field("remove", ValueMissing()))),
		),
	}
	got, err := runner.Run(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(
		Field("count", ValueInt32(1)),
		Field("profile", ValueObject(Field("name", ValueString("new")))),
	)
	if got.Action != worker.Replace || !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("unexpected missing-record merge: %#v", got)
	}
}

func TestRunnerActionsAndRestrictedEnvironment(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
	cases := []struct {
		source string
		want   worker.Action
	}{
		{source: "return weir.keep()", want: worker.Keep},
		{source: "return weir.delete()", want: worker.Delete},
		{source: "return weir.reject('invalid')", want: worker.Reject},
	}
	for _, tc := range cases {
		program := testProgram(tc.source)
		got, err := runner.Run(context.Background(), program)
		if err != nil || got.Action != tc.want {
			t.Fatalf("source %q: got %#v, err %v", tc.source, got, err)
		}
	}
	for _, source := range []string{"return print('not available')", "return require('os')"} {
		program := testProgram(source)
		if _, err := runner.Run(context.Background(), program); err == nil {
			t.Fatalf("standard library was available: %q", source)
		}
	}
}

func TestRunnerChecksTypedArithmetic(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
	program := testProgram(`return weir.replace(weir.object("n", weir.add(weir.i64("9007199254740993"), weir.i64("1"))))`)
	got, err := runner.Run(context.Background(), program)
	if err != nil {
		t.Fatal(err)
	}
	want := ValueObject(Field("n", ValueInt64(9007199254740994)))
	if !reflect.DeepEqual(got.Value, want) {
		t.Fatalf("got %#v, want %#v", got.Value, want)
	}
	program = testProgram(`return weir.add(weir.i64("9223372036854775807"), weir.i64("1"))`)
	if _, err := runner.Run(context.Background(), program); err == nil {
		t.Fatal("accepted overflowing integer arithmetic")
	}
}

func TestRunnerDeadlineStopsWorker(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	started := time.Now()
	program := testProgram("while true do end")
	_, err := runner.Run(ctx, program)
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > time.Second {
		t.Fatalf("worker was not stopped at the deadline: elapsed=%s err=%v", time.Since(started), err)
	}
}

func TestRunnerEnforcesInputAndOutputCaps(t *testing.T) {
	runner := newTestRunner(t, worker.DefaultTimeout)
	program := testProgram("return weir.keep()")
	program.Source = strings.Repeat(" ", worker.MaxSourceBytes+1)
	if _, err := runner.Run(context.Background(), program); err == nil {
		t.Fatal("accepted oversized source")
	}

	program = testProgram("return weir.replace(current)")
	program.Current = ValueObject(Field("large", ValueString(strings.Repeat("\x00", 70<<10))))
	_, err := runner.Run(context.Background(), program)
	if err == nil || !strings.Contains(err.Error(), "worker response exceeds limit") {
		t.Fatalf("output cap not enforced: %v", err)
	}

	program.Current = ValueObject(Field("large", ValueString(strings.Repeat("\x00", 100<<10))))
	if _, err := runner.Run(context.Background(), program); err == nil || !strings.Contains(err.Error(), "request exceeds limit") {
		t.Fatalf("input cap not enforced: %v", err)
	}
}

func TestServeRejectsMalformedAndOversizedRequests(t *testing.T) {
	cases := [][]byte{
		[]byte(`{"program":{},"timeout_millis":0}`),
		[]byte(strings.Repeat(" ", worker.MaxRequestBytes+1)),
		[]byte(`{"program":{},"timeout_millis":1} {}`),
	}
	for _, raw := range cases {
		var output strings.Builder
		if err := luaengine.Serve(strings.NewReader(string(raw)), &output); err != nil {
			t.Fatal(err)
		}
		var answer worker.Response
		if err := json.Unmarshal([]byte(output.String()), &answer); err != nil || answer.Error == "" {
			t.Fatalf("request unexpectedly accepted: %v %q", err, output.String())
		}
	}
}

func newTestRunner(t *testing.T, timeout time.Duration) *worker.Runner {
	t.Helper()
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	config := worker.Config{Executable: executable, Timeout: timeout}
	runner, err := worker.New(config)
	if err != nil {
		t.Fatal(err)
	}
	return runner
}

func testProgram(source string) worker.Program {
	current := ValueObject(Field("n", ValueInt32(1)))
	input := ValueObject()
	program := worker.Program{Source: source, Current: current, Input: input}
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
