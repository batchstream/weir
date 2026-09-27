package goluaprobe

import (
	"errors"
	"io"
	"math"
	"reflect"
	"testing"

	rt "github.com/arnodel/golua/runtime"
	"github.com/batchstream/weir/internal/value"
)

// Resource screening failed: do not build a second transform/codec framework.
// These tests distinguish exact Value transport from native Lua arithmetic.
func TestTypedValueTransportIsExactButNotABridge(t *testing.T) {
	vectors := []value.Value{
		{Kind: value.Int32, Integer: math.MinInt32}, {Kind: value.Int32, Integer: math.MaxInt32},
		{Kind: value.Int64, Integer: math.MinInt64}, {Kind: value.Int64, Integer: math.MaxInt64},
		{Kind: value.Missing}, {Kind: value.Null}, {Kind: value.Array, Items: []value.Value{}},
		{Kind: value.Object, Fields: []value.Field{}}, {Kind: value.Bytes, Data: []byte{0, 1, 255}},
		{Kind: value.Extended, Type: "mongodb.bson.objectid.v1", Data: []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}},
		{Kind: value.Extended, Type: "mongodb.bson.decimal128.v1", Data: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 48}},
		{Kind: value.Extended, Type: "mongodb.bson.binary.v1", Data: []byte{3, 0, 0, 0, 128, 0, 1, 255}},
		{Kind: value.Extended, Type: "unknown.example.v1", Data: []byte{1, 2, 3}},
		{Kind: value.Object, Fields: []value.Field{{Name: "z", Value: value.Value{Kind: value.Null}}, {Name: "a", Value: value.Value{Kind: value.Missing}}, {Name: "z", Value: value.Value{Kind: value.Bool}}}},
	}
	for _, input := range vectors {
		for repeat := 0; repeat < 3; repeat++ {
			r := rt.New(io.Discard)
			bindings := rt.NewTable()
			// Host-owned immutable vectors: no claim that this copies slices or
			// accounts their payload. A real bridge must do both before admission.
			r.SetEnv(bindings, "current", r.NewUserDataValue(input, nil))
			env := readonlyEnv(r, bindings)
			def := profile()
			_, err := r.MainThread().CallContext(def, func() error {
				got, err := runChunk(r, `local x=current; return x`, env)
				if err != nil {
					return err
				}
				out := got.AsUserData().Value().(value.Value)
				if !reflect.DeepEqual(input, out) {
					return errors.New("typed value changed")
				}
				return nil
			})
			r.Close(nil)
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	if _, err := vectors[len(vectors)-1].Lookup("z"); err == nil {
		t.Fatal("duplicate typed field must be ambiguous")
	}
}

func TestNativeNumbersCannotImplementCheckedValues(t *testing.T) {
	r := rt.New(io.Discard)
	defer r.Close(nil)
	def := profile()
	_, err := r.MainThread().CallContext(def, func() error {
		got, err := runChunk(r, `local n=9223372036854775807; return n+1`, r.GlobalEnv())
		if err != nil {
			return err
		}
		if got.AsInt() != math.MinInt64 {
			t.Fatal("native Lua wrap counterexample changed")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	max, _ := value.Integer(value.Int64, math.MaxInt64)
	one, _ := value.Integer(value.Int64, 1)
	if _, err := value.Arithmetic(max, one, '+'); err == nil {
		t.Fatal("checked Value overflow accepted")
	}
	narrow, _ := value.Integer(value.Int32, 1)
	if _, err := value.Arithmetic(narrow, one, '+'); err == nil {
		t.Fatal("mixed widths accepted")
	}
	if _, err := value.Convert(max, value.Int32); err == nil {
		t.Fatal("narrowing overflow accepted")
	}
	converted, err := value.Convert(narrow, value.Int64)
	if err != nil || converted.Kind != value.Int64 || converted.Integer != 1 {
		t.Fatal(err)
	}
	float := value.Value{Kind: value.Float64, Float: 1}
	if _, err := value.Convert(float, value.Int64); err == nil {
		t.Fatal("existing model silently converts floats")
	}
	t.Log("native Lua int64 wraps; Value checks widths/overflow; integer/float explicit bridge conversion remains unimplemented")
}

func TestOpaqueValueRejectsGuestOperationsAndExposesHostAliasGap(t *testing.T) {
	input := value.Value{Kind: value.Bytes, Data: []byte{1, 2, 3}}
	for _, source := range []string{`return current+1`, `return current.."x"`, `current[1]=2`, `return current.Data`} {
		r := rt.New(io.Discard)
		bindings := rt.NewTable()
		r.SetEnv(bindings, "current", r.NewUserDataValue(input, nil))
		env := readonlyEnv(r, bindings)
		def := profile()
		_, err := r.MainThread().CallContext(def, func() error {
			_, err := runChunk(r, source, env)
			return err
		})
		r.Close(nil)
		if err == nil {
			t.Fatal("opaque guest operation allowed", source)
		}
	}
	r := rt.New(io.Discard)
	defer r.Close(nil)
	wrapped := r.NewUserDataValue(input, nil)
	out := wrapped.AsUserData().Value().(value.Value)
	out.Data[0] = 42
	if input.Data[0] != 42 {
		t.Fatal("fixed-version shallow alias counterexample changed")
	}
	t.Log("NO-GO naive bridge: userdata keeps host slice aliases; exact transport is not independent output ownership")
}

// Only fuzz the established typed scalar boundary after the compiler fails
// screening. Finite fuzzing is not evidence for arbitrary-source parser safety.
func FuzzTypedIntegerTransport(f *testing.F) {
	f.Add(int64(math.MinInt64))
	f.Add(int64(math.MaxInt64))
	f.Add(int64(9007199254740993))
	f.Fuzz(func(t *testing.T, n int64) {
		r := rt.New(io.Discard)
		defer r.Close(nil)
		v, err := value.Integer(value.Int64, n)
		if err != nil {
			t.Fatal(err)
		}
		r.SetEnv(r.GlobalEnv(), "current", r.NewUserDataValue(v, nil))
		def := profile()
		_, err = r.MainThread().CallContext(def, func() error {
			got, err := runChunk(r, `return current`, r.GlobalEnv())
			if err != nil {
				return err
			}
			out := got.AsUserData().Value().(value.Value)
			if out.Kind != value.Int64 || out.Integer != n {
				t.Fatal("lost type or precision")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	})
}
