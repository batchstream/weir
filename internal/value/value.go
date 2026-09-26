// Package value is the bounded, ordered transform value model. It has no codec or VM dependency.
package value

import (
	"fmt"
	"math"
)

type Kind uint8

const (
	Missing Kind = iota
	Null
	Bool
	Int32
	Int64
	Float64
	String
	Bytes
	Array
	Object
	Extended
)

type Field struct {
	Name  string
	Value Value
}
type Value struct {
	Kind    Kind
	Integer int64
	Float   float64
	Boolean bool
	Text    string
	Data    []byte
	Fields  []Field
	Items   []Value
	Type    string
}

func Integer(kind Kind, n int64) (Value, error) {
	var zero Value
	if kind != Int32 && kind != Int64 || kind == Int32 && (n < math.MinInt32 || n > math.MaxInt32) {
		return zero, fmt.Errorf("integer conversion overflow")
	}
	v := Value{Kind: kind, Integer: n}
	return v, nil
}
func Convert(v Value, kind Kind) (Value, error) {
	var zero Value
	if v.Kind != Int32 && v.Kind != Int64 {
		return zero, fmt.Errorf("not an integer")
	}
	return Integer(kind, v.Integer)
}
func Arithmetic(a, b Value, op byte) (Value, error) {
	var zero Value
	if a.Kind != b.Kind || a.Kind != Int32 && a.Kind != Int64 {
		return zero, fmt.Errorf("explicit integer conversion required")
	}
	x, y := a.Integer, b.Integer
	var n int64
	switch op {
	case '+':
		if y > 0 && x > math.MaxInt64-y || y < 0 && x < math.MinInt64-y {
			return zero, fmt.Errorf("overflow")
		}
		n = x + y
	case '-':
		if y < 0 && x > math.MaxInt64+y || y > 0 && x < math.MinInt64+y {
			return zero, fmt.Errorf("overflow")
		}
		n = x - y
	case '*':
		if x == math.MinInt64 && y == -1 || y == math.MinInt64 && x == -1 {
			return zero, fmt.Errorf("overflow")
		}
		n = x * y
		if y != 0 && n/y != x {
			return zero, fmt.Errorf("overflow")
		}
	default:
		return zero, fmt.Errorf("invalid operator")
	}
	return Integer(a.Kind, n)
}
func (v Value) Lookup(name string) (Value, error) {
	var found Value
	seen := false
	if v.Kind != Object {
		return found, fmt.Errorf("not an object")
	}
	for _, f := range v.Fields {
		if f.Name == name {
			if seen {
				return found, fmt.Errorf("ambiguous field")
			}
			found = f.Value
			seen = true
		}
	}
	return found, nil
}

// Increment is a finite conformance transform, not a general runtime or public DSL.
// Missing records receive an explicit identity supplied by the adapter.
func Increment(current Value, identity Field) (Value, error) {
	var zero Value
	if current.Kind == Missing {
		z := Value{Kind: Int64}
		current = Value{Kind: Object, Fields: []Field{identity, {Name: "n", Value: z}}}
	}
	n, err := current.Lookup("n")
	if err != nil {
		return zero, err
	}
	one, err := Integer(n.Kind, 1)
	if err != nil {
		return zero, err
	}
	next, err := Arithmetic(n, one, '+')
	if err != nil {
		return zero, err
	}
	for i := range current.Fields {
		if current.Fields[i].Name == "n" {
			current.Fields[i].Value = next
		}
	}
	return current, nil
}
