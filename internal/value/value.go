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
