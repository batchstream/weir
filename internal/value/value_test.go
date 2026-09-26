package value

import (
	"math"
	"testing"
)

func TestCheckedIntegers(t *testing.T) {
	cases := []struct {
		kind Kind
		a, b int64
		op   byte
		want int64
		fail bool
	}{
		{Int32, 1, 1, '+', 2, false}, {Int64, 9007199254740993, 1, '+', 9007199254740994, false},
		{Int32, math.MaxInt32, 1, '+', 0, true}, {Int64, math.MaxInt64, 1, '+', 0, true}, {Int64, math.MinInt64, 1, '-', 0, true},
		{Int64, math.MinInt64, -1, '*', 0, true}, {Int64, 3037000500, 3037000500, '*', 0, true}, {Int32, 100000, 100000, '*', 0, true},
	}
	for _, c := range cases {
		a, _ := Integer(c.kind, c.a)
		b, _ := Integer(c.kind, c.b)
		got, err := Arithmetic(a, b, c.op)
		if (err != nil) != c.fail || err == nil && (got.Integer != c.want || got.Kind != c.kind) {
			t.Fatalf("%+v: %+v %v", c, got, err)
		}
	}
	a, _ := Integer(Int32, 1)
	b, _ := Integer(Int64, 1)
	if _, err := Arithmetic(a, b, '+'); err == nil {
		t.Fatal("mixed widths")
	}
	converted, err := Convert(b, Int32)
	if err != nil || converted.Kind != Int32 {
		t.Fatal(err)
	}
	big, _ := Integer(Int64, math.MaxInt64)
	if _, err := Convert(big, Int32); err == nil {
		t.Fatal("narrow overflow")
	}
}
