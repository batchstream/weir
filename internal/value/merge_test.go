package value

import (
	"reflect"
	"strings"
	"testing"
)

func TestMergeNestedObjectsAndPreservesOrder(t *testing.T) {
	base := Value{Kind: Object, Fields: []Field{
		{Name: "keep", Value: Value{Kind: Bool, Boolean: true}},
		{Name: "nested", Value: Value{Kind: Object, Fields: []Field{
			{Name: "left", Value: Value{Kind: Int32, Integer: 1}},
			{Name: "replace", Value: Value{Kind: String, Text: "old"}},
		}}},
		{Name: "remove", Value: Value{Kind: Null}},
	}}
	patch := Value{Kind: Object, Fields: []Field{
		{Name: "nested", Value: Value{Kind: Object, Fields: []Field{
			{Name: "replace", Value: Value{Kind: String, Text: "new"}},
			{Name: "right", Value: Value{Kind: Int64, Integer: 2}},
		}}},
		{Name: "remove", Value: Value{Kind: Missing}},
		{Name: "append", Value: Value{Kind: Bytes, Data: []byte{0, 1, 255}}},
	}}
	want := Value{Kind: Object, Fields: []Field{
		{Name: "keep", Value: Value{Kind: Bool, Boolean: true}},
		{Name: "nested", Value: Value{Kind: Object, Fields: []Field{
			{Name: "left", Value: Value{Kind: Int32, Integer: 1}},
			{Name: "replace", Value: Value{Kind: String, Text: "new"}},
			{Name: "right", Value: Value{Kind: Int64, Integer: 2}},
		}}},
		{Name: "append", Value: Value{Kind: Bytes, Data: []byte{0, 1, 255}}},
	}}
	got, err := Merge(base, patch)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("merged value mismatch:\n got: %#v\nwant: %#v", got, want)
	}
	got.Fields[1].Value.Fields[0].Value.Integer = 99
	got.Fields[2].Value.Data[0] = 99
	if base.Fields[1].Value.Fields[0].Value.Integer != 1 || patch.Fields[2].Value.Data[0] != 0 {
		t.Fatal("result aliases an input value")
	}
}

func TestMergeMissingBaseCreatesObject(t *testing.T) {
	base := Value{Kind: Missing}
	patch := Value{Kind: Object, Fields: []Field{
		{Name: "count", Value: Value{Kind: Int64, Integer: 9007199254740993}},
		{Name: "absent", Value: Value{Kind: Missing}},
		{Name: "profile", Value: Value{Kind: Object, Fields: []Field{
			{Name: "name", Value: Value{Kind: String, Text: "new"}},
			{Name: "absent", Value: Value{Kind: Missing}},
		}}},
	}}
	result, err := Merge(base, patch)
	if err != nil {
		t.Fatal(err)
	}
	if result.Kind != Object || len(result.Fields) != 2 || result.Fields[0].Name != "count" || result.Fields[1].Name != "profile" || len(result.Fields[1].Value.Fields) != 1 {
		t.Fatalf("unexpected merged object: %#v", result)
	}
	result.Fields[1].Value.Fields[0].Value.Text = "changed"
	if patch.Fields[2].Value.Fields[0].Value.Text != "new" {
		t.Fatal("created object aliases patch")
	}
}

func TestMergeTreatsMissingInsideArraysAsInvalid(t *testing.T) {
	base := Value{Kind: Object}
	patch := Value{Kind: Object, Fields: []Field{{Name: "items", Value: Value{Kind: Array, Items: []Value{{Kind: Missing}}}}}}
	if _, err := Merge(base, patch); err == nil {
		t.Fatal("accepted missing array element")
	}
}

func TestMergeRejectsInvalidTreesAndNonObjects(t *testing.T) {
	duplicate := Value{Kind: Object, Fields: []Field{
		{Name: "x", Value: Value{Kind: Null}},
		{Name: "x", Value: Value{Kind: Null}},
	}}
	empty := Value{Kind: Object}
	array := Value{Kind: Array}
	tooDeep := Value{Kind: Null}
	for i := 0; i < MaxDepth+1; i++ {
		parent := Value{Kind: Array, Items: []Value{tooDeep}}
		tooDeep = parent
	}
	tooLarge := Value{Kind: Object, Fields: []Field{{Name: "payload", Value: Value{Kind: String, Text: strings.Repeat("x", MaxBytes)}}}}
	cases := []struct {
		name  string
		base  Value
		patch Value
	}{
		{name: "base not object", base: array, patch: empty},
		{name: "patch not object", base: empty, patch: array},
		{name: "duplicate base key", base: duplicate, patch: empty},
		{name: "too deep", base: tooDeep, patch: empty},
		{name: "too large", base: tooLarge, patch: empty},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Merge(tc.base, tc.patch); err == nil {
				t.Fatal("accepted invalid merge input")
			}
		})
	}
}
