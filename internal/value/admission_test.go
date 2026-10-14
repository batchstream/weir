package value

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestJSONValueDataHasNoFormerByteNodeOrDepthGates(t *testing.T) {
	raw := []byte(`{"large":"` + strings.Repeat("x", 3<<20) + `","items":[` + strings.Repeat("true,", 9999) + `true],"nested":` + strings.Repeat(`{"x":`, 2000) + `{}` + strings.Repeat(`}`, 2000) + `}`)
	decoded, err := DecodeJSON(t.Context(), raw)
	if err != nil {
		t.Fatal("valid JSON retained a data admission gate", err)
	}
	encoded, err := EncodeJSON(t.Context(), decoded)
	if err != nil || !bytes.Equal(encoded, raw) {
		t.Fatal("deep/large JSON did not round trip exactly", err)
	}
}

func TestValueValidationAllowsSharedTreesAndRejectsCycles(t *testing.T) {
	leaf := Value{Kind: String, Text: "shared"}
	field := Field{Name: "v", Value: leaf}
	shared := Value{Kind: Object, Fields: []Field{field}}
	first := Field{Name: "a", Value: shared}
	second := Field{Name: "b", Value: shared}
	root := Value{Kind: Object, Fields: []Field{first, second}}
	if err := Validate(t.Context(), root); err != nil {
		t.Fatal("shared acyclic values rejected", err)
	}
	items := make([]Value, 1)
	cycle := Value{Kind: Array, Items: items}
	items[0] = cycle
	if err := Validate(t.Context(), cycle); err == nil {
		t.Fatal("cyclic Go value accepted")
	}
}

type cancelValueContext struct {
	context.Context
	cancel context.CancelFunc
	checks int
}

func (c *cancelValueContext) Err() error {
	c.checks++
	if c.checks == 10 {
		c.cancel()
	}
	return c.Context.Err()
}

func TestValueCodecsAndValidationCancelDuringTraversal(t *testing.T) {
	items := make([]Value, 10000)
	for index := range items {
		items[index] = Value{Kind: Bool, Boolean: true}
	}
	array := Value{Kind: Array, Items: items}
	field := Field{Name: "items", Value: array}
	root := Value{Kind: Object, Fields: []Field{field}}
	for _, operation := range []string{"validate", "encode", "decode"} {
		ctx, cancel := context.WithCancel(t.Context())
		bounded := &cancelValueContext{Context: ctx, cancel: cancel}
		var err error
		switch operation {
		case "validate":
			err = Validate(bounded, root)
		case "encode":
			_, err = EncodeJSON(bounded, root)
		case "decode":
			raw := []byte(`{"items":[` + strings.Repeat("true,", 9999) + `true]}`)
			_, err = DecodeJSON(bounded, raw)
		}
		cancel()
		if !errors.Is(err, context.Canceled) || bounded.checks < 10 {
			t.Fatal("large data traversal ignored cancellation", operation, err, bounded.checks)
		}
	}
}
