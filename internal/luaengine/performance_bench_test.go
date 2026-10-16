package luaengine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/compiler"
	"github.com/iceisfun/golua/parser"
	"github.com/iceisfun/golua/vm"
)

const performanceSource = "return function(current, incoming) current.n = incoming.n; return current end"

func performanceProgram(fields int) Program {
	current := value.Value{Kind: value.Object}
	integer := value.Value{Kind: value.Int32, Integer: 1}
	field := value.Field{Name: "n", Value: integer}
	current.Fields = append(current.Fields, field)
	for index := 1; index < fields; index++ {
		text := value.Value{Kind: value.String, Text: "a moderately sized record field used to measure conversion"}
		field := value.Field{Name: fmt.Sprintf("field_%03d", index), Value: text}
		current.Fields = append(current.Fields, field)
	}
	integer.Integer = 2
	field = value.Field{Name: "n", Value: integer}
	incoming := value.Value{Kind: value.Object, Fields: []value.Field{field}}
	program := Program{Source: performanceSource, Current: current, Input: incoming, ObservedAt: time.Unix(1, 0)}
	return program
}

func BenchmarkLuaPhases(b *testing.B) {
	program := performanceProgram(1)
	ctx := context.Background()
	b.Run("ParseCompile", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			block, err := parser.Parse("transform.lua", program.Source)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := compiler.Compile("transform.lua", block); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("FreshVM", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			state := vm.New(vm.WithContext(ctx))
			state.Close(ctx)
		}
	})
	b.Run("FreshVMEnvironment", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			state := vm.New(vm.WithContext(ctx))
			installEnvironment(state, program.ObservedAt)
			state.Close(ctx)
		}
	})
	b.Run("ValidateProgram", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if err := ValidateProgram(ctx, program); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("BridgeRoundtrip", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			conversion := newBridge(ctx)
			incoming, err := conversion.toLua(program.Current)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := conversion.fromLua(incoming); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEvaluatePayload(b *testing.B) {
	for _, fields := range []int{1, 100} {
		b.Run(fmt.Sprintf("Fields%d", fields), func(b *testing.B) {
			program := performanceProgram(fields)
			b.ReportAllocs()
			for b.Loop() {
				if _, err := Evaluate(context.Background(), program); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkBridgeKind(b *testing.B) {
	for _, depth := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("Depth%d", depth), func(b *testing.B) {
			child := value.Value{Kind: value.String, Text: "payload"}
			for range depth {
				field := value.Field{Name: "nested", Value: child}
				child = value.Value{Kind: value.Object, Fields: []value.Field{field}}
			}
			conversion := newBridge(context.Background())
			input, err := conversion.toLua(child)
			if err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := conversion.kind(input); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
