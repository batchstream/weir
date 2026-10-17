package luaengine

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"
	"unsafe"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/vm"
)

type decodeFrame struct {
	source vm.Value
	hint   value.Value
	output *value.Value
	table  *vm.Table
	keys   []vm.Value
	info   tableInfo
	index  int
}

func (b *bridge) fromLua(v vm.Value) (value.Value, error) {
	var result value.Value
	scope := b.state.Budget()
	first := decodeFrame{source: v, output: &result}
	stack := budget.Append(scope, []decodeFrame(nil), first)
	scope.Require(256)
	active := make(map[*vm.Table]bool)
	for len(stack) != 0 {
		if err := b.checkpoint(); err != nil {
			var empty value.Value
			return empty, err
		}
		frame := &stack[len(stack)-1]
		if !frame.source.IsTable() {
			decoded, err := b.decodeScalar(frame.source, frame.hint)
			if err != nil {
				var empty value.Value
				return empty, err
			}
			*frame.output = decoded
			stack = stack[:len(stack)-1]
			continue
		}
		if frame.table == nil {
			if err := b.openTable(frame, active); err != nil {
				var empty value.Value
				return empty, err
			}
		}
		if frame.index == len(frame.keys) {
			delete(active, frame.table)
			stack = stack[:len(stack)-1]
			continue
		}
		key := frame.keys[frame.index]
		var destination *value.Value
		if frame.output.Kind == value.Object {
			frame.output.Fields[frame.index].Name = cloneLuaText(scope, key.AsString())
			destination = &frame.output.Fields[frame.index].Value
		} else {
			destination = &frame.output.Items[frame.index]
			key = vm.NewInt(int64(frame.index + 1))
		}
		frame.index++
		next := decodeFrame{source: frame.table.Get(key), hint: frame.info.hints[key], output: destination}
		stack = budget.Append(scope, stack, next)
	}
	return result, nil
}

func (b *bridge) openTable(frame *decodeFrame, active map[*vm.Table]bool) error {
	table, ok := frame.source.AsTable().(*vm.Table)
	if !ok || table.IsThread() {
		return fmt.Errorf("unsupported Lua table")
	}
	if active[table] {
		return fmt.Errorf("cyclic Lua table")
	}
	scope := b.state.Budget()
	var field value.Field
	var item value.Value
	scope.Require(256)
	active[table] = true
	frame.table, frame.info = table, b.tables[table]
	stringsOnly, integersOnly := true, true
	var interrupted error
	table.ForEach(func(key, child vm.Value) bool {
		if interrupted = b.checkpoint(); interrupted != nil {
			return false
		}
		stringsOnly = stringsOnly && key.IsString()
		integersOnly = integersOnly && key.IsInt() && key.AsInt() > 0
		frame.keys = budget.Append(scope, frame.keys, key)
		return true
	})
	if interrupted != nil {
		return interrupted
	}
	kind := frame.info.kind
	if kind == value.Missing {
		switch {
		case len(frame.keys) == 0 || stringsOnly:
			kind = value.Object
		case integersOnly:
			kind = value.Array
		default:
			return fmt.Errorf("mixed or unsupported Lua table keys")
		}
	}
	frame.output.Kind = kind
	if kind == value.Object {
		if !stringsOnly {
			return fmt.Errorf("object keys must be strings")
		}
		for _, key := range frame.keys {
			if err := b.checkpoint(); err != nil {
				return err
			}
			name := key.AsString()
			if !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
				return fmt.Errorf("invalid field name")
			}
		}
		if len(frame.keys) != 0 {
			scope.RequireArray(unsafe.Sizeof(field), len(frame.keys))
			frame.output.Fields = make([]value.Field, len(frame.keys))
		}
		return nil
	}
	if !integersOnly {
		return fmt.Errorf("array keys must be positive integers")
	}
	for _, key := range frame.keys {
		if err := b.checkpoint(); err != nil {
			return err
		}
		if key.AsInt() > int64(len(frame.keys)) {
			return fmt.Errorf("sparse Lua array")
		}
	}
	if len(frame.keys) != 0 {
		scope.RequireArray(unsafe.Sizeof(item), len(frame.keys))
		frame.output.Items = make([]value.Value, len(frame.keys))
	}
	return nil
}

func (b *bridge) decodeScalar(v vm.Value, hint value.Value) (value.Value, error) {
	var empty value.Value
	var result value.Value
	switch {
	case v.IsNil():
		return empty, fmt.Errorf("document contains missing value")
	case v.IsBool():
		result = value.Value{Kind: value.Bool, Boolean: v.ToBool()}
	case v.IsInt():
		kind := value.Int64
		if hint.Kind == value.Int32 && v.AsInt() >= math.MinInt32 && v.AsInt() <= math.MaxInt32 {
			kind = value.Int32
		}
		result = value.Value{Kind: kind, Integer: v.AsInt()}
	case v.IsFloat():
		if math.IsNaN(v.AsFloat()) || math.IsInf(v.AsFloat(), 0) {
			return empty, fmt.Errorf("invalid float64")
		}
		result = value.Value{Kind: value.Float64, Float: v.AsFloat()}
		if hint.Kind == value.Extended {
			original, err := strconv.ParseFloat(string(hint.Data), 64)
			if err == nil && math.Float64bits(original) == math.Float64bits(v.AsFloat()) {
				result = hint
			}
		}
	case v.IsString():
		text := v.AsString()
		if !utf8.ValidString(text) {
			return empty, fmt.Errorf("invalid UTF-8 string")
		}
		result = value.Value{Kind: value.String, Text: cloneLuaText(b.state.Budget(), text)}
	case v.IsUserdata():
		leaf, ok := v.AsUserdata().Data.(scalarLeaf)
		if !ok || v.AsUserdata().Metatable() != b.leafMeta {
			return empty, fmt.Errorf("unsupported userdata")
		}
		result = leaf.value
		result.Type = cloneLuaText(b.state.Budget(), result.Type)
	default:
		return empty, fmt.Errorf("unsupported Lua value")
	}
	if len(result.Data) != 0 {
		b.state.Budget().RequireArray(1, len(result.Data))
		result.Data = append([]byte(nil), result.Data...)
	}
	return result, nil
}
