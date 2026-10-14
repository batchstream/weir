package luaengine

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/vm"
)

type tableInfo struct {
	kind  value.Kind
	hints map[vm.Value]value.Value
}

type scalarLeaf struct {
	value value.Value
}

type bridge struct {
	ctx      context.Context
	state    *vm.VM
	tables   map[*vm.Table]tableInfo
	leafMeta *vm.Table
	null     vm.Value
}

func newBridge(ctx context.Context) *bridge {
	b := &bridge{ctx: ctx, tables: make(map[*vm.Table]tableInfo)}
	b.leafMeta = vm.NewEmptyTable()
	b.leafMeta.SetString(vm.MetaMetatable, vm.NewString("weir scalar"))
	b.leafMeta.SetString(vm.MetaTostring, vm.NewNativeFunc(func(state *vm.VM) int {
		userdata := state.Get(1).AsUserdata()
		if userdata == nil {
			panic("invalid weir scalar")
		}
		leaf, ok := userdata.Data.(scalarLeaf)
		if !ok {
			panic("invalid weir scalar")
		}
		text := "null"
		switch leaf.value.Kind {
		case value.Bytes:
			text = "bytes:" + base64.StdEncoding.EncodeToString(leaf.value.Data)
		case value.Extended:
			text = "extended:" + leaf.value.Type + ":" + base64.StdEncoding.EncodeToString(leaf.value.Data)
		}
		state.Set(0, vm.NewString(text))
		return 1
	}))
	null := value.Value{Kind: value.Null}
	b.null = b.wrapLeaf(null)
	return b
}

func (b *bridge) install(state *vm.VM, module *vm.Table) {
	b.state = state
	module.SetString("null", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 0 {
			panic("weir.null expects no arguments")
		}
		state.Set(0, b.null)
		return 1
	}))
	module.SetString("object", vm.NewNativeFunc(func(state *vm.VM) int {
		return b.container(state, value.Object)
	}))
	module.SetString("array", vm.NewNativeFunc(func(state *vm.VM) int {
		return b.container(state, value.Array)
	}))
	module.SetString("bytes", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 1 || !state.Get(1).IsString() {
			panic("weir.bytes expects a byte string")
		}
		text := state.Get(1).AsString()
		leaf := value.Value{Kind: value.Bytes, Data: []byte(text)}
		state.Set(0, b.wrapLeaf(leaf))
		return 1
	}))
	module.SetString("extended", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 2 || !state.Get(1).IsString() || !state.Get(2).IsString() {
			panic("weir.extended expects a type and byte string")
		}
		typeName, data := state.Get(1).AsString(), state.Get(2).AsString()
		leaf := value.Value{Kind: value.Extended, Type: typeName, Data: []byte(data)}
		if err := value.Validate(b.ctx, leaf); err != nil {
			panic(err)
		}
		if typeName == value.JSONNumberType && !value.IsJSONNumber(leaf) {
			panic("invalid exact JSON number")
		}
		state.Set(0, b.wrapLeaf(leaf))
		return 1
	}))
	module.SetString("kind", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 1 {
			panic("weir.kind expects one value")
		}
		kind, err := b.kind(state.Get(1))
		if err != nil {
			panic(err)
		}
		state.Set(0, vm.NewString(kind))
		return 1
	}))
	module.SetString("data", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 1 || !state.Get(1).IsUserdata() {
			panic("weir.data expects bytes or an extended value")
		}
		leaf, ok := state.Get(1).AsUserdata().Data.(scalarLeaf)
		if !ok {
			panic("weir.data expects bytes or an extended value")
		}
		switch leaf.value.Kind {
		case value.Bytes:
			state.Set(0, vm.NewString(string(leaf.value.Data)))
			return 1
		case value.Extended:
			state.Set(0, vm.NewString(leaf.value.Type))
			state.Set(1, vm.NewString(string(leaf.value.Data)))
			return 2
		default:
			panic("weir.data expects bytes or an extended value")
		}
	}))
}

func (b *bridge) container(state *vm.VM, kind value.Kind) int {
	if state.ArgCount() != 0 {
		panic("weir container constructors expect no arguments")
	}
	table := vm.NewEmptyTable()
	info := tableInfo{kind: kind}
	b.tables[table] = info
	state.Set(0, vm.NewTable(table))
	return 1
}

func (b *bridge) wrapLeaf(v value.Value) vm.Value {
	leaf := scalarLeaf{value: v}
	return vm.NewUserdataValue(leaf, b.leafMeta)
}

type encodeFrame struct {
	source *value.Value
	table  *vm.Table
	info   tableInfo
	index  int
}

func (b *bridge) checkpoint() error {
	if b.state != nil {
		return b.state.CheckInterrupt()
	}
	return b.ctx.Err()
}

// Convert the validated tree with an explicit stack so document depth does not
// consume the Go call stack. Hints stay attached to their original table slots.
func (b *bridge) toLua(v value.Value) (vm.Value, error) {
	if v.Kind != value.Object && v.Kind != value.Array {
		converted, _, err := b.encodeChild(v)
		return converted, err
	}
	table := vm.NewTableWithSize(len(v.Items), len(v.Fields))
	info := tableInfo{kind: v.Kind}
	first := encodeFrame{source: &v, table: table, info: info}
	stack := []encodeFrame{first}
	for len(stack) != 0 {
		if err := b.checkpoint(); err != nil {
			return vm.Nil, err
		}
		frame := &stack[len(stack)-1]
		if frame.index == len(frame.source.Fields)+len(frame.source.Items) {
			b.tables[frame.table] = frame.info
			stack = stack[:len(stack)-1]
			continue
		}
		var source *value.Value
		var key vm.Value
		if frame.source.Kind == value.Object {
			field := &frame.source.Fields[frame.index]
			source = &field.Value
			key = vm.NewString(field.Name)
		} else {
			source = &frame.source.Items[frame.index]
			key = vm.NewInt(int64(frame.index + 1))
			if source.Kind == value.Missing {
				return vm.Nil, fmt.Errorf("array contains missing value")
			}
		}
		frame.index++
		if source.Kind == value.Object || source.Kind == value.Array {
			child := vm.NewTableWithSize(len(source.Items), len(source.Fields))
			if err := frame.table.Set(key, vm.NewTable(child)); err != nil {
				return vm.Nil, err
			}
			info := tableInfo{kind: source.Kind}
			next := encodeFrame{source: source, table: child, info: info}
			stack = append(stack, next)
			continue
		}
		converted, hint, err := b.encodeChild(*source)
		if err != nil {
			return vm.Nil, err
		}
		if err := frame.table.Set(key, converted); err != nil {
			return vm.Nil, err
		}
		if hint.Kind != value.Missing {
			if frame.info.hints == nil {
				frame.info.hints = make(map[vm.Value]value.Value)
			}
			frame.info.hints[key] = hint
		}
	}
	return vm.NewTable(table), nil
}

func (b *bridge) encodeScalar(v value.Value) (vm.Value, error) {
	if err := b.checkpoint(); err != nil {
		return vm.Nil, err
	}
	switch v.Kind {
	case value.Missing:
		return vm.Nil, nil
	case value.Null:
		return b.null, nil
	case value.Bool:
		return vm.NewBool(v.Boolean), nil
	case value.Int32, value.Int64:
		return vm.NewInt(v.Integer), nil
	case value.Float64:
		return vm.NewFloat(v.Float), nil
	case value.String:
		return vm.NewString(v.Text), nil
	case value.Bytes, value.Extended:
		v.Data = append([]byte(nil), v.Data...)
		return b.wrapLeaf(v), nil
	default:
		return vm.Nil, fmt.Errorf("unsupported value kind")
	}
}

func (b *bridge) encodeChild(v value.Value) (vm.Value, value.Value, error) {
	var hint value.Value
	if v.Kind == value.Int32 {
		hint = v
	}
	if value.IsJSONNumber(v) {
		if number, ok := nativeJSONNumber(string(v.Data)); ok {
			if b.state != nil {
				if err := b.state.CheckInterrupt(); err != nil {
					return vm.Nil, hint, err
				}
			}
			hint = v
			hint.Data = append([]byte(nil), v.Data...)
			return vm.NewFloat(number), hint, nil
		}
	}
	converted, err := b.encodeScalar(v)
	return converted, hint, err
}

// A JSON number is natural Lua float only when float64's shortest decimal
// representation preserves its decimal value. Source spelling stays attached
// to its table slot; moving or changing the number gives a normal Lua float.
func nativeJSONNumber(text string) (float64, bool) {
	number, err := strconv.ParseFloat(text, 64)
	if err != nil || math.IsInf(number, 0) || math.IsNaN(number) {
		return 0, false
	}
	formatted := strconv.FormatFloat(number, 'g', -1, 64)
	digits, exponent, negative, ok := decimalParts(text)
	if !ok {
		return 0, false
	}
	otherDigits, otherExponent, otherNegative, ok := decimalParts(formatted)
	return number, ok && digits == otherDigits && exponent == otherExponent && negative == otherNegative
}

func decimalParts(text string) (string, int, bool, bool) {
	negative := strings.HasPrefix(text, "-")
	text = strings.TrimPrefix(text, "-")
	mantissa, power, hasPower := strings.Cut(strings.ToLower(text), "e")
	whole, fraction, _ := strings.Cut(mantissa, ".")
	digits := strings.TrimLeft(whole+fraction, "0")
	if digits == "" {
		return "0", 0, negative, true
	}
	exponent := 0
	if hasPower {
		var err error
		exponent, err = strconv.Atoi(power)
		if err != nil {
			return "", 0, false, false
		}
	}
	exponent -= len(fraction)
	trimmed := strings.TrimRight(digits, "0")
	exponent += len(digits) - len(trimmed)
	return trimmed, exponent, negative, true
}

func (b *bridge) kind(v vm.Value) (string, error) {
	if v.IsUserdata() {
		leaf, ok := v.AsUserdata().Data.(scalarLeaf)
		if !ok || v.AsUserdata().Metatable() != b.leafMeta {
			return "", fmt.Errorf("unsupported userdata")
		}
		switch leaf.value.Kind {
		case value.Null:
			return "null", nil
		case value.Bytes:
			return "bytes", nil
		case value.Extended:
			return "extended", nil
		}
	}
	if v.IsTable() {
		converted, err := b.fromLua(v)
		if err != nil {
			return "", err
		}
		if converted.Kind == value.Array {
			return "array", nil
		}
		return "object", nil
	}
	switch {
	case v.IsNil():
		return "missing", nil
	case v.IsBool():
		return "bool", nil
	case v.IsInt():
		return "i64", nil
	case v.IsFloat():
		return "f64", nil
	case v.IsString():
		return "string", nil
	default:
		return "", fmt.Errorf("unsupported Lua value")
	}
}
