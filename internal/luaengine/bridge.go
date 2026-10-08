package luaengine

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"
	"strings"
	"unicode/utf8"

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
	limits   value.Limits
	state    *vm.VM
	tables   map[*vm.Table]tableInfo
	leafMeta *vm.Table
	null     vm.Value
}

func newBridge(limits value.Limits) *bridge {
	b := &bridge{limits: limits, tables: make(map[*vm.Table]tableInfo)}
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
		if len(text) > b.limits.MaxBytes {
			panic("value byte limit")
		}
		leaf := value.Value{Kind: value.Bytes, Data: []byte(text)}
		state.Set(0, b.wrapLeaf(leaf))
		return 1
	}))
	module.SetString("extended", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 2 || !state.Get(1).IsString() || !state.Get(2).IsString() {
			panic("weir.extended expects a type and byte string")
		}
		typeName, data := state.Get(1).AsString(), state.Get(2).AsString()
		if len(typeName) > b.limits.MaxBytes-len(data) {
			panic("value byte limit")
		}
		leaf := value.Value{Kind: value.Extended, Type: typeName, Data: []byte(data)}
		if err := value.Validate(leaf, b.limits); err != nil {
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

// toLua converts values already checked by ValidateProgram.
func (b *bridge) toLua(v value.Value) (vm.Value, error) {
	if b.state != nil {
		if err := b.state.CheckInterrupt(); err != nil {
			return vm.Nil, err
		}
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
	case value.Object, value.Array:
		table := vm.NewTableWithSize(len(v.Items), len(v.Fields))
		info := tableInfo{kind: v.Kind}
		for _, field := range v.Fields {
			child, hint, err := b.encodeChild(field.Value)
			if err != nil {
				return vm.Nil, err
			}
			table.SetString(field.Name, child)
			if hint.Kind != value.Missing {
				if info.hints == nil {
					info.hints = make(map[vm.Value]value.Value)
				}
				info.hints[vm.NewString(field.Name)] = hint
			}
		}
		for index, item := range v.Items {
			if item.Kind == value.Missing {
				return vm.Nil, fmt.Errorf("array contains missing value")
			}
			child, hint, err := b.encodeChild(item)
			if err != nil {
				return vm.Nil, err
			}
			table.SetInt(index+1, child)
			if hint.Kind != value.Missing {
				if info.hints == nil {
					info.hints = make(map[vm.Value]value.Value)
				}
				info.hints[vm.NewInt(int64(index+1))] = hint
			}
		}
		b.tables[table] = info
		return vm.NewTable(table), nil
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
	converted, err := b.toLua(v)
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
		if err != nil || exponent < -value.MaxBytes || exponent > value.MaxBytes {
			return "", 0, false, false
		}
	}
	exponent -= len(fraction)
	trimmed := strings.TrimRight(digits, "0")
	exponent += len(digits) - len(trimmed)
	return trimmed, exponent, negative, true
}

type bridgeBudget struct {
	nodes  int
	bytes  int
	active map[*vm.Table]bool
}

func (b *bridge) fromLua(v vm.Value) (value.Value, error) {
	used := bridgeBudget{active: make(map[*vm.Table]bool)}
	var hint value.Value
	return b.decode(v, 0, hint, &used)
}

func (b *bridge) decode(v vm.Value, depth int, hint value.Value, used *bridgeBudget) (value.Value, error) {
	var empty value.Value
	if b.state != nil {
		if err := b.state.CheckInterrupt(); err != nil {
			return empty, err
		}
	}
	used.nodes++
	if depth > b.limits.MaxDepth || used.nodes > b.limits.MaxNodes {
		return empty, fmt.Errorf("value depth or node limit")
	}
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
		result = value.Value{Kind: value.String, Text: text}
	case v.IsUserdata():
		leaf, ok := v.AsUserdata().Data.(scalarLeaf)
		if !ok || v.AsUserdata().Metatable() != b.leafMeta {
			return empty, fmt.Errorf("unsupported userdata")
		}
		result = leaf.value
	case v.IsTable():
		return b.decodeTable(v, depth, used)
	default:
		return empty, fmt.Errorf("unsupported Lua value")
	}
	used.bytes += len(result.Text) + len(result.Type) + len(result.Data)
	if used.bytes > b.limits.MaxBytes {
		return empty, fmt.Errorf("value byte limit")
	}
	result.Data = append([]byte(nil), result.Data...)
	return result, nil
}

func (b *bridge) decodeTable(v vm.Value, depth int, used *bridgeBudget) (value.Value, error) {
	var empty value.Value
	table, ok := v.AsTable().(*vm.Table)
	if !ok || table.IsThread() {
		return empty, fmt.Errorf("unsupported Lua table")
	}
	if used.active[table] {
		return empty, fmt.Errorf("cyclic Lua table")
	}
	used.active[table] = true
	defer delete(used.active, table)
	info := b.tables[table]
	var keys []vm.Value
	var err error
	stringsOnly, integersOnly := true, true
	table.ForEach(func(key, child vm.Value) bool {
		if len(keys) >= b.limits.MaxNodes-used.nodes {
			err = fmt.Errorf("value node limit")
			return false
		}
		stringsOnly = stringsOnly && key.IsString()
		integersOnly = integersOnly && key.IsInt() && key.AsInt() > 0
		keys = append(keys, key)
		return true
	})
	if err != nil {
		return empty, err
	}
	kind := info.kind
	if kind == value.Missing {
		switch {
		case len(keys) == 0 || stringsOnly:
			kind = value.Object
		case integersOnly:
			kind = value.Array
		default:
			return empty, fmt.Errorf("mixed or unsupported Lua table keys")
		}
	}
	result := value.Value{Kind: kind}
	if kind == value.Object {
		if !stringsOnly {
			return empty, fmt.Errorf("object keys must be strings")
		}
		for _, key := range keys {
			name := key.AsString()
			if !utf8.ValidString(name) || strings.IndexByte(name, 0) >= 0 {
				return empty, fmt.Errorf("invalid field name")
			}
			used.bytes += len(name)
			if used.bytes > b.limits.MaxBytes {
				return empty, fmt.Errorf("value byte limit")
			}
			child, err := b.decode(table.Get(key), depth+1, info.hints[key], used)
			if err != nil {
				return empty, err
			}
			field := value.Field{Name: name, Value: child}
			result.Fields = append(result.Fields, field)
		}
		return result, nil
	}
	if !integersOnly {
		return empty, fmt.Errorf("array keys must be positive integers")
	}
	for _, key := range keys {
		if key.AsInt() > int64(len(keys)) {
			return empty, fmt.Errorf("sparse Lua array")
		}
	}
	for index := 1; index <= len(keys); index++ {
		key := vm.NewInt(int64(index))
		child, err := b.decode(table.GetInt(index), depth+1, info.hints[key], used)
		if err != nil {
			return empty, err
		}
		result.Items = append(result.Items, child)
	}
	return result, nil
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
