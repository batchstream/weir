package luaengine

import (
	"context"
	"errors"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/batchstream/weir/internal/luaworker"
	"github.com/batchstream/weir/internal/value"
	lua "github.com/yuin/gopher-lua"
)

var errInvalidLuaResult = errors.New("invalid Lua result")

func Evaluate(ctx context.Context, program luaworker.Program) (luaworker.Result, error) {
	var empty luaworker.Result
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	options := lua.Options{
		CallStackSize:       128,
		RegistrySize:        128,
		RegistryMaxSize:     4096,
		RegistryGrowStep:    128,
		SkipOpenLibs:        true,
		MinimizeStackMemory: true,
	}
	state := lua.NewState(options)
	defer state.Close()
	state.SetContext(ctx)
	installModule(state)
	pushValue(state, program.Current)
	state.SetGlobal("current", state.Get(-1))
	state.Pop(1)
	pushValue(state, program.Input)
	state.SetGlobal("input", state.Get(-1))
	state.Pop(1)
	if err := state.DoString(program.Source); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	if state.GetTop() == 0 || state.Get(1) == lua.LNil {
		result := luaworker.Result{Action: luaworker.Keep, Value: value.Value{Kind: value.Missing}}
		return result, nil
	}
	if state.GetTop() != 1 {
		return empty, errInvalidLuaResult
	}
	userdata, ok := state.Get(1).(*lua.LUserData)
	if !ok {
		return empty, errInvalidLuaResult
	}
	switch typed := userdata.Value.(type) {
	case value.Value:
		result := luaworker.Result{Action: luaworker.Replace, Value: typed}
		return result, nil
	case luaworker.Result:
		return typed, nil
	default:
		return empty, errInvalidLuaResult
	}
}

func installModule(state *lua.LState) {
	module := state.NewTable()
	module.RawSetString("kind", state.NewFunction(luaKind))
	module.RawSetString("get", state.NewFunction(luaGet))
	module.RawSetString("set", state.NewFunction(luaSet))
	module.RawSetString("merge", state.NewFunction(luaMerge))
	module.RawSetString("object", state.NewFunction(luaObject))
	module.RawSetString("array", state.NewFunction(luaArray))
	module.RawSetString("missing", state.NewFunction(luaMissing))
	module.RawSetString("null", state.NewFunction(luaNull))
	module.RawSetString("bool", state.NewFunction(luaBool))
	module.RawSetString("i32", state.NewFunction(luaInt32))
	module.RawSetString("i64", state.NewFunction(luaInt64))
	module.RawSetString("f64", state.NewFunction(luaFloat64))
	module.RawSetString("string", state.NewFunction(luaString))
	module.RawSetString("bytes", state.NewFunction(luaBytes))
	module.RawSetString("add", state.NewFunction(luaAdd))
	module.RawSetString("sub", state.NewFunction(luaSub))
	module.RawSetString("mul", state.NewFunction(luaMul))
	module.RawSetString("to32", state.NewFunction(luaTo32))
	module.RawSetString("to64", state.NewFunction(luaTo64))
	module.RawSetString("keep", state.NewFunction(luaKeep))
	module.RawSetString("replace", state.NewFunction(luaReplace))
	module.RawSetString("delete", state.NewFunction(luaDelete))
	module.RawSetString("reject", state.NewFunction(luaReject))
	state.SetGlobal("weir", module)
}

func pushValue(state *lua.LState, v value.Value) {
	userdata := state.NewUserData()
	userdata.Value = v
	state.Push(userdata)
}

func pushAction(state *lua.LState, result luaworker.Result) int {
	userdata := state.NewUserData()
	userdata.Value = result
	state.Push(userdata)
	return 1
}

func argumentValue(state *lua.LState, index int) value.Value {
	userdata, ok := state.Get(index).(*lua.LUserData)
	if !ok {
		state.ArgError(index, "typed value required")
	}
	v, ok := userdata.Value.(value.Value)
	if !ok {
		state.ArgError(index, "typed value required")
	}
	return v
}

func argumentString(state *lua.LState, index int) string {
	text, ok := state.Get(index).(lua.LString)
	if !ok {
		state.ArgError(index, "string required")
	}
	return string(text)
}

func pushCheckedValue(state *lua.LState, v value.Value) int {
	if err := value.Validate(v); err != nil {
		state.RaiseError("invalid typed value")
	}
	pushValue(state, v)
	return 1
}

func luaKind(state *lua.LState) int {
	if state.GetTop() != 1 {
		state.RaiseError("kind expects one argument")
	}
	v := argumentValue(state, 1)
	names := [...]string{"missing", "null", "bool", "int32", "int64", "float64", "string", "bytes", "array", "object", "extended"}
	if int(v.Kind) >= len(names) {
		state.RaiseError("invalid typed value")
	}
	state.Push(lua.LString(names[v.Kind]))
	return 1
}

func luaGet(state *lua.LState) int {
	if state.GetTop() != 2 {
		state.RaiseError("get expects two arguments")
	}
	object := argumentValue(state, 1)
	name := argumentString(state, 2)
	child, err := object.Lookup(name)
	if err != nil {
		state.RaiseError("field lookup failed")
	}
	cloned, err := value.Clone(child)
	if err != nil {
		state.RaiseError("field lookup failed")
	}
	return pushCheckedValue(state, cloned)
}

func luaSet(state *lua.LState) int {
	if state.GetTop() != 3 {
		state.RaiseError("set expects three arguments")
	}
	object := argumentValue(state, 1)
	name := argumentString(state, 2)
	child := argumentValue(state, 3)
	if object.Kind != value.Object {
		state.RaiseError("set requires an object")
	}
	if len(name) == 0 || !utf8.ValidString(name) || len(name) > 1024 {
		state.RaiseError("invalid field name")
	}
	cloned, err := value.Clone(object)
	if err != nil {
		state.RaiseError("set rejected")
	}
	child, err = value.Clone(child)
	if err != nil {
		state.RaiseError("set rejected")
	}
	found := false
	for i := range cloned.Fields {
		if cloned.Fields[i].Name != name {
			continue
		}
		found = true
		if child.Kind == value.Missing {
			cloned.Fields = append(cloned.Fields[:i], cloned.Fields[i+1:]...)
		} else {
			cloned.Fields[i].Value = child
		}
		break
	}
	if !found && child.Kind != value.Missing {
		field := value.Field{Name: name, Value: child}
		cloned.Fields = append(cloned.Fields, field)
	}
	return pushCheckedValue(state, cloned)
}

func luaMerge(state *lua.LState) int {
	if state.GetTop() != 2 {
		state.RaiseError("merge expects two objects")
	}
	merged, err := value.Merge(argumentValue(state, 1), argumentValue(state, 2))
	if err != nil {
		state.RaiseError("merge rejected")
	}
	return pushCheckedValue(state, merged)
}

func luaObject(state *lua.LState) int {
	if state.GetTop()%2 != 0 || state.GetTop() > value.MaxNodes*2 {
		state.RaiseError("object expects key/value pairs")
	}
	object := value.Value{Kind: value.Object}
	for i := 1; i <= state.GetTop(); i += 2 {
		name := argumentString(state, i)
		field := value.Field{Name: name, Value: argumentValue(state, i+1)}
		object.Fields = append(object.Fields, field)
	}
	return pushCheckedValue(state, object)
}

func luaArray(state *lua.LState) int {
	if state.GetTop() > value.MaxNodes {
		state.RaiseError("array exceeds limit")
	}
	array := value.Value{Kind: value.Array}
	for i := 1; i <= state.GetTop(); i++ {
		array.Items = append(array.Items, argumentValue(state, i))
	}
	return pushCheckedValue(state, array)
}

func luaMissing(state *lua.LState) int {
	if state.GetTop() != 0 {
		state.RaiseError("missing takes no arguments")
	}
	v := value.Value{Kind: value.Missing}
	return pushCheckedValue(state, v)
}

func luaNull(state *lua.LState) int {
	if state.GetTop() != 0 {
		state.RaiseError("null takes no arguments")
	}
	v := value.Value{Kind: value.Null}
	return pushCheckedValue(state, v)
}

func luaBool(state *lua.LState) int {
	boolean, ok := state.Get(1).(lua.LBool)
	if !ok || state.GetTop() != 1 {
		state.RaiseError("bool requires one boolean")
	}
	v := value.Value{Kind: value.Bool, Boolean: bool(boolean)}
	return pushCheckedValue(state, v)
}

func luaInt32(state *lua.LState) int {
	n, err := strconv.ParseInt(argumentString(state, 1), 10, 32)
	if err != nil || state.GetTop() != 1 {
		state.RaiseError("invalid int32")
	}
	v, err := value.Integer(value.Int32, n)
	if err != nil {
		state.RaiseError("invalid int32")
	}
	return pushCheckedValue(state, v)
}

func luaInt64(state *lua.LState) int {
	n, err := strconv.ParseInt(argumentString(state, 1), 10, 64)
	if err != nil || state.GetTop() != 1 {
		state.RaiseError("invalid int64")
	}
	v, err := value.Integer(value.Int64, n)
	if err != nil {
		state.RaiseError("invalid int64")
	}
	return pushCheckedValue(state, v)
}

func luaFloat64(state *lua.LState) int {
	n, err := strconv.ParseFloat(argumentString(state, 1), 64)
	if err != nil || math.IsInf(n, 0) || math.IsNaN(n) || state.GetTop() != 1 {
		state.RaiseError("invalid float64")
	}
	v := value.Value{Kind: value.Float64, Float: n}
	return pushCheckedValue(state, v)
}

func luaString(state *lua.LState) int {
	text := argumentString(state, 1)
	if state.GetTop() != 1 {
		state.RaiseError("string requires one argument")
	}
	v := value.Value{Kind: value.String, Text: text}
	return pushCheckedValue(state, v)
}

func luaBytes(state *lua.LState) int {
	text := argumentString(state, 1)
	if state.GetTop() != 1 {
		state.RaiseError("bytes requires one argument")
	}
	v := value.Value{Kind: value.Bytes, Data: []byte(text)}
	return pushCheckedValue(state, v)
}

func luaAdd(state *lua.LState) int {
	return luaArithmetic(state, '+')
}

func luaSub(state *lua.LState) int {
	return luaArithmetic(state, '-')
}

func luaMul(state *lua.LState) int {
	return luaArithmetic(state, '*')
}

func luaArithmetic(state *lua.LState, op byte) int {
	if state.GetTop() != 2 {
		state.RaiseError("integer operation expects two arguments")
	}
	result, err := value.Arithmetic(argumentValue(state, 1), argumentValue(state, 2), op)
	if err != nil {
		state.RaiseError("integer operation failed")
	}
	return pushCheckedValue(state, result)
}

func luaTo32(state *lua.LState) int {
	if state.GetTop() != 1 {
		state.RaiseError("to32 expects one argument")
	}
	result, err := value.Convert(argumentValue(state, 1), value.Int32)
	if err != nil {
		state.RaiseError("int32 conversion failed")
	}
	return pushCheckedValue(state, result)
}

func luaTo64(state *lua.LState) int {
	if state.GetTop() != 1 {
		state.RaiseError("to64 expects one argument")
	}
	result, err := value.Convert(argumentValue(state, 1), value.Int64)
	if err != nil {
		state.RaiseError("int64 conversion failed")
	}
	return pushCheckedValue(state, result)
}

func luaKeep(state *lua.LState) int {
	if state.GetTop() != 0 {
		state.RaiseError("keep takes no arguments")
	}
	result := luaworker.Result{Action: luaworker.Keep, Value: value.Value{Kind: value.Missing}}
	return pushAction(state, result)
}

func luaReplace(state *lua.LState) int {
	if state.GetTop() != 1 {
		state.RaiseError("replace expects one object")
	}
	result := luaworker.Result{Action: luaworker.Replace, Value: argumentValue(state, 1)}
	return pushAction(state, result)
}

func luaDelete(state *lua.LState) int {
	if state.GetTop() != 0 {
		state.RaiseError("delete takes no arguments")
	}
	result := luaworker.Result{Action: luaworker.Delete, Value: value.Value{Kind: value.Missing}}
	return pushAction(state, result)
}

func luaReject(state *lua.LState) int {
	if state.GetTop() != 1 {
		state.RaiseError("reject expects one message")
	}
	message := argumentString(state, 1)
	if len(message) > luaworker.MaxMessageBytes || !utf8.ValidString(message) {
		state.RaiseError("invalid rejection message")
	}
	result := luaworker.Result{Action: luaworker.Reject, Value: value.Value{Kind: value.Missing}, Message: message}
	return pushAction(state, result)
}
