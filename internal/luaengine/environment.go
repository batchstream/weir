package luaengine

import (
	"sort"
	"strings"
	"time"

	"github.com/batchstream/weir/internal/value"
	"github.com/iceisfun/golua/vm"
)

func installEnvironment(state *vm.VM, observedAt time.Time, limits value.Limits) *vm.Table {
	for _, entry := range standardLibraries.globals {
		state.SetGlobal(entry.name, entry.value)
	}
	stringLibrary := copyLibrary(standardLibraries.strings)
	format := stringLibrary.GetString("format")
	for _, entry := range standardLibraries.strings {
		name := entry.name
		if name == "format" {
			continue
		}
		function := entry.value.AsNativeFunc()
		stringLibrary.SetString(name, vm.NewNativeFunc(func(state *vm.VM) int {
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
			for index := 1; index <= state.ArgCount(); index++ {
				argument := state.Get(index)
				if argument.IsString() && len(argument.AsString()) > limits.MaxBytes {
					panic("string argument exceeds the value byte limit")
				}
			}
			count := function(state)
			if state.Get(0).IsString() && len(state.Get(0).AsString()) > limits.MaxBytes {
				panic("string result exceeds the value byte limit")
			}
			return count
		}))
	}
	stringLibrary.SetString("format", vm.NewNativeFunc(func(state *vm.VM) int {
		return luaFormat(state, format, limits)
	}))
	state.SetGlobal("string", vm.NewTable(stringLibrary))
	stringMeta := copyLibrary(standardLibraries.stringMeta)
	stringMeta.SetString(vm.MetaIndex, vm.NewTable(stringLibrary))
	state.SetStringMeta(stringMeta)

	tableLibrary := copyLibrary(standardLibraries.tables)
	for _, name := range []string{"insert", "remove"} {
		function := tableLibrary.GetString(name).AsNativeFunc()
		tableLibrary.SetString(name, vm.NewNativeFunc(func(state *vm.VM) int {
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
			if !state.Get(1).IsTable() || state.Get(1).AsTable().Len() > limits.MaxNodes {
				panic("table operation requires an array within the value node limit")
			}
			return function(state)
		}))
	}
	tableLibrary.SetString("sort", vm.NewNativeFunc(func(state *vm.VM) int { return luaSort(state, limits) }))
	tableLibrary.SetString("concat", vm.NewNativeFunc(func(state *vm.VM) int { return luaConcat(state, limits) }))
	state.SetGlobal("table", vm.NewTable(tableLibrary))
	mathLibrary := copyLibrary(standardLibraries.math)
	state.SetGlobal("math", vm.NewTable(mathLibrary))
	module := vm.NewEmptyTable()
	module.SetString("keep", vm.NewNativeFunc(luaKeep))
	module.SetString("delete", vm.NewNativeFunc(luaDelete))
	module.SetString("reject", vm.NewNativeFunc(luaReject))
	timeLibrary := vm.NewEmptyTable()
	stamp := observedAt.UTC().Format(time.RFC3339Nano)
	timeLibrary.SetString("now", vm.NewNativeFunc(func(state *vm.VM) int {
		if state.ArgCount() != 0 {
			panic("weir.time.now takes no arguments")
		}
		state.Set(0, vm.NewString(stamp))
		return 1
	}))
	module.SetString("time", vm.NewTable(timeLibrary))
	state.SetGlobal("weir", vm.NewTable(module))
	return module
}

// Format each directive separately so repeated arguments cannot build an
// oversized result before the cumulative output bound is checked.
func luaFormat(state *vm.VM, format vm.Value, limits value.Limits) int {
	if !state.Get(1).IsString() {
		panic("string.format requires a format string")
	}
	source := state.Get(1).AsString()
	if len(source) > limits.MaxBytes {
		panic("format string exceeds the value byte limit")
	}
	arguments := make([]vm.Value, state.ArgCount()-1)
	for index := range arguments {
		arguments[index] = state.Get(index + 2)
	}
	var output strings.Builder
	argument := 0
	for position := 0; position < len(source); {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		start := position
		if source[position] != '%' {
			end := strings.IndexByte(source[position:], '%')
			if end < 0 {
				end = len(source) - position
			}
			position += end
			appendLuaText(&output, source[start:position], limits)
			continue
		}
		position++
		if position < len(source) && source[position] == '%' {
			appendLuaText(&output, "%", limits)
			position++
			continue
		}
		for position < len(source) && strings.ContainsRune("#0- +.0123456789", rune(source[position])) {
			position++
			if position-start > 32 {
				panic("invalid format directive")
			}
		}
		if position == len(source) || argument == len(arguments) {
			panic("incomplete format or missing argument")
		}
		position++
		input := arguments[argument]
		if input.IsString() && len(input.AsString()) > limits.MaxBytes {
			panic("format argument exceeds the value byte limit")
		}
		callArguments := []vm.Value{vm.NewString(source[start:position]), input}
		parts, err := state.ProtectedCall(format, callArguments)
		if err != nil {
			panic(err)
		}
		appendLuaText(&output, parts[0].AsString(), limits)
		argument++
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}

func appendLuaText(output *strings.Builder, text string, limits value.Limits) {
	if len(text) > limits.MaxBytes-output.Len() {
		panic("string result exceeds the value byte limit")
	}
	output.WriteString(text)
}

func luaConcat(state *vm.VM, limits value.Limits) int {
	if !state.Get(1).IsTable() {
		panic("table.concat requires an array")
	}
	items := state.Get(1).AsTable()
	separator := ""
	if !state.Get(2).IsNil() {
		if !state.Get(2).IsString() && !state.Get(2).IsNumber() {
			panic("table.concat separator must be a string or number")
		}
		separator = state.Get(2).String()
	}
	first, last := int64(1), int64(items.Len())
	if !state.Get(3).IsNil() {
		var ok bool
		first, ok = state.Get(3).ToInt()
		if !ok {
			panic("table.concat start must be an integer")
		}
	}
	if !state.Get(4).IsNil() {
		var ok bool
		last, ok = state.Get(4).ToInt()
		if !ok {
			panic("table.concat end must be an integer")
		}
	}
	if first <= last && uint64(last)-uint64(first) >= uint64(limits.MaxNodes) {
		panic("table.concat range exceeds the value node limit")
	}
	var output strings.Builder
	for index := first; index <= last; index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		item := items.Get(vm.NewInt(index))
		if !item.IsString() && !item.IsNumber() {
			panic("table.concat items must be strings or numbers")
		}
		if index != first {
			appendLuaText(&output, separator, limits)
		}
		appendLuaText(&output, item.String(), limits)
		if index == last {
			break
		}
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}

func luaSort(state *vm.VM, limits value.Limits) int {
	if !state.Get(1).IsTable() {
		panic("table.sort requires an array")
	}
	items := state.Get(1).AsTable()
	length := items.Len()
	if length > limits.MaxNodes {
		panic("table.sort exceeds the value node limit")
	}
	compare := state.Get(2)
	if !compare.IsNil() && !compare.IsCallable() {
		panic("table.sort comparator must be a function")
	}
	sorter := luaArraySorter{state: state, items: items, compare: compare}
	sort.Sort(sorter)
	return 0
}

type luaArraySorter struct {
	state   *vm.VM
	items   vm.LuaTable
	compare vm.Value
}

func (s luaArraySorter) Len() int {
	return s.items.Len()
}

func (s luaArraySorter) Less(left, right int) bool {
	if err := s.state.CheckInterrupt(); err != nil {
		panic(err)
	}
	a := s.items.Get(vm.NewInt(int64(left + 1)))
	b := s.items.Get(vm.NewInt(int64(right + 1)))
	if s.compare.IsNil() {
		less, err := s.state.CompareLT(a, b)
		if err != nil {
			panic(err)
		}
		return less
	}
	arguments := []vm.Value{a, b}
	results, err := s.state.ProtectedCall(s.compare, arguments)
	if err != nil {
		panic(err)
	}
	return len(results) != 0 && results[0].ToBool()
}

func (s luaArraySorter) Swap(left, right int) {
	a, b := vm.NewInt(int64(left+1)), vm.NewInt(int64(right+1))
	leftValue, rightValue := s.items.Get(a), s.items.Get(b)
	if err := s.items.Set(a, rightValue); err != nil {
		panic(err)
	}
	if err := s.items.Set(b, leftValue); err != nil {
		panic(err)
	}
}
