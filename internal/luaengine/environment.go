package luaengine

import (
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/vm"
)

func installEnvironment(state *vm.VM, observedAt time.Time) *vm.Table {
	scope := state.Budget()
	// Native boxes and their captured values are fixed per fresh environment.
	scope.RequireArray(128, len(standardLibraries.strings)+len(standardLibraries.tables)+12)
	for _, entry := range standardLibraries.globals {
		state.SetGlobal(entry.name, entry.value)
	}
	stringLibrary := copyLibrary(state, standardLibraries.strings)
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
			count := function(state)
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
			return count
		}))
	}
	stringLibrary.SetString("format", vm.NewNativeFunc(func(state *vm.VM) int {
		return luaFormat(state, format)
	}))
	state.SetGlobal("string", vm.NewTable(stringLibrary))
	stringMeta := copyLibrary(state, standardLibraries.stringMeta)
	stringMeta.SetString(vm.MetaIndex, vm.NewTable(stringLibrary))
	state.SetStringMeta(stringMeta)
	unicodeLibrary := state.NewEmptyTable()
	unicodeLibrary.SetString("upper", vm.NewNativeFunc(func(state *vm.VM) int {
		return luaUnicodeCase(state, true)
	}))
	unicodeLibrary.SetString("lower", vm.NewNativeFunc(func(state *vm.VM) int {
		return luaUnicodeCase(state, false)
	}))
	state.SetGlobal("utf8", vm.NewTable(unicodeLibrary))

	tableLibrary := copyLibrary(state, standardLibraries.tables)
	for _, name := range []string{"insert", "remove"} {
		function := tableLibrary.GetString(name).AsNativeFunc()
		tableLibrary.SetString(name, vm.NewNativeFunc(func(state *vm.VM) int {
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
			if !state.Get(1).IsTable() {
				panic("table operation requires an array")
			}
			return function(state)
		}))
	}
	tableLibrary.SetString("sort", vm.NewNativeFunc(luaSort))
	tableLibrary.SetString("concat", vm.NewNativeFunc(luaConcat))
	state.SetGlobal("table", vm.NewTable(tableLibrary))
	mathLibrary := copyLibrary(state, standardLibraries.math)
	state.SetGlobal("math", vm.NewTable(mathLibrary))
	module := state.NewEmptyTable()
	module.SetString("keep", vm.NewNativeFunc(luaKeep))
	module.SetString("delete", vm.NewNativeFunc(luaDelete))
	module.SetString("reject", vm.NewNativeFunc(luaReject))
	timeLibrary := state.NewEmptyTable()
	scope.RequireAllocation(64)
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

// Formatting individual directives provides cancellation checkpoints while
// preserving the standard library's format syntax and argument semantics.
func luaFormat(state *vm.VM, format vm.Value) int {
	if !state.Get(1).IsString() {
		panic("string.format requires a format string")
	}
	source := state.Get(1).AsString()
	scope := state.Budget()
	var argumentValue vm.Value
	scope.RequireArray(unsafe.Sizeof(argumentValue), state.ArgCount()-1)
	arguments := make([]vm.Value, state.ArgCount()-1)
	for index := range arguments {
		arguments[index] = state.Get(index + 2)
	}
	output := budget.NewBuilder(scope)
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
			output.WriteString(source[start:position])
			continue
		}
		position++
		if position < len(source) && source[position] == '%' {
			output.WriteString("%")
			position++
			continue
		}
		for position < len(source) && strings.ContainsRune("#0- +.0123456789", rune(source[position])) {
			position++
		}
		if position == len(source) || argument == len(arguments) {
			panic("incomplete format or missing argument")
		}
		position++
		input := arguments[argument]
		scope.RequireArray(unsafe.Sizeof(argumentValue), 2)
		callArguments := []vm.Value{vm.NewString(source[start:position]), input}
		parts, err := state.ProtectedCall(format, callArguments)
		if err != nil {
			panic(err)
		}
		output.WriteString(parts[0].AsString())
		argument++
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}

func luaConcat(state *vm.VM) int {
	if !state.Get(1).IsTable() {
		panic("table.concat requires an array")
	}
	items := state.Get(1).AsTable()
	separator := ""
	if !state.Get(2).IsNil() {
		if !state.Get(2).IsString() && !state.Get(2).IsNumber() {
			panic("table.concat separator must be a string or number")
		}
		if state.Get(2).IsNumber() {
			state.Budget().RequireAllocation(64)
		}
		separator = state.Get(2).String()
	}
	first, last := int64(1), int64(items.Len())
	if !state.Get(3).IsNil() {
		var ok bool
		first, ok = state.ToInt(state.Get(3))
		if !ok {
			panic("table.concat start must be an integer")
		}
	}
	if !state.Get(4).IsNil() {
		var ok bool
		last, ok = state.ToInt(state.Get(4))
		if !ok {
			panic("table.concat end must be an integer")
		}
	}
	output := budget.NewBuilder(state.Budget())
	for index := first; index <= last; index++ {
		if err := state.CheckInterrupt(); err != nil {
			panic(err)
		}
		item := items.Get(vm.NewInt(index))
		if !item.IsString() && !item.IsNumber() {
			panic("table.concat items must be strings or numbers")
		}
		if index != first {
			output.WriteString(separator)
		}
		if item.IsNumber() {
			state.Budget().RequireAllocation(64)
		}
		output.WriteString(item.String())
		if index == last {
			break
		}
	}
	state.Set(0, vm.NewString(output.String()))
	return 1
}

func luaSort(state *vm.VM) int {
	if !state.Get(1).IsTable() {
		panic("table.sort requires an array")
	}
	items := state.Get(1).AsTable()
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
	var argument vm.Value
	s.state.Budget().RequireArray(unsafe.Sizeof(argument), 2)
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
