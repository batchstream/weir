package luaengine

import (
	"context"

	"github.com/iceisfun/golua/stdlib"
	"github.com/iceisfun/golua/vm"
)

type libraryEntry struct {
	name  string
	value vm.Value
}

type libraries struct {
	globals, strings, tables, math, stringMeta []libraryEntry
}

var standardLibraries = loadLibraries()

func loadLibraries() libraries {
	state := vm.New()
	defer state.Close(context.Background())
	stdlib.Open(state)
	baseNames := []string{
		"assert", "error", "type", "tostring", "tonumber", "pairs", "ipairs", "next",
		"pcall", "xpcall", "select", "rawequal", "rawlen", "_VERSION",
	}
	stringNames := []string{"len", "sub", "upper", "lower", "reverse", "byte", "char", "format"}
	tableNames := []string{"insert", "remove", "sort", "concat"}
	mathNames := []string{
		"abs", "acos", "asin", "atan", "atan2", "ceil", "cos", "cosh", "deg", "exp", "floor",
		"fmod", "frexp", "ldexp", "log", "log10", "max", "min", "modf", "pow", "rad",
		"sin", "sinh", "sqrt", "tan", "tanh", "tointeger", "type", "ult",
		"pi", "huge", "maxinteger", "mininteger",
	}
	metaNames := []string{
		vm.MetaAdd, vm.MetaSub, vm.MetaMul, vm.MetaDiv, vm.MetaIDiv, vm.MetaMod, vm.MetaPow, vm.MetaUnm,
	}
	// These functions use their supplied VM and carry no library or VM state.
	// Keep only immutable function/scalar values; no template table is exposed.
	result := libraries{
		globals:    selectLibrary(state.Globals().(*vm.Table), baseNames),
		strings:    selectLibrary(state.GetGlobal("string").AsTable().(*vm.Table), stringNames),
		tables:     selectLibrary(state.GetGlobal("table").AsTable().(*vm.Table), tableNames),
		math:       selectLibrary(state.GetGlobal("math").AsTable().(*vm.Table), mathNames),
		stringMeta: selectLibrary(state.StringMeta().(*vm.Table), metaNames),
	}
	return result
}

func selectLibrary(table *vm.Table, names []string) []libraryEntry {
	entries := make([]libraryEntry, len(names))
	for index, name := range names {
		entry := libraryEntry{name: name, value: table.GetString(name)}
		entries[index] = entry
	}
	return entries
}

func copyLibrary(entries []libraryEntry) *vm.Table {
	table := vm.NewTableWithSize(0, len(entries))
	for _, entry := range entries {
		table.SetString(entry.name, entry.value)
	}
	return table
}
