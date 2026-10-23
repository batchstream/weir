package luaengine

import (
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/iceisfun/golua/vm"
)

// Simple Unicode casing follows Go's strings.ToUpper/ToLower semantics and
// checkpoints long native calls within the VM.
func luaUnicodeCase(state *vm.VM, upper bool) int {
	if state.ArgCount() != 1 || !state.Get(1).IsString() {
		panic("Unicode casing requires one UTF-8 string")
	}
	text := state.Get(1).AsString()
	var result strings.Builder
	result.Grow(len(text))
	checkpoint := 0
	for offset := 0; offset < len(text); {
		if offset >= checkpoint {
			if err := state.CheckInterrupt(); err != nil {
				panic(err)
			}
			checkpoint = offset + 4096
		}
		character, width := utf8.DecodeRuneInString(text[offset:])
		if character == utf8.RuneError && width == 1 {
			panic("Unicode casing requires valid UTF-8")
		}
		if upper {
			character = unicode.ToUpper(character)
		} else {
			character = unicode.ToLower(character)
		}
		result.WriteRune(character)
		offset += width
	}
	if err := state.CheckInterrupt(); err != nil {
		panic(err)
	}
	state.Set(0, vm.NewString(result.String()))
	return 1
}
