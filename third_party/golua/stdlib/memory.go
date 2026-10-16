package stdlib

import (
	"unsafe"

	"github.com/iceisfun/golua/vm"
)

func prepayNumeric(v *vm.VM, input vm.Value) {
	if input.IsString() {
		// strconv may retain a copied input in NumError for each attempted
		// integer/float parse. The lower-case token check is another copy.
		for range 4 {
			v.Budget().RequireAllocation(len(input.AsString()))
		}
	}
}

func prepayArguments(v *vm.VM, count int) {
	var argument vm.Value
	v.Budget().RequireArray(unsafe.Sizeof(argument), count)
}

func prepayStringCopies(v *vm.VM, length, copies int) {
	for range copies {
		v.Budget().RequireAllocation(length)
	}
}
