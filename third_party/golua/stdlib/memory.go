package stdlib

import (
	"unsafe"

	"github.com/iceisfun/golua/vm"
)

func prepayArguments(v *vm.VM, count int) {
	var argument vm.Value
	v.Budget().RequireArray(unsafe.Sizeof(argument), count)
}

func prepayStringCopies(v *vm.VM, length, copies int) {
	for range copies {
		v.Budget().RequireAllocation(length)
	}
}
