package vm

import (
	"strconv"
	"unsafe"

	"github.com/iceisfun/golua/budget"
)

// strconv clones its input into NumError on failure. Reserve that possible
// allocation before each actual parse, rather than charging unused fallbacks.
func reserveNumericParse(owner *budget.Budget, input string) {
	if owner == nil {
		return
	}
	owner.RequireAllocation(len(input))
	var parseError strconv.NumError
	owner.RequireArray(unsafe.Sizeof(parseError), 1)
}

func parseNumericInt(owner *budget.Budget, input string, base int) (int64, error) {
	reserveNumericParse(owner, input)
	integer, err := strconv.ParseInt(input, base, 64)
	owner.Require(0)
	return integer, err
}

func parseNumericUint(owner *budget.Budget, input string, base int) (uint64, error) {
	reserveNumericParse(owner, input)
	integer, err := strconv.ParseUint(input, base, 64)
	owner.Require(0)
	return integer, err
}

func parseNumericFloat(owner *budget.Budget, input string) (float64, error) {
	reserveNumericParse(owner, input)
	number, err := strconv.ParseFloat(input, 64)
	owner.Require(0)
	return number, err
}

func (vm *VM) ToInt(input Value) (int64, bool) {
	return input.toInt(vm.budget)
}

func (vm *VM) ToNumber(input Value) (float64, bool) {
	return input.toNumber(vm.budget)
}

func (vm *VM) StringToNumericValue(input string) (Value, bool) {
	return stringToNumericValue(vm.budget, input)
}

func (vm *VM) ParseInt(input string, base int) (int64, error) {
	return parseNumericInt(vm.budget, input, base)
}

func (vm *VM) ParseUint(input string, base int) (uint64, error) {
	return parseNumericUint(vm.budget, input, base)
}

func (vm *VM) ParseFloat(input string) (float64, error) {
	return parseNumericFloat(vm.budget, input)
}

func (vm *VM) ParseHexFloat(input string) (float64, bool) {
	return parseHexFloat(vm.budget, input)
}
