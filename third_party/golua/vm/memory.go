package vm

import (
	"math"
	"sync"
	"unsafe"

	"github.com/iceisfun/golua/budget"
	"github.com/iceisfun/golua/compiler"
)

const callStackBytes = 32 << 10

func WithBudget(owner *budget.Budget) VMOption {
	// Weir constructs this option for each evaluation, before New allocates
	// its VM object. The remaining variable storage is allocated after options.
	var instance VM
	owner.RequireArray(unsafe.Sizeof(instance), 1)
	// Credit the first New/Run/ProtectedCall prologue before entering the VM.
	// Further credit follows maximum logical depth, including native frames.
	owner.RequireArray(callStackBytes, 1)
	return func(state *VM) {
		state.budget = owner
		state.budgetCallDepth = 1
	}
}

func (vm *VM) Budget() *budget.Budget { return vm.budget }

func (vm *VM) initializeMemory() {
	vm.stack = makeSlice[Value](vm.budget, initialStackSize, initialStackSize)
	vm.callStack = makeSlice[callFrame](vm.budget, 0, 32)
	vm.globals = vm.NewEmptyTable()
	vm.budget.RequireArray(unsafe.Sizeof(int32(0)), 1)
	vm.closeDepth = new(int32)
	var queue gcQueue
	vm.budget.RequireArray(unsafe.Sizeof(queue), 1)
	vm.gcQueue = &gcQueue{}
	vm.internalState = makeMap[string, any](vm.budget, 0)
	vm.budget.RequireAllocation(64)
	vm.internalMu = new(sync.Mutex)
}

func (vm *VM) reserveCallStack() {
	if vm.budget == nil {
		return
	}
	depth := vm.callDepthBase + len(vm.callStack) + 1
	if depth > vm.budgetCallDepth {
		// This covers Go-stack doubling and the interpreter/native call
		// chain, separately from the explicit Lua register/call-frame arrays.
		vm.budget.RequireArray(callStackBytes, depth-vm.budgetCallDepth)
		vm.budgetCallDepth = depth
	}
}

// Every closure or native entry uses this ownership boundary before invoking
// its callee. Native pcall/xpcall can recurse without a closure/checkCallDepth.
func (vm *VM) pushCallFrame(frame callFrame) {
	vm.reserveCallStack()
	vm.callStack = budget.Append(vm.budget, vm.callStack, frame)
}

// Quota termination skips Lua handlers and discards VM-owned references without
// running allocating cleanup code. Its scope is still owned by the caller.
func (vm *VM) discardMemory() {
	vm.stack = nil
	vm.callStack = nil
	vm.openUpvalues = nil
	vm.tbcVars = nil
	vm.globals = nil
	vm.stringMeta = nil
	vm.numberMeta = nil
	vm.boolMeta = nil
	vm.nilMeta = nil
	vm.functionMeta = nil
	vm.threadMeta = nil
	vm.lightUserdataMeta = nil
	vm.registry = nil
	vm.threadObj = Nil
	vm.msgHandler = Nil
	vm.msgHandlerResult = Nil
	vm.lastErrorCallStack = nil
	vm.hookFunc = Nil
	vm.internalState = nil
	vm.closeHooks = nil
	vm.registeredProviders = nil
	vm.userProtectedBases = nil
	vm.retBuf = [8]Value{}
	vm.outputLines = nil
	if vm.gcQueue != nil {
		vm.gcQueue.pending = nil
	}
}

func makeSlice[T any](owner *budget.Budget, length, capacity int) []T {
	var item T
	owner.RequireArray(unsafe.Sizeof(item), capacity)
	return make([]T, length, capacity)
}

// Go maps allocate groups and can retain old groups during growth. Charging a
// new key before insertion covers its slot, group slack, directory and growth
// copy. Updating an existing key does not consume another reservation.
const mapEntryAllocation = 512

func makeMap[K comparable, V any](owner *budget.Budget, capacity int) map[K]V {
	owner.RequireArray(mapEntryAllocation, capacity)
	return make(map[K]V, capacity)
}

func (vm *VM) NewEmptyTable() *Table {
	return vm.NewTableWithSize(0, 0)
}

func (vm *VM) NewTableWithSize(narray, nhash int) *Table {
	var emptyTable Table
	vm.budget.RequireArray(unsafe.Sizeof(emptyTable), 1)
	table := &Table{budget: vm.budget}
	if narray > 0 {
		table.array = makeSlice[Value](vm.budget, 0, narray)
	}
	if nhash > 0 {
		table.keys = makeSlice[Value](vm.budget, 0, nhash)
	}
	return table
}

func (vm *VM) NewUserdataValue(data interface{}, metatable LuaTable) Value {
	var emptyUserdata Userdata
	var emptyValue Value
	vm.budget.RequireArray(unsafe.Sizeof(emptyUserdata)+unsafe.Sizeof(emptyValue), 1)
	result := NewUserdataValue(data, metatable)
	return result
}

func (vm *VM) newClosure(proto *compiler.Proto) *Closure {
	var emptyClosure Closure
	vm.budget.RequireArray(unsafe.Sizeof(emptyClosure), 1)
	if len(proto.Upvalues) > closureInlineUpvalues {
		vm.budget.RequireArray(unsafe.Sizeof((*Upvalue)(nil)), len(proto.Upvalues))
	}
	closure := NewClosure(proto)
	closure.budget = vm.budget
	return closure
}

func (vm *VM) prepayFormat(format string, arguments []any) {
	if vm.budget == nil {
		return
	}
	vm.budget.RequireAllocation(checkedBytes(vm.budget, len(format), 4))
	for _, argument := range arguments {
		if text, ok := argument.(string); ok {
			vm.budget.RequireAllocation(checkedBytes(vm.budget, len(text), 4))
		}
	}
}

func checkedBytes(owner *budget.Budget, bytes, factor int) int {
	if bytes < 0 || factor < 0 || factor != 0 && bytes > (math.MaxInt-64)/factor {
		owner.Require(-1)
		panic("Lua allocation size overflow")
	}
	return bytes*factor + 64
}
