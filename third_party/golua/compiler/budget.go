package compiler

import (
	"errors"
	"fmt"
	"hash/maphash"
	"math"
	"unsafe"

	"github.com/iceisfun/golua/budget"
)

// Compiler recursion enters a guarded method for each retained Go frame.
// The largest release-target method frame is below 1.25 KiB; this payload
// covers its stack-doubling copies and the bounded helpers before the next guard.
const methodStackBytes = 8 << 10

// Reserve the first callee prologue and fixed nonrecursive helper tails before
// entering compiler methods. Arbitrary external caller stacks are outside this
// library's accounting; Weir bounds its entry path separately.
const initialStackBytes = 32 << 10

// compilerMap is used by the compiler's four append-only lookup indexes.
// Its backing array, including rehash peaks, is reserved before make. Runtime
// Go maps cannot expose that allocation boundary to the evaluation owner.
type compilerMap[K comparable, V any] struct {
	memory *budget.Budget
	seed   maphash.Seed
	items  []compilerMapEntry[K, V]
	count  int
}

type compilerMapEntry[K comparable, V any] struct {
	hash  uint64
	key   K
	value V
	used  bool
}

func newCompilerMap[K comparable, V any](memory *budget.Budget) compilerMap[K, V] {
	result := compilerMap[K, V]{memory: memory, seed: maphash.MakeSeed()}
	return result
}

func (m *compilerMap[K, V]) slot(key K, hash uint64) (int, bool) {
	if len(m.items) == 0 {
		return 0, false
	}
	mask := uint64(len(m.items) - 1)
	index := int(hash & mask)
	for probes := 0; ; probes++ {
		if probes&1023 == 0 {
			m.memory.Require(0)
		}
		entry := &m.items[index]
		if !entry.used {
			return index, false
		}
		if entry.hash == hash && entry.key == key {
			return index, true
		}
		index = (index + 1) & int(mask)
	}
}

func (m *compilerMap[K, V]) get(key K) (V, bool) {
	m.memory.Require(0)
	hash := maphash.Comparable(m.seed, key)
	index, exists := m.slot(key, hash)
	if exists {
		return m.items[index].value, true
	}
	var empty V
	return empty, false
}

func (m *compilerMap[K, V]) set(key K, value V) {
	m.memory.Require(0)
	hash := maphash.Comparable(m.seed, key)
	index, exists := m.slot(key, hash)
	if exists {
		m.items[index].value = value
		return
	}
	if m.count >= len(m.items)/2 {
		m.grow()
		index, _ = m.slot(key, hash)
	}
	entry := compilerMapEntry[K, V]{hash: hash, key: key, value: value, used: true}
	m.items[index] = entry
	m.count++
}

func (m *compilerMap[K, V]) grow() {
	if len(m.items) > math.MaxInt/2 {
		m.memory.Require(-1)
		panic("compiler map capacity overflow")
	}
	capacity := max(8, 2*len(m.items))
	var entry compilerMapEntry[K, V]
	m.memory.RequireArray(unsafe.Sizeof(entry), capacity)
	items := make([]compilerMapEntry[K, V], capacity)
	previous := m.items
	m.items = items
	for _, item := range previous {
		if item.used {
			index, _ := m.slot(item.key, item.hash)
			m.items[index] = item
		}
	}
}

func requireArray[T any](memory *budget.Budget, count int) {
	var item T
	memory.RequireArray(unsafe.Sizeof(item), count)
}

func (c *compiler) enterBudget() {
	c.memory.Require(0)
	c.budgetDepth++
	if c.budgetDepth > c.maxBudgetDepth {
		// Credit from the caller and initial reserve covers this prologue;
		// pay for deeper callees before entering them, retaining peak credit.
		c.memory.RequireAllocation(methodStackBytes)
		c.maxBudgetDepth = c.budgetDepth
	}
}

func (c *compiler) leaveBudget() { c.budgetDepth-- }

func (c *compiler) format(format string, args ...any) string {
	if c.memory != nil {
		c.memory.RequireAllocation(len(format) + 256)
		for _, arg := range args {
			if text, ok := arg.(string); ok {
				for range 8 {
					c.memory.RequireAllocation(len(text))
				}
			} else {
				c.memory.RequireAllocation(64)
			}
		}
	}
	return fmt.Sprintf(format, args...)
}

func (c *compiler) newError(format string, args ...any) error {
	message := c.format(format, args...)
	c.memory.RequireAllocation(32)
	return errors.New(message)
}
