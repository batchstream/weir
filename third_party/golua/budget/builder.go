package budget

import (
	"math"
	"unicode/utf8"
	"unsafe"
)

// Builder uses explicitly prepaid capacities. String shares the completed
// backing bytes in the same way as strings.Builder.String.
type Builder struct {
	budget *Budget
	data   []byte
}

func NewBuilder(b *Budget) *Builder {
	var empty Builder
	b.RequireArray(unsafe.Sizeof(empty), 1)
	result := &Builder{budget: b}
	return result
}

func (b *Builder) Grow(additional int) {
	if additional < 0 || additional > math.MaxInt-len(b.data) {
		if b.budget != nil {
			b.budget.fail(errAllocationOverflow)
		}
		panic(errAllocationOverflow)
	}
	needed := len(b.data) + additional
	if needed <= cap(b.data) {
		return
	}
	capacity := max(16, cap(b.data))
	if capacity <= math.MaxInt/2 {
		capacity *= 2
	}
	capacity = max(capacity, needed)
	b.budget.RequireArray(1, capacity)
	next := make([]byte, len(b.data), capacity)
	copy(next, b.data)
	b.data = next
}

func (b *Builder) WriteByte(value byte) error {
	b.Grow(1)
	b.data = append(b.data, value)
	return nil
}

func (b *Builder) WriteRune(value rune) (int, error) {
	var encoded [utf8.UTFMax]byte
	length := utf8.EncodeRune(encoded[:], value)
	b.Grow(length)
	b.data = append(b.data, encoded[:length]...)
	return length, nil
}

func (b *Builder) WriteString(value string) (int, error) {
	b.Grow(len(value))
	b.data = append(b.data, value...)
	return len(value), nil
}

func (b *Builder) Write(value []byte) (int, error) {
	b.Grow(len(value))
	b.data = append(b.data, value...)
	return len(value), nil
}

func (b *Builder) Len() int { return len(b.data) }
func (b *Builder) Cap() int { return cap(b.data) }
func (b *Builder) Reset()   { b.data = nil }
func (b *Builder) String() string {
	return unsafe.String(unsafe.SliceData(b.data), len(b.data))
}
