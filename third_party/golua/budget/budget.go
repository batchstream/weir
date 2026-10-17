// Package budget prepays allocations owned by one Lua evaluation.
// Bytes remain charged for the scope's lifetime; this is an allocation budget,
// rather than a claim to measure the Go collector's live heap.
package budget

import (
	"context"
	"errors"
	"math"
	"unsafe"
)

var errAllocationOverflow = errors.New("Lua allocation size overflow")

type Options struct {
	Context context.Context
	Reserve func(totalBytes int) error
}

type Budget struct {
	ctx     context.Context
	reserve func(int) error
	used    int
	err     error
}

// ExhaustedError terminates the entire evaluation, including Lua pcall/xpcall.
// Cause retains either caller cancellation or the shared allocator's refusal.
type ExhaustedError struct {
	Cause error
}

func (e *ExhaustedError) Error() string { return e.Cause.Error() }
func (e *ExhaustedError) Unwrap() error { return e.Cause }

func New(options Options) *Budget {
	ctx := options.Context
	if ctx == nil {
		ctx = context.Background()
	}
	var empty Budget
	initial := int(unsafe.Sizeof(empty))
	initial += initial/4 + 64
	if err := ctx.Err(); err != nil {
		failure := &ExhaustedError{Cause: err}
		panic(failure)
	}
	if options.Reserve != nil {
		if err := options.Reserve(initial); err != nil {
			failure := &ExhaustedError{Cause: err}
			panic(failure)
		}
	}
	b := &Budget{ctx: ctx, reserve: options.Reserve, used: initial}
	return b
}

// Require reserves a new allocation before it is made. Reusing existing
// capacity must not call Require. The reservation callback receives the scope's
// full cumulative allocation, allowing it to use the existing Node ledger.
func (b *Budget) Require(bytes int) {
	if b == nil {
		return
	}
	if err := b.Check(); err != nil {
		panic(err)
	}
	if bytes < 0 || bytes > math.MaxInt-b.used {
		b.fail(errAllocationOverflow)
	}
	if bytes == 0 {
		return
	}
	next := b.used + bytes
	if b.reserve != nil {
		if err := b.reserve(next); err != nil {
			b.fail(err)
		}
	}
	b.used = next
}

// RequireAllocation bounds the physical allocation, including Go 1.27 size
// classes and large-object page rounding. Above 256 bytes adjacent small size
// classes differ by at most 25%; +64 covers the smaller classes and pointer
// headers. Above 32 KiB, 25% covers the 8 KiB page-rounding excess.
func (b *Budget) RequireAllocation(bytes int) {
	if b == nil {
		return
	}
	if bytes < 0 {
		b.fail(errAllocationOverflow)
	}
	if bytes == 0 {
		b.Require(0)
		return
	}
	extra := bytes/4 + 64
	if bytes > math.MaxInt-extra {
		b.fail(errAllocationOverflow)
	}
	b.Require(bytes + extra)
}

// RequireArray checks multiplication, then prepays the physical backing block.
// Zero-sized backing storage does not allocate and consumes no byte credit.
func (b *Budget) RequireArray(elementBytes uintptr, count int) {
	if b == nil {
		return
	}
	if count < 0 || uint64(elementBytes) > uint64(math.MaxInt) ||
		elementBytes != 0 && uint64(count) > uint64(math.MaxInt)/uint64(elementBytes) {
		b.fail(errAllocationOverflow)
	}
	b.RequireAllocation(int(elementBytes) * count)
}

func (b *Budget) fail(cause error) {
	if b.err != nil {
		panic(b.err)
	}
	failure := &ExhaustedError{Cause: cause}
	b.err = failure
	panic(failure)
}

// Check observes cancellation and an earlier refusal without clearing either.
func (b *Budget) Check() error {
	if b == nil {
		return nil
	}
	if b.err != nil {
		return b.err
	}
	if err := b.ctx.Err(); err != nil {
		failure := &ExhaustedError{Cause: err}
		b.err = failure
	}
	return b.err
}

func (b *Budget) Err() error {
	if b == nil {
		return nil
	}
	return b.err
}

func (b *Budget) UsedBytes() int {
	if b == nil {
		return 0
	}
	return b.used
}

func IsExhausted(value any) bool {
	err, ok := value.(error)
	if !ok {
		return false
	}
	var failure *ExhaustedError
	return errors.As(err, &failure)
}

// Recover is only for a Go API's outer boundary. Lua's protected-call handlers
// must rethrow ExhaustedError without invoking a Lua error handler.
func Recover(err *error) {
	if failure := recover(); failure != nil {
		if IsExhausted(failure) {
			*err = failure.(error)
			return
		}
		panic(failure)
	}
}

// Append controls the new backing capacity so its complete allocation can be
// reserved before make. Existing capacity, including overlapping copies, uses
// the ordinary append operation without another charge.
func Append[T any](b *Budget, dst []T, values ...T) []T {
	if b == nil || len(values) <= cap(dst)-len(dst) {
		return append(dst, values...)
	}
	if len(values) > math.MaxInt-len(dst) {
		b.fail(errAllocationOverflow)
	}
	needed := len(dst) + len(values)
	capacity := cap(dst)
	if capacity < 4 {
		capacity = 4
	}
	if capacity <= math.MaxInt/2 {
		capacity *= 2
	}
	capacity = max(capacity, needed)
	var element T
	b.RequireArray(unsafe.Sizeof(element), capacity)
	next := make([]T, len(dst), capacity)
	copy(next, dst)
	return append(next, values...)
}
