package store

import (
	"context"
	"sync"

	"github.com/batchstream/weir/internal/execution"
)

// IngressMemory owns native receive bytes before the Store can inspect a
// request. Its promise and actual DATA owners use the Node's execution ledger.
// The connection reader never waits; only the individual message reader may.
type IngressMemory struct {
	memory                                *ExecutionMemory
	ctx                                   context.Context
	fixed, raw, decoded, message, charged uint64
	decodeExtra                           uint64
	buffers                               int
	closed, retired                       bool
	wake                                  chan struct{}
}

func (m *ExecutionMemory) NewIngress(ctx context.Context, fixed int) (*IngressMemory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fixed < 0 || !m.ingressFitsLocked(uint64(fixed), false) {
		return nil, execution.ErrExecutionMemory
	}
	account := &IngressMemory{memory: m, ctx: ctx, fixed: uint64(fixed), charged: uint64(fixed), wake: make(chan struct{}, 1)}
	m.used += account.charged
	m.resident += account.charged
	m.ingressReserved += account.charged
	m.wakes[account.wake] = struct{}{}
	return account, nil
}

func (m *ExecutionMemory) ingressFitsLocked(additional uint64, large bool) bool {
	limit := m.residentLimitLocked()
	// Keep a bounded 1MiB inside the existing resident budget for fresh short
	// requests, cancellation and capacity responses while large uploads wait.
	if large {
		limit -= min(limit, uint64(1<<20))
	}
	return m.trusted && m.used <= m.budget && additional <= m.budget-m.used && m.resident <= limit && additional <= limit-m.resident
}

func (a *IngressMemory) resizeLocked(message, raw, decoded uint64, large bool) bool {
	m := a.memory
	next := a.fixed + raw + decoded
	if message != 0 {
		length := (message - 4096) / 3
		next = a.fixed + max(length, raw) + 2*length + 4096 + a.decodeExtra + decoded
	}
	if next > a.charged {
		delta := next - a.charged
		if !m.ingressFitsLocked(delta, large) {
			return false
		}
		m.used += delta
		m.resident += delta
		m.ingressReserved += delta
	} else {
		delta := a.charged - next
		m.used -= delta
		m.resident -= delta
		m.ingressReserved -= delta
		if delta > 0 {
			m.signalLocked()
		}
	}
	a.message, a.raw, a.decoded, a.charged = message, raw, decoded, next
	return true
}

func (a *IngressMemory) ReserveMessage(length int) error {
	if length < 0 {
		return execution.ErrExecutionMemory
	}
	m := a.memory
	// Raw backing storage, protobuf materialization and decoded field copies
	// coexist. Fixed protobuf envelopes are validated before allocation.
	target := uint64(length)*3 + 4096
	m.mu.Lock()
	if a.message != 0 || a.closed {
		m.mu.Unlock()
		return execution.ErrExecutionMemory
	}
	ctx := a.ctx
	m.ingressWaiting++
	defer func() { m.mu.Lock(); m.ingressWaiting--; m.mu.Unlock() }()
	for {
		if err := ctx.Err(); err != nil {
			m.mu.Unlock()
			return err
		}
		if a.closed {
			m.mu.Unlock()
			return execution.ErrExecutionMemory
		}
		limit := m.residentLimitLocked()
		if length > 64<<10 {
			limit -= min(limit, uint64(1<<20))
		}
		if a.fixed > limit || target > limit-a.fixed {
			m.rejections++
			m.mu.Unlock()
			return execution.ErrExecutionMemory
		}
		if a.resizeLocked(target, a.raw, a.decoded, length > 64<<10) {
			m.ingressMessages++
			m.mu.Unlock()
			return nil
		}
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-a.wake:
		}
		m.mu.Lock()
	}
}

func (a *IngressMemory) Buffer(capacity int) (func(), error) {
	if capacity < 0 {
		return nil, execution.ErrExecutionMemory
	}
	m := a.memory
	// Include the tracked Buffer, owner closure, channel/backlog entry and the
	// BufferSlice reference, original pooled Buffer and slice, and worst-case
	// doubled backlog capacity. Small frames cannot hide this fixed overhead.
	bytes := uint64(capacity) + 512
	m.mu.Lock()
	if a.closed || !a.resizeLocked(a.message, a.raw+bytes, a.decoded, false) {
		m.rejections++
		m.mu.Unlock()
		return nil, execution.ErrExecutionMemory
	}
	a.buffers++
	m.ingressBuffers++
	m.ingressBytes += bytes
	m.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			a.buffers--
			m.ingressBuffers--
			m.ingressBytes -= bytes
			a.resizeLocked(a.message, a.raw-bytes, a.decoded, false)
			a.removeLocked()
		})
	}
	return release, nil
}

// Decoded retains the decoded request after gRPC drops its raw/materialization
// references. The returned count is released only on execution handoff or after
// the handler has returned and discarded the request.
func (a *IngressMemory) Decoded() int {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.message == 0 {
		return 0
	}
	bytes := (a.message-4096)/3 + 4096 + a.decodeExtra
	m.ingressMessages--
	if !a.resizeLocked(0, a.raw, a.decoded+bytes, false) {
		panic("decoded ingress exceeds pre-reserved peak")
	}
	a.decodeExtra = 0
	return int(bytes)
}
func (a *IngressMemory) ReleaseMessage() {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.message != 0 {
		m.ingressMessages--
		a.resizeLocked(0, a.raw, a.decoded, false)
		a.decodeExtra = 0
	}
}
func (a *IngressMemory) ReleaseDecoded(bytes int) {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if bytes < 0 || uint64(bytes) > a.decoded {
		panic("invalid decoded ingress ownership")
	}
	a.resizeLocked(a.message, a.raw, a.decoded-uint64(bytes), false)
}
func (a *IngressMemory) Close() {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.closed {
		return
	}
	a.closed = true
	select {
	case a.wake <- struct{}{}:
	default:
	}
	if a.message != 0 {
		m.ingressMessages--
	}
	if a.retired {
		a.fixed = 0
	}
	a.ctx = context.Background()
	a.resizeLocked(0, a.raw, 0, false)
	a.removeLocked()
	m.signalLocked()
}
func (a *IngressMemory) removeLocked() {
	if a.closed && a.retired && a.buffers == 0 {
		delete(a.memory.wakes, a.wake)
	}
}

// ReserveDecode adjusts a variable repeated-message allocation estimate before
// the codec allocates it. The full raw message is already owned; never wait on
// another partially uploaded message while retaining that raw storage.
func (a *IngressMemory) ReserveDecode(bytes int) error {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if a.message == 0 || bytes < 0 {
		return execution.ErrExecutionMemory
	}
	base := (a.message-4096)/3 + 4096
	extra := uint64(max(bytes, 0)) - min(uint64(max(bytes, 0)), base)
	previous := a.decodeExtra
	a.decodeExtra = extra
	if !a.resizeLocked(a.message, a.raw, a.decoded, false) {
		a.decodeExtra = previous
		m.rejections++
		return execution.ErrExecutionMemory
	}
	return nil
}

// Retire follows native stream deletion. The handler and transport may finish
// in either order; fixed metadata stays charged until both have let it go.
func (a *IngressMemory) Retire() {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	a.retired = true
	if a.closed {
		a.fixed = 0
		a.resizeLocked(a.message, a.raw, a.decoded, false)
	}
	a.removeLocked()
}

// SetContext runs before native dispatch, after tap/deadline processing. Header
// metadata was reserved earlier, before its map allocations were made.
func (a *IngressMemory) SetContext(ctx context.Context) {
	m := a.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if !a.closed {
		a.ctx = ctx
	}
}
