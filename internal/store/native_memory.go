package store

import (
	"sync"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
)

const nativeCountLimit = 1024
const defaultNativeIOReserve = 48_000_000 + 2*protocol.NativeChunk + 8192

var errNativeMemory = execution.ErrNativeMemory

const (
	nativeWaiting uint8 = iota
	nativeStarting
	nativeSettled
	nativeTerminal
	nativeBorrowed
)

// NativeMemory belongs to one Node, rather than to any individual Store. A
// reservation covers queued inputs, initial backend work, retained responses,
// and publication. No reservation is released while its owner still holds data.
type NativeMemory struct {
	mu         sync.Mutex
	budget     uint64
	reserve    uint64
	used       uint64
	resident   uint64
	borrowed   int
	io         uint64
	known      bool
	trusted    bool
	rejections uint64
	leases     map[*nativeLease]struct{}
	wakes      map[chan struct{}]struct{}
}

type NativeMemorySnapshot struct {
	BudgetBytes, UsedBytes, IOReserveBytes uint64
	ResidentBytes                          uint64
	Count, CountLimit                      int
	Starting, Settled, Waiting, Terminal   int
	Borrowed                               int
	Known                                  bool
	BudgetRejections                       uint64
}

type nativeLease struct {
	memory                 *NativeMemory
	input, working, window int
	extra                  int
	state                  uint8
	resident               uint64
	preparing              bool
}

func NewNativeMemory() *NativeMemory {
	memory := &NativeMemory{reserve: defaultNativeIOReserve, leases: make(map[*nativeLease]struct{}), wakes: make(map[chan struct{}]struct{})}
	return memory
}

// ObserveMemory receives the existing Guard sample. Unknown samples retain the
// last trusted bound. A shrink never pretends that already owned memory vanished.
func (m *NativeMemory) ObserveMemory(snapshot overload.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.known = snapshot.RecoveryAllowanceKnown && !snapshot.Unknown
	if m.known {
		m.budget = snapshot.RecoveryAllowance/3*2 + snapshot.RecoveryAllowance%3*2/3
		m.trusted = true
	}
	m.signalLocked()
}

func (m *NativeMemory) Snapshot() NativeMemorySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := NativeMemorySnapshot{BudgetBytes: m.budget, UsedBytes: m.used, ResidentBytes: m.resident, IOReserveBytes: m.reserve, Count: len(m.leases), CountLimit: nativeCountLimit, Known: m.known, BudgetRejections: m.rejections}
	for lease := range m.leases {
		switch lease.state {
		case nativeWaiting:
			snapshot.Waiting++
		case nativeStarting:
			snapshot.Starting++
		case nativeSettled:
			snapshot.Settled++
		case nativeTerminal:
			snapshot.Terminal++
		case nativeBorrowed:
			snapshot.Borrowed++
		}
	}
	return snapshot
}

func (m *NativeMemory) register(wake chan struct{}) {
	m.mu.Lock()
	m.wakes[wake] = struct{}{}
	m.mu.Unlock()
}

func (m *NativeMemory) unregister(wake chan struct{}) {
	m.mu.Lock()
	delete(m.wakes, wake)
	m.mu.Unlock()
}

func (m *NativeMemory) signalLocked() {
	for wake := range m.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (m *NativeMemory) residentLimitLocked() uint64 {
	return m.budget - min(m.budget, m.reserve)
}

func (m *NativeMemory) admit(plan *execution.Plan) (*nativeLease, *pb.Failure) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.trusted {
		m.rejections++
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "Native memory capacity is unknown")
	}
	window := max(plan.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes)
	bytes := uint64(plan.Bytes) + uint64(window)
	limit := m.residentLimitLocked()
	if len(m.leases) >= nativeCountLimit || m.used > m.budget || bytes > m.budget-m.used || m.resident > limit || bytes > limit-m.resident {
		m.rejections++
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, errNativeMemory.Error())
	}
	lease := &nativeLease{memory: m, input: plan.Bytes, window: window, resident: bytes}
	m.leases[lease] = struct{}{}
	m.used += bytes
	m.resident += bytes
	return lease, nil
}

// start is called before any command I/O. Existing initial invocations may
// release conservative estimates as replies arrive; wait for that convergence
// in the bounded input queue. Settled slow outputs never hold backend slots.
func (l *nativeLease) start(working, reserve int) (bool, bool) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reserve = max(m.reserve, uint64(reserve))
	if m.used <= m.budget && uint64(working) <= m.budget-m.used {
		l.working = working
		l.state = nativeStarting
		l.preparing = true
		m.used += uint64(working)
		return true, false
	}
	if m.convergingLocked() {
		return false, true
	}
	m.rejections++
	return false, false
}

func (l *nativeLease) grow(working int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if working <= l.working {
		return nil
	}
	delta := uint64(working - l.working)
	if m.used > m.budget || delta > m.budget-m.used {
		m.rejections++
		return errNativeMemory
	}
	m.used += delta
	l.working = working
	return nil
}

func (l *nativeLease) beginRead(extra int) (bool, bool) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.used > m.budget || uint64(extra) > m.budget-m.used {
		if m.convergingLocked() {
			return false, true
		}
		// Waiting outputs cannot release their response working sets by first
		// reading more body bytes. Fail this attempted exchange instead of
		// cyclically waiting for memory that no current IO can relinquish.
		m.rejections++
		return false, false
	}
	m.used += uint64(extra)
	m.io += uint64(extra)
	l.extra = extra
	return true, false
}

func (m *NativeMemory) convergingLocked() bool {
	if m.io != 0 {
		return true
	}
	for lease := range m.leases {
		if lease.preparing {
			return true
		}
	}
	return false
}

func (l *nativeLease) settle(working, eventBytes int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	window := max(l.window, 2*eventBytes+execution.ResultOverheadBytes)
	previous := uint64(l.working) + uint64(l.window) + uint64(l.extra)
	next := uint64(working) + uint64(window)
	if next > previous && (m.used > m.budget || next-previous > m.budget-m.used) {
		m.rejections++
		return errNativeMemory
	}
	m.used = m.used - previous + next
	m.io -= uint64(l.extra)
	changed := next != previous || l.preparing
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	l.working, l.window, l.extra, l.preparing = working, window, 0, false
	total := uint64(l.input) + next
	otherResident := m.resident - l.resident
	limit := m.residentLimitLocked()
	if otherResident <= limit && total <= limit-otherResident {
		m.resident = otherResident + total
		l.resident = total
		l.state = nativeSettled
	} else {
		// The original IO reservation already owns this response. Preserve it while
		// publishing, rather than killing a fast consumer or pretending it is IO.
		base := uint64(l.input) + uint64(l.window)
		l.resident = min(l.resident, base)
		m.resident = otherResident + l.resident
		l.state = nativeBorrowed
		m.borrowed++
	}
	m.promoteLocked()
	if changed {
		m.signalLocked()
	}
	return nil
}

func (l *nativeLease) terminal(eventBytes int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := uint64(l.working) + uint64(l.window) + uint64(l.extra)
	// The largest encoded emission can remain transport-owned until publication
	// finishes. Keep its bounded window, while discarding the cleaned reply peak.
	window := max(l.window, 2*eventBytes+execution.ResultOverheadBytes)
	if uint64(window) > previous && (m.used > m.budget || uint64(window)-previous > m.budget-m.used) {
		m.rejections++
		return errNativeMemory
	}
	l.window = window
	m.used = m.used - previous + uint64(window)
	m.io -= uint64(l.extra)
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	l.working, l.extra, l.state, l.preparing = 0, 0, nativeTerminal, false
	otherResident := m.resident - l.resident
	total := uint64(l.input) + uint64(l.window)
	limit := m.residentLimitLocked()
	if otherResident <= limit && total <= limit-otherResident {
		l.resident = total
	} else {
		l.resident = min(l.resident, total)
	}
	m.resident = otherResident + l.resident
	m.promoteLocked()
	m.signalLocked()
	return nil
}

func (l *nativeLease) release() {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, live := m.leases[l]; !live {
		return
	}
	delete(m.leases, l)
	m.resident -= l.resident
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	m.used -= uint64(l.input) + uint64(l.working) + uint64(l.window) + uint64(l.extra)
	m.io -= uint64(l.extra)
	m.promoteLocked()
	m.signalLocked()
}

// Credit classification follows actual held ownership; promotion never changes
// UsedBytes or grants memory that has not really been released.
func (m *NativeMemory) promoteLocked() {
	if m.borrowed == 0 {
		return
	}
	limit := m.residentLimitLocked()
	for l := range m.leases {
		if l.state != nativeBorrowed {
			continue
		}
		total := uint64(l.input) + uint64(l.window) + uint64(l.working)
		delta := total - l.resident
		if m.resident <= limit && delta <= limit-m.resident {
			m.resident += delta
			l.resident = total
			l.state = nativeSettled
			m.borrowed--
		}
	}
}
