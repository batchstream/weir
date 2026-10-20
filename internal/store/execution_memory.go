package store

import (
	"context"
	"sync"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	pb "github.com/batchstream/weir-protocol/api/weir/v1"
	"github.com/batchstream/weir/internal/execution"
	"github.com/batchstream/weir/internal/overload"
)

const nativeCountLimit = 1024
const defaultNativeIOReserve = 48_000_000 + 2*protocol.NativeChunk + 8192
const nativeQueuedWindow = 2*execution.ResultOverheadBytes + execution.MinimumWireBytes

// Each short handoff can retain two chunks of response/framing workspace and
// two publication chunks. It is an actual nonresident-byte bound, not a limit
// on otherwise resident documents or metadata.
const executionBorrowWindow = 4*protocol.NativeChunk + execution.ResultOverheadBytes
const executionBorrowLimit = 2
const executionProgressReserve = executionBorrowLimit*executionBorrowWindow + 2*protocol.NativeChunk

// Unused queue promises and large receive promises both leave entry space for
// bounded short RPCs. This is unused capacity in the same Node ledger, never a
// second pool or a revocable allowance for already held inputs and buffers.
const ingressProgressReserve = 1 << 20

var errExecutionMemory = execution.ErrExecutionMemory

const (
	nativeWaiting uint8 = iota
	nativeStarting
	nativeSettled
	nativeTerminal
	nativeBorrowed
)

// ExecutionMemory belongs to one Node, rather than to any individual Store. A
// reservation covers queued inputs, initial backend work, retained responses,
// and publication. No reservation is released while its owner still holds data.
type ExecutionMemory struct {
	mu                                              sync.Mutex
	budget                                          uint64
	reserve                                         uint64
	used                                            uint64
	resident                                        uint64
	ingressConnectionBytes                          uint64
	ingressConnections                              int
	ingressBytes, ingressReserved                   uint64
	ingressBuffers, ingressMessages, ingressWaiting int
	inputReserved                                   uint64
	inputUsed                                       uint64
	wireBytes                                       uint64
	wireCount                                       int
	nativeCount                                     int
	borrowed                                        int
	io                                              uint64
	known                                           bool
	trusted                                         bool
	rejections                                      uint64
	leases                                          map[*executionLease]struct{}
	wakes                                           map[chan struct{}]struct{}
	ingressHandoffs                                 map[*execution.Plan]*IngressInput
	workingWaits                                    []*workingWait
	observations                                    executionMemoryObservations
}

// A wait is a scheduling claim, never ledger credit. Its donor identities are
// captured once; later Native starts and retained outputs cannot extend it.
type workingWait struct {
	donors map[*executionLease]struct{}
	wake   chan struct{}
	since  time.Time
}

type ExecutionMemorySnapshot struct {
	BudgetBytes, UsedBytes, IOReserveBytes          uint64
	ResidentBytes                                   uint64
	InputReservedBytes, InputUsedBytes              uint64
	WireBytes                                       uint64
	IngressConnectionBytes                          uint64
	IngressConnections                              int
	IngressBytes, IngressReservedBytes              uint64
	IngressBuffers, IngressMessages, IngressWaiting int
	WireCount                                       int
	NativeCount, NativeCountLimit                   int
	Count, CountLimit                               int
	Starting, Settled, Waiting, Terminal            int
	Borrowed                                        int
	Known                                           bool
	BudgetRejections                                uint64
}

type executionLease struct {
	memory                 *ExecutionMemory
	input, working, window int
	publication            int
	result, wire           int
	released               bool
	native                 bool
	extra                  int
	state                  uint8
	resident               uint64
	preparing              bool
}

func NewExecutionMemory() *ExecutionMemory {
	// Keep two bounded borrowed outputs and a short Native request's two frame
	// copies beside the negotiated response and its publication workspace.
	memory := &ExecutionMemory{reserve: defaultNativeIOReserve + executionProgressReserve, leases: make(map[*executionLease]struct{}), wakes: make(map[chan struct{}]struct{})}
	return memory
}

// ObserveMemory receives the existing Guard sample. Unknown samples retain the
// last trusted bound. A shrink never pretends that already owned memory vanished.
func (m *ExecutionMemory) ObserveMemory(snapshot overload.Snapshot) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.known = snapshot.RecoveryAllowanceKnown && !snapshot.Unknown
	if m.known {
		m.budget = snapshot.RecoveryAllowance/3*2 + snapshot.RecoveryAllowance%3*2/3
		m.trusted = true
		m.resizeInputLocked()
	}
	m.signalLocked()
}

func (m *ExecutionMemory) Snapshot() ExecutionMemorySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	snapshot := ExecutionMemorySnapshot{IngressConnectionBytes: m.ingressConnectionBytes, IngressConnections: m.ingressConnections, IngressBytes: m.ingressBytes, IngressReservedBytes: m.ingressReserved, IngressBuffers: m.ingressBuffers, IngressMessages: m.ingressMessages, IngressWaiting: m.ingressWaiting, BudgetBytes: m.budget, UsedBytes: m.used, ResidentBytes: m.resident, InputReservedBytes: m.inputReserved, InputUsedBytes: m.inputUsed, WireBytes: m.wireBytes, WireCount: m.wireCount, IOReserveBytes: m.reserve, Count: len(m.leases), CountLimit: nativeCountLimit * execution.RecordStreamItems, NativeCount: m.nativeCount, NativeCountLimit: nativeCountLimit, Known: m.known, BudgetRejections: m.rejections}
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

func (m *ExecutionMemory) register(wake chan struct{}) {
	m.mu.Lock()
	m.wakes[wake] = struct{}{}
	m.mu.Unlock()
}

func (m *ExecutionMemory) unregister(wake chan struct{}) {
	m.mu.Lock()
	delete(m.wakes, wake)
	m.mu.Unlock()
}

func (m *ExecutionMemory) signalLocked() {
	for _, wait := range m.workingWaits {
		select {
		case wait.wake <- struct{}{}:
		default:
		}
	}
	for wake := range m.wakes {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func (m *ExecutionMemory) residentLimitLocked() uint64 {
	return m.budget - min(m.budget, m.reserve)
}

func (m *ExecutionMemory) inputTargetLocked() uint64 {
	// Promise queue space before initial IO can consume it. Half of the space
	// above the IO reserve leaves room for backend request/response bookkeeping;
	// the existing input queue policy remains the maximum automatic promise.
	return min(uint64(DefaultLimits().QueueBytes), m.residentLimitLocked()/2)
}

func (m *ExecutionMemory) resizeInputLocked() {
	if len(m.leases) == 0 {
		return
	}
	target := max(m.inputUsed, m.inputTargetLocked())
	if target < m.inputReserved {
		delta := m.inputReserved - target
		m.inputReserved = target
		m.used -= delta
		m.resident -= delta
	} else if len(m.workingWaits) == 0 && target > m.inputReserved && m.used < m.budget && m.resident < m.residentLimitLocked() {
		// New automatic credit leaves short-RPC entry space. Existing input
		// promises remain intact while other resident owners grow; only the
		// trusted-capacity shrink above may revoke their unused portion.
		available := m.residentLimitLocked() - m.resident
		progress := min(uint64(ingressProgressReserve), m.residentLimitLocked()/2)
		available -= min(available, progress)
		delta := min(target-m.inputReserved, m.budget-m.used, available)
		m.inputReserved += delta
		m.used += delta
		m.resident += delta
	}
}

// A lone operation can exchange unused queue promises for its actual first
// execution/result peak. Other inputs and live outputs keep their promises;
// neither real input, working memory nor the protected IO reserve is revoked.
func (l *executionLease) reclaimUnusedInputLocked(additional, resident uint64) bool {
	m := l.memory
	_, owns := m.leases[l]
	if !owns || len(m.leases) != 1 || l.released || l.wire != 0 || l.extra != 0 || l.result != 0 || l.window != nativeQueuedWindow || l.resident != 0 ||
		m.inputReserved < m.inputUsed || m.inputUsed != uint64(l.input)+nativeQueuedWindow || m.resident != m.inputReserved+m.ingressReserved+m.ingressConnectionBytes || m.used < m.inputReserved+m.ingressReserved+m.ingressConnectionBytes || m.used-m.inputReserved-m.ingressReserved-m.ingressConnectionBytes != uint64(l.working) {
		return false
	}
	unused := m.inputReserved - m.inputUsed
	used := m.used - unused
	held := m.resident - unused
	limit := m.residentLimitLocked()
	if used > m.budget || additional > m.budget-used || held > limit || resident > limit-held {
		return false
	}
	// Subtract only after both complete peaks fit, so a failed reservation leaves
	// every promise intact. Safe differences also cover a capacity shrink.
	retained := min(unused, m.budget-used-additional, limit-held-resident)
	revoked := unused - retained
	m.inputReserved -= revoked
	m.used -= revoked
	m.resident -= revoked
	return true
}

func (m *ExecutionMemory) admit(plan *execution.Plan) (*executionLease, *pb.Failure, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.trusted {
		m.observeAdmissionLocked(plan)
		m.rejections++
		return nil, protocol.Fail(pb.FailureCode_UNAVAILABLE, "execution memory capacity is unknown"), false
	}
	publication := nativeQueuedWindow
	if plan.Command.GetNative() != nil {
		publication = max(plan.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes)
	}
	bytes := uint64(plan.Bytes) + nativeQueuedWindow
	input := m.ingressHandoffs[plan]
	credit := uint64(0)
	if input != nil && !input.account.closed {
		credit = input.bytes
	}
	used, resident := m.used-credit, m.resident-credit
	reserved := m.inputReserved
	if len(m.leases) == 0 {
		available := m.residentLimitLocked() - min(resident, m.residentLimitLocked())
		// At very small capacities the existing half-resident queue policy
		// already leaves half for progress; do not consume that entire queue.
		progress := min(uint64(ingressProgressReserve), m.residentLimitLocked()/2)
		automatic := min(m.inputTargetLocked(), m.budget-min(used, m.budget), available-min(available, progress))
		reserved = max(bytes, automatic)
	}
	// A single input larger than the normal queue promise can proceed alone,
	// provided its full initial execution peak genuinely fits the same budget.
	oversized := len(m.leases) == 0 && bytes > m.inputTargetLocked()
	peak := uint64(plan.WorkingBytes) + uint64(publication-nativeQueuedWindow)
	native := plan.Command.GetNative() != nil
	if len(m.leases) >= nativeCountLimit*execution.RecordStreamItems || native && m.nativeCount >= nativeCountLimit || m.budget == 0 || reserved > m.budget || (m.inputReserved == 0 && m.ingressReserved+m.ingressConnectionBytes > 0 && (used > m.budget || reserved > m.budget-used || resident > m.residentLimitLocked() || reserved > m.residentLimitLocked()-resident)) || bytes > reserved-m.inputUsed || (oversized && peak > m.budget-reserved) {
		// A decoded input already has a bounded same-Node owner. Temporary
		// input-credit exhaustion may wait for an existing lease to release its
		// real ownership. An input that cannot run alone, or an unowned source,
		// must still fail promptly instead of creating an external input queue.
		wait := credit != 0 && len(m.leases) != 0 && bytes <= m.residentLimitLocked() && bytes <= m.budget && peak <= m.budget-bytes
		if !wait {
			m.observeAdmissionLocked(plan)
			m.rejections++
		}
		return nil, protocol.Fail(pb.FailureCode_RESOURCE_EXHAUSTED, errExecutionMemory.Error()), wait
	}
	if input != nil {
		input.releaseLocked()
	}
	lease := &executionLease{memory: m, input: plan.Bytes, window: nativeQueuedWindow, publication: publication, native: native}
	m.leases[lease] = struct{}{}
	if native {
		m.nativeCount++
	}
	if m.inputReserved == 0 {
		m.inputReserved = reserved
		m.used += reserved
		m.resident += reserved
	}
	m.inputUsed += bytes
	return lease, nil, false
}

// start is called before any command I/O. Existing initial invocations may
// release conservative estimates as replies arrive; wait for that convergence
// in the bounded input queue. Settled slow outputs never hold backend slots.
func (l *executionLease) start(working, reserve int) (bool, bool) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	ioReserve := uint64(reserve) + executionProgressReserve
	if !l.native {
		ioReserve += 2*protocol.NativeChunk + 8192
	}
	m.reserve = max(m.reserve, ioReserve)
	m.resizeInputLocked()
	additional := uint64(working) + uint64(l.publication-l.window)
	// Existing initial Native invocations finish through GrowIO/EndIO, not
	// start. Do not let fresh speculative execution spend an older growth
	// claimant's next real release, even across different Store runtimes.
	if additional != 0 && len(m.workingWaits) != 0 {
		return false, true
	}
	if l.state == nativeWaiting && l.working == 0 && (m.used > m.budget || additional > m.budget-m.used) {
		l.reclaimUnusedInputLocked(additional, 0)
	}
	if m.used <= m.budget && additional <= m.budget-m.used {
		l.working = working
		l.window = l.publication
		l.state = nativeStarting
		l.preparing = true
		m.used += additional
		return true, false
	}
	if m.convergingLocked() {
		return false, true
	}
	m.observeRejectionLocked(memoryStart, l, working)
	m.rejections++
	return false, false
}

func (l *executionLease) grow(working int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if working <= l.working {
		return nil
	}
	delta := uint64(working - l.working)
	if m.used > m.budget || delta > m.budget-m.used {
		m.observeRejectionLocked(memoryNativeGrowth, l, working)
		m.rejections++
		return errExecutionMemory
	}
	m.used += delta
	l.working = working
	return nil
}

func (l *executionLease) setWorking(working int) error {
	if working < 0 {
		return execution.ErrExecutionMemory
	}
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if working > l.working {
		delta := uint64(working - l.working)
		if m.used > m.budget || delta > m.budget-m.used {
			m.observeRejectionLocked(memoryRecordGrowth, l, working)
			m.rejections++
			return execution.ErrExecutionMemory
		}
		m.used += delta
	} else {
		m.used -= uint64(l.working - working)
		m.signalLocked()
	}
	l.working = working
	return nil
}

// requestWorking runs with the Ticket's Runtime locked. It never sleeps or
// advances ownership speculatively. The caller drops that lock before waiting
// and validates the live batch again before retrying this atomic ledger check.
func (l *executionLease) requestWorking(ctx context.Context, working int, wait **workingWait, live func() error) (<-chan struct{}, error) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released || working < 0 {
		return nil, errExecutionMemory
	}
	if working <= l.working {
		m.used -= uint64(l.working - working)
		l.working = working
		m.removeWorkingWaitLocked(*wait)
		m.signalLocked()
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		if err == context.DeadlineExceeded {
			m.observeGrowthLocked(memoryGrowthDeadline, l, working, *wait)
		}
		return nil, err
	}
	if err := live(); err != nil {
		if err == context.DeadlineExceeded {
			m.observeGrowthLocked(memoryGrowthDeadline, l, working, *wait)
		}
		return nil, err
	}
	delta := uint64(working - l.working)
	first := len(m.workingWaits) == 0 || m.workingWaits[0] == *wait
	if first && m.used <= m.budget && delta <= m.budget-m.used {
		m.used += delta
		l.working = working
		m.removeWorkingWaitLocked(*wait)
		m.signalLocked()
		return nil, nil
	}
	if !first && *wait != nil && m.used <= m.budget && delta <= m.budget-m.used {
		// This already captured claimant needs no further release. Yield only
		// its scheduling turn to older claims, never wait for their ownership.
		return (*wait).wake, nil
	}
	if _, bounded := ctx.Deadline(); !bounded || l.native {
		m.observeGrowthLocked(memoryRecordGrowth, l, working, *wait)
		m.rejections++
		return nil, errExecutionMemory
	}
	captured := *wait == nil
	if captured {
		donors := make(map[*executionLease]struct{})
		for donor := range m.leases {
			if donor != l && donor.native && donor.preparing {
				donors[donor] = struct{}{}
			}
		}
		pending := &workingWait{donors: donors, wake: make(chan struct{}, 1), since: time.Now()}
		*wait = pending
	}
	// Only actual current speculative working bytes form an optimistic
	// feasibility bound. Inputs, result/wire windows and all other owners stay
	// charged. Converting a donor to equally large retained output ends its
	// eligibility; it does not permit waiting for the consumer's later Ack.
	reclaimable := uint64(0)
	for donor := range (*wait).donors {
		if !donor.released && donor.native && donor.preparing {
			reclaimable += uint64(donor.working)
		}
	}
	remaining := m.used - reclaimable
	if reclaimable == 0 || remaining > m.budget || delta > m.budget-remaining {
		m.observeGrowthLocked(memoryRecordGrowth, l, working, *wait)
		m.rejections++
		return nil, errExecutionMemory
	}
	if captured {
		m.workingWaits = append(m.workingWaits, *wait)
	}
	return (*wait).wake, nil
}

func (m *ExecutionMemory) removeWorkingWaitLocked(wait *workingWait) {
	for index, pending := range m.workingWaits {
		if pending == wait {
			copy(m.workingWaits[index:], m.workingWaits[index+1:])
			m.workingWaits[len(m.workingWaits)-1] = nil
			m.workingWaits = m.workingWaits[:len(m.workingWaits)-1]
			break
		}
	}
}

func (m *ExecutionMemory) cancelWorkingWait(wait *workingWait) {
	m.mu.Lock()
	m.removeWorkingWaitLocked(wait)
	m.signalLocked()
	m.mu.Unlock()
}

func (l *executionLease) reserveResult(bytes int) error {
	if bytes < 0 {
		return execution.ErrExecutionMemory
	}
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	// The fixed source/failure metadata is already inside the input promise.
	// Reserve the actual source capacity and its later encoded copy together;
	// slow output must not consume the workspace protected for fresh IO.
	result := max(0, bytes-2*execution.ResultOverheadBytes)
	window := max(nativeQueuedWindow, max(bytes, execution.MinimumWireBytes)+2*execution.ResultOverheadBytes)
	if result != 0 {
		// Real backends use fixed 16-byte BSON/JSON content types. Their nested
		// protobuf tags/lengths and even a maximum uint64 response index fit well
		// inside this existing metadata allowance, including finishResult's
		// conservative source estimate. Promise it before copying the source.
		window += execution.ResultOverheadBytes
	}
	if !l.native && l.preparing && l.state == nativeStarting {
		next := uint64(result) + uint64(window-nativeQueuedWindow)
		l.reclaimUnusedInputLocked(next, next)
	}
	return l.resizeResultLocked(result, window)
}

func (l *executionLease) finishResult(bytes int, retainedDocument bool) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	result := max(0, bytes-2*execution.ResultOverheadBytes)
	if retainedDocument {
		// A smaller encoded length does not relinquish a RawMessage's backing
		// capacity. Only its future wire promise can shrink after serialization
		// size is known; the original allocation remains owned until Ack.
		result = max(result, l.result)
	}
	window := max(nativeQueuedWindow, max(bytes, execution.MinimumWireBytes)+2*execution.ResultOverheadBytes)
	return l.resizeResultLocked(result, window)
}

func (l *executionLease) resizeResultLocked(result, window int) error {
	m := l.memory
	previous := uint64(l.result) + uint64(l.window-nativeQueuedWindow)
	next := uint64(result) + uint64(window-nativeQueuedWindow)
	otherResident := m.resident - l.resident
	limit := m.residentLimitLocked()
	residentFits := otherResident <= limit && next <= limit-otherResident
	resident := next
	if !residentFits {
		resident = min(l.resident, next)
	}
	borrow := next > resident
	if next > previous && (m.used > m.budget || next-previous > m.budget-m.used) ||
		borrow && (next-resident > executionBorrowWindow || m.borrowed >= executionBorrowLimit && l.state != nativeBorrowed) {
		m.observeRejectionLocked(memoryResult, l, l.working)
		m.rejections++
		return execution.ErrExecutionMemory
	}
	m.used = m.used - previous + next
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	l.result, l.window = result, window
	l.resident = resident
	l.state = nativeSettled
	if borrow {
		l.state = nativeBorrowed
		m.borrowed++
	}
	m.resident = otherResident + l.resident
	m.signalLocked()
	return nil
}

func (l *executionLease) markTerminal() {
	m := l.memory
	m.mu.Lock()
	if l.state != nativeBorrowed {
		l.state = nativeTerminal
	}
	m.mu.Unlock()
}

func (l *executionLease) finishWorking() {
	m := l.memory
	m.mu.Lock()
	m.used -= uint64(l.working)
	l.working, l.preparing = 0, false
	m.signalLocked()
	m.mu.Unlock()
}

// WireReservation follows one actual encoded buffer, independently of the
// caller's result Ack. The lease remains counted until its last owner retires.
type WireReservation struct {
	lease *executionLease
	bytes int
	once  sync.Once
}

func (l *executionLease) reserveWire(bytes int) (*WireReservation, error) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if l.released || bytes < execution.MinimumWireBytes || l.wire != 0 {
		return nil, execution.ErrExecutionMemory
	}
	window := max(l.window, bytes+2*execution.ResultOverheadBytes)
	delta := uint64(window - l.window)
	limit := m.residentLimitLocked()
	if delta != 0 && (m.used > m.budget || delta > m.budget-m.used || m.resident > limit || delta > limit-m.resident) {
		m.observeRejectionLocked(memoryWire, l, l.working)
		m.rejections++
		return nil, execution.ErrExecutionMemory
	}
	l.window = window
	l.wire = bytes
	l.resident += delta
	m.resident += delta
	m.used += delta
	m.wireBytes += uint64(bytes)
	m.wireCount++
	reservation := &WireReservation{lease: l, bytes: bytes}
	return reservation, nil
}

// ReserveWire accounts a response without an admitted operation, such as an
// admission failure. It uses the same finite count/input promise and lifetime.
func (m *ExecutionMemory) ReserveWire(bytes int) (*WireReservation, error) {
	defer m.flushObservations()
	plan := &execution.Plan{Bytes: execution.EntryOverheadBytes, ResultBytes: execution.ResultOverheadBytes}
	lease, failure, _ := m.admit(plan)
	if failure != nil {
		return nil, execution.ErrExecutionMemory
	}
	reservation, err := lease.reserveWire(bytes)
	lease.release()
	return reservation, err
}

func (w *WireReservation) Release() {
	if w == nil {
		return
	}
	w.once.Do(func() {
		l := w.lease
		m := l.memory
		m.mu.Lock()
		defer m.mu.Unlock()
		m.wireBytes -= uint64(w.bytes)
		m.wireCount--
		if l.released {
			input := min(l.wire, nativeQueuedWindow-execution.ResultOverheadBytes) + execution.ResultOverheadBytes
			m.inputUsed -= uint64(input)
			m.used -= uint64(l.wire + execution.ResultOverheadBytes - input)
			m.resident -= l.resident
			l.resident = 0
			if l.state == nativeBorrowed {
				m.borrowed--
			}
		}
		l.wire = 0
		if l.released {
			m.removeLeaseLocked(l)
		}
		m.signalLocked()
	})
}

func (l *executionLease) beginRead(extra int) (bool, bool) {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if extra != 0 && len(m.workingWaits) != 0 && !l.preparing {
		return false, true
	}
	if m.used > m.budget || uint64(extra) > m.budget-m.used {
		if m.convergingLocked() {
			return false, true
		}
		// Waiting outputs cannot release their response working sets by first
		// reading more body bytes. Fail this attempted exchange instead of
		// cyclically waiting for memory that no current IO can relinquish.
		m.observeRejectionLocked(memoryBeginRead, l, l.working+extra)
		m.rejections++
		return false, false
	}
	m.used += uint64(extra)
	m.io += uint64(extra)
	l.extra = extra
	return true, false
}

func (m *ExecutionMemory) convergingLocked() bool {
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

func (l *executionLease) settle(working, eventBytes int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	window := max(l.window, 2*eventBytes+execution.ResultOverheadBytes)
	previous := uint64(l.working) + uint64(l.window) + uint64(l.extra)
	next := uint64(working) + uint64(window)
	total := next - nativeQueuedWindow
	otherResident := m.resident - l.resident
	limit := m.residentLimitLocked()
	resident := total
	if otherResident > limit || total > limit-otherResident {
		resident = min(l.resident, total)
	}
	borrow := total > resident
	if next > previous && (m.used > m.budget || next-previous > m.budget-m.used) ||
		borrow && (total-resident > executionBorrowWindow || m.borrowed >= executionBorrowLimit && l.state != nativeBorrowed) {
		m.observeRejectionLocked(memorySettle, l, working)
		m.rejections++
		return errExecutionMemory
	}
	m.used = m.used - previous + next
	m.io -= uint64(l.extra)
	changed := next != previous || l.preparing || l.extra != 0
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	l.working, l.window, l.extra, l.preparing = working, window, 0, false
	l.resident = resident
	m.resident = otherResident + resident
	l.state = nativeSettled
	if borrow {
		l.state = nativeBorrowed
		m.borrowed++
	}
	m.promoteLocked()
	if changed {
		m.signalLocked()
	}
	return nil
}

func (l *executionLease) terminal(eventBytes int) error {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	previous := uint64(l.working) + uint64(l.window) + uint64(l.extra)
	// Normal publication joins the previous wire owner before the next emission.
	// Cancellation may retain that wire, but sends no further terminal message.
	// Keep that actual owner or the new terminal promise, not unused chunks.
	window := max(nativeQueuedWindow, l.wire+2*execution.ResultOverheadBytes, 2*eventBytes+execution.ResultOverheadBytes)
	otherResident := m.resident - l.resident
	total := uint64(window - nativeQueuedWindow)
	limit := m.residentLimitLocked()
	resident := total
	if otherResident > limit || total > limit-otherResident {
		resident = min(l.resident, total)
	}
	borrow := total > resident
	if uint64(window) > previous && (m.used > m.budget || uint64(window)-previous > m.budget-m.used) ||
		borrow && (total-resident > executionBorrowWindow || m.borrowed >= executionBorrowLimit && l.state != nativeBorrowed) {
		m.observeRejectionLocked(memoryTerminal, l, 0)
		m.rejections++
		return errExecutionMemory
	}
	l.window = window
	m.used = m.used - previous + uint64(window)
	m.io -= uint64(l.extra)
	if l.state == nativeBorrowed {
		m.borrowed--
	}
	l.working, l.extra, l.state, l.preparing = 0, 0, nativeTerminal, false
	l.resident = resident
	if borrow {
		l.state = nativeBorrowed
		m.borrowed++
	}
	m.resident = otherResident + l.resident
	m.promoteLocked()
	m.signalLocked()
	return nil
}

func (l *executionLease) release() {
	m := l.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, live := m.leases[l]; !live || l.released {
		return
	}
	l.released = true
	wasBorrowed := l.state == nativeBorrowed
	// A small wire can draw from the fixed terminal credit already inside Q.
	// Keep that draw promised until the real buffer owner ends; do not hand the
	// same bytes to a fresh input merely because its caller has canceled.
	heldInput, heldExtra := 0, uint64(0)
	if l.wire != 0 {
		wireInput := min(l.wire, nativeQueuedWindow-execution.ResultOverheadBytes)
		heldInput = wireInput + execution.ResultOverheadBytes
		heldExtra = uint64(l.wire - wireInput)
	}
	m.inputUsed -= uint64(l.input+nativeQueuedWindow) - uint64(heldInput)
	m.used -= uint64(l.working+l.result+l.window-nativeQueuedWindow+l.extra) - heldExtra
	m.io -= uint64(l.extra)
	resident := min(l.resident, heldExtra)
	m.resident -= l.resident - resident
	l.resident = resident
	l.working, l.result, l.extra, l.preparing, l.state = 0, 0, 0, false, nativeTerminal
	l.window = nativeQueuedWindow + int(heldExtra)
	if wasBorrowed {
		if heldExtra > resident {
			l.state = nativeBorrowed
		} else {
			m.borrowed--
		}
	}
	if l.wire == 0 {
		m.removeLeaseLocked(l)
	}
	m.promoteLocked()
	m.signalLocked()
}

func (m *ExecutionMemory) removeLeaseLocked(l *executionLease) {
	delete(m.leases, l)
	if l.native {
		m.nativeCount--
	}
	if len(m.leases) == 0 {
		m.used -= m.inputReserved
		m.resident -= m.inputReserved
		m.inputReserved = 0
	}
}

// Credit classification follows actual held ownership; promotion never changes
// UsedBytes or grants memory that has not really been released.
func (m *ExecutionMemory) promoteLocked() {
	if m.borrowed == 0 {
		return
	}
	limit := m.residentLimitLocked()
	for l := range m.leases {
		if l.state != nativeBorrowed {
			continue
		}
		total := uint64(l.window-nativeQueuedWindow) + uint64(l.result)
		if l.native {
			total += uint64(l.working)
		}
		delta := total - l.resident
		if m.resident <= limit && delta <= limit-m.resident {
			m.resident += delta
			l.resident = total
			l.state = nativeSettled
			m.borrowed--
		}
	}
}
