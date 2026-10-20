package store

import (
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/batchstream/weir-protocol/api/protocol"
	"github.com/batchstream/weir/internal/execution"
)

// These internal codes describe ledger boundaries, never commands or callers.
// Each Node reports a reason at most once, with at most eight reports in total.
type executionMemoryReason uint8

const (
	memoryAdmit executionMemoryReason = iota
	memoryStart
	memoryRecordGrowth
	memoryNativeGrowth
	memoryBeginRead
	memorySettle
	memoryResult
	memoryIngress
	memoryQueuedDeadline
	memoryGrowthDeadline
	memoryWire
	memoryTerminal
	memoryIngressConnection
	memoryIngressMessage
	memoryIngressBuffer
	memoryIngressDecode
	memoryNativeReadDeadline
)

const executionMemoryObservationLimit = 8

func (reason executionMemoryReason) String() string {
	switch reason {
	case memoryAdmit:
		return "admit"
	case memoryStart:
		return "start"
	case memoryRecordGrowth:
		return "record_growth"
	case memoryNativeGrowth:
		return "native_grow"
	case memoryBeginRead:
		return "begin_read"
	case memorySettle:
		return "settle"
	case memoryResult:
		return "result"
	case memoryIngress:
		return "ingress_new"
	case memoryIngressConnection:
		return "ingress_connection"
	case memoryIngressMessage:
		return "ingress_message"
	case memoryIngressBuffer:
		return "ingress_buffer"
	case memoryIngressDecode:
		return "ingress_decode"
	case memoryNativeReadDeadline:
		return "native_read_deadline"
	case memoryQueuedDeadline:
		return "queued_deadline"
	case memoryGrowthDeadline:
		return "growth_deadline"
	case memoryWire:
		return "wire"
	case memoryTerminal:
		return "terminal"
	default:
		return "unknown"
	}
}

// All fields are numeric or booleans copied atomically under the ledger lock.
// In particular, no Plan, context, command, source, resource or backend survives
// into the log call after the ownership locks are released.
type executionMemoryObservation struct {
	Reason                         executionMemoryReason
	BudgetBytes, UsedBytes         uint64
	ReserveBytes                   uint64
	Known, Trusted                 bool
	RequestedInputBytes            uint64
	RequestedPublicationBytes      uint64
	IngressCreditBytes             uint64
	AccountedBytes, ResidentBytes  uint64
	InputReservedBytes, InputBytes uint64
	IngressBytes, ConnectionBytes  uint64
	IOBytes, WireBytes             uint64
	WorkingBytes, ResultBytes      uint64
	WindowBytes                    uint64
	NativePreparingWorkingBytes    uint64
	NativeSettledWorkingBytes      uint64
	RecordWorkingBytes             uint64
	Leases, RecordLeases           int
	NativeLeases                   int
	NativePreparing, NativeSettled int
	Waiting, Starting              int
	Settled, Terminal, Borrowed    int
	OwnerNative, OwnerPreparing    bool
	OwnerState                     uint8
	OwnerWorkingBytes              uint64
	TargetWorkingBytes, DeltaBytes uint64
	CohortCount, CohortPreparing   int
	CohortOwnedWorkingBytes        uint64
	CohortPreparingWorkingBytes    uint64
	FIFOPosition                   int
	WorkingClaims                  int
	WaitNanos                      int64
}

type executionMemoryObservationRequest struct {
	reason      executionMemoryReason
	lease       *executionLease
	target      int
	wait        *workingWait
	queued      time.Duration
	input       uint64
	publication uint64
	credit      uint64
	native      bool
}

func (m *ExecutionMemory) observeAdmissionLocked(plan *execution.Plan) {
	publication := 0
	native := plan.Command.GetNative() != nil
	if native {
		publication = max(plan.ResultBytes, 2*protocol.NativeChunk+execution.ResultOverheadBytes) - nativeQueuedWindow
	}
	credit := uint64(0)
	if input := m.ingressHandoffs[plan]; input != nil && !input.account.closed {
		credit = input.bytes
	}
	request := executionMemoryObservationRequest{reason: memoryAdmit, target: plan.WorkingBytes, input: uint64(plan.Bytes) + nativeQueuedWindow, publication: uint64(publication), credit: credit, native: native}
	m.observeLocked(request)
}

func (m *ExecutionMemory) observeIngressLocked(reason executionMemoryReason, bytes int) {
	request := executionMemoryObservationRequest{reason: reason, input: uint64(max(0, bytes))}
	m.observeLocked(request)
}

func (m *ExecutionMemory) observeRejectionLocked(reason executionMemoryReason, lease *executionLease, target int) {
	request := executionMemoryObservationRequest{reason: reason, lease: lease, target: target}
	m.observeLocked(request)
}

func (m *ExecutionMemory) observeGrowthLocked(reason executionMemoryReason, lease *executionLease, target int, wait *workingWait) {
	request := executionMemoryObservationRequest{reason: reason, lease: lease, target: target, wait: wait}
	m.observeLocked(request)
}

func (m *ExecutionMemory) observeGrowthDeadline(lease *executionLease, target int, wait *workingWait) {
	m.mu.Lock()
	m.observeGrowthLocked(memoryGrowthDeadline, lease, target, wait)
	m.mu.Unlock()
}

func (m *ExecutionMemory) observeNativeReadDeadline(lease *executionLease, extra int, queued time.Duration) {
	m.mu.Lock()
	request := executionMemoryObservationRequest{reason: memoryNativeReadDeadline, lease: lease, target: lease.working + extra, queued: queued}
	m.observeLocked(request)
	m.mu.Unlock()
}

func (m *ExecutionMemory) observeQueuedDeadline(lease *executionLease, queued time.Duration) {
	m.mu.Lock()
	request := executionMemoryObservationRequest{reason: memoryQueuedDeadline, lease: lease, queued: queued}
	m.observeLocked(request)
	m.mu.Unlock()
}

type executionMemoryObservations struct {
	ready   atomic.Bool
	reasons uint32
	count   int
	pending []executionMemoryObservation
	sink    func(executionMemoryObservation)
}

func (m *ExecutionMemory) observeLocked(request executionMemoryObservationRequest) {
	observations := &m.observations
	bit := uint32(1) << request.reason
	if observations.count >= executionMemoryObservationLimit || observations.reasons&bit != 0 {
		return
	}
	observations.reasons |= bit
	observations.count++
	observation := m.observationLocked(request)
	observations.pending = append(observations.pending, observation)
	observations.ready.Store(true)
}

// FlushObservations emits pending bounded failure snapshots from an external
// diagnostic/caller-owned context. It must not run on dispatch, adapter IO or
// runtime retirement paths: a blocked sink must never prevent their joining.
// Node diagnostic collection uses this pull boundary even after new RPCs stop.
func (m *ExecutionMemory) FlushObservations() {
	m.flushObservations()
}

// Only external caller/diagnostic operations flush. Capture itself never logs.
// Normal operations perform one atomic load and allocate nothing. A slow logger
// may delay its external caller, but dispatch and backend retirement never wait
// for that caller or acquire any logger-owned state.
func (m *ExecutionMemory) flushObservations() {
	if !m.observations.ready.Load() {
		return
	}
	m.mu.Lock()
	pending := m.observations.pending
	m.observations.pending = nil
	m.observations.ready.Store(false)
	sink := m.observations.sink
	m.mu.Unlock()
	for _, observation := range pending {
		if sink != nil {
			sink(observation)
		} else {
			slog.Warn("execution memory boundary observation", "reason", observation.Reason.String(), "ownership", observation)
		}
	}
}

func (m *ExecutionMemory) observationLocked(request executionMemoryObservationRequest) executionMemoryObservation {
	observation := executionMemoryObservation{
		Reason: request.reason, BudgetBytes: m.budget, UsedBytes: m.used,
		ReserveBytes: m.reserve, Known: m.known, Trusted: m.trusted,
		RequestedInputBytes: request.input, RequestedPublicationBytes: request.publication,
		IngressCreditBytes: request.credit, OwnerNative: request.native,
		TargetWorkingBytes: uint64(max(0, request.target)), DeltaBytes: uint64(max(0, request.target)),
		ResidentBytes: m.resident, InputReservedBytes: m.inputReserved,
		InputBytes: m.inputUsed, IngressBytes: m.ingressReserved,
		ConnectionBytes: m.ingressConnectionBytes, IOBytes: m.io,
		WireBytes: m.wireBytes, Leases: len(m.leases), NativeLeases: m.nativeCount, FIFOPosition: -1,
		WorkingClaims: len(m.workingWaits),
		WaitNanos:     int64(request.queued),
	}
	for lease := range m.leases {
		observation.WorkingBytes += uint64(lease.working)
		observation.ResultBytes += uint64(lease.result)
		observation.WindowBytes += uint64(lease.window - nativeQueuedWindow)
		if lease.native {
			if lease.preparing {
				observation.NativePreparing++
				observation.NativePreparingWorkingBytes += uint64(lease.working)
			} else if lease.state == nativeSettled || lease.state == nativeBorrowed {
				observation.NativeSettled++
				observation.NativeSettledWorkingBytes += uint64(lease.working)
			}
		} else {
			observation.RecordLeases++
			observation.RecordWorkingBytes += uint64(lease.working)
		}
		switch lease.state {
		case nativeWaiting:
			observation.Waiting++
		case nativeStarting:
			observation.Starting++
		case nativeSettled:
			observation.Settled++
		case nativeTerminal:
			observation.Terminal++
		case nativeBorrowed:
			observation.Borrowed++
		}
	}
	observation.AccountedBytes = observation.InputReservedBytes + observation.IngressBytes + observation.ConnectionBytes + observation.IOBytes + observation.WorkingBytes + observation.ResultBytes + observation.WindowBytes
	if lease := request.lease; lease != nil {
		observation.OwnerNative, observation.OwnerPreparing = lease.native, lease.preparing
		observation.OwnerState = lease.state
		observation.OwnerWorkingBytes = uint64(lease.working)
		observation.TargetWorkingBytes = uint64(max(0, request.target))
		observation.DeltaBytes = uint64(max(0, request.target-lease.working))
	}
	if wait := request.wait; wait != nil {
		if !wait.since.IsZero() {
			observation.WaitNanos = int64(time.Since(wait.since))
		}
		observation.CohortCount = len(wait.donors)
		for donor := range wait.donors {
			if !donor.released {
				observation.CohortOwnedWorkingBytes += uint64(donor.working)
				if donor.native && donor.preparing {
					observation.CohortPreparing++
					observation.CohortPreparingWorkingBytes += uint64(donor.working)
				}
			}
		}
		for position, pending := range m.workingWaits {
			if pending == wait {
				observation.FIFOPosition = position
				break
			}
		}
	}
	return observation
}
