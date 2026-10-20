package store

import (
	"sync"

	"github.com/batchstream/weir/internal/execution"
)

// IngressConnectionBytes is a reservation, not measured RSS. Native receive
// owns 32KiB read + 32KiB write buffers and a 16KiB DATA frame. With the 64KiB
// header-list limit, x/net/hpack permits 2*(64KiB+8) bytes of an incomplete
// field; bytes.Buffer geometric growth can retain a 256KiB saveBuf after Reset.
// The remaining 176KiB covers bounded HeaderField/string arrays, HPACK tables,
// temporary metadata maps and fixed transport/control state. Retained RPC
// metadata is separately charged. This keeps the upstream parser and 64KiB
// compatibility bound without treating hundreds of maximum parser peaks as free.
const IngressConnectionBytes = 512 << 10

func (m *ExecutionMemory) ReserveIngressConnection() (func(), error) {
	defer m.flushObservations()
	m.mu.Lock()
	if !m.ingressFitsLocked(IngressConnectionBytes, false) {
		m.observeIngressLocked(memoryIngressConnection, IngressConnectionBytes)
		m.rejections++
		m.mu.Unlock()
		return nil, execution.ErrExecutionMemory
	}
	m.ingressConnections++
	m.ingressConnectionBytes += IngressConnectionBytes
	m.used += IngressConnectionBytes
	m.resident += IngressConnectionBytes
	m.mu.Unlock()
	var once sync.Once
	release := func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.ingressConnections--
			m.ingressConnectionBytes -= IngressConnectionBytes
			m.used -= IngressConnectionBytes
			m.resident -= IngressConnectionBytes
			m.signalLocked()
		})
	}
	return release, nil
}
