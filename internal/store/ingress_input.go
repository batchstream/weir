package store

import "github.com/batchstream/weir/internal/execution"

// IngressInput transfers one decoded source into a Store input lease under the
// same Node ledger lock. Failed admission keeps the source's original account.
type IngressInput struct {
	account *IngressMemory
	bytes   uint64
	plan    *execution.Plan
}

func (a *IngressMemory) Input(bytes int) *IngressInput {
	input := &IngressInput{account: a, bytes: uint64(bytes)}
	return input
}
func (input *IngressInput) Bind(plan *execution.Plan) {
	m := input.account.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	if input.bytes == 0 || input.account.closed {
		return
	}
	if input.plan != nil && input.plan != plan {
		panic("ingress source already assigned")
	}
	input.plan = plan
	if m.ingressHandoffs == nil {
		m.ingressHandoffs = make(map[*execution.Plan]*IngressInput)
	}
	m.ingressHandoffs[plan] = input
}
func (input *IngressInput) Release() {
	m := input.account.memory
	m.mu.Lock()
	defer m.mu.Unlock()
	input.releaseLocked()
}
func (input *IngressInput) releaseLocked() {
	a := input.account
	m := a.memory
	if input.plan != nil {
		delete(m.ingressHandoffs, input.plan)
		input.plan = nil
	}
	if input.bytes != 0 && !a.closed {
		a.resizeLocked(a.message, a.raw, a.decoded-input.bytes, false)
	}
	input.bytes = 0
}

func (m *ExecutionMemory) ownsIngress(plan *execution.Plan) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	input := m.ingressHandoffs[plan]
	return input != nil && input.account.memory == m && !input.account.closed && input.bytes != 0
}
