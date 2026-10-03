package execution

import "sync"

// ResultBudget charges copied read data against both this response and the
// Store's retained-result ledger. Refusing additional data never blocks a
// partially filled response while another response waits for the same bytes.
type ResultBudget struct {
	mu     sync.Mutex
	Limit  int
	Used   int
	Retain func(int) bool
}

func (b *ResultBudget) Reserve(bytes int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes < 0 || bytes > b.Limit-b.Used || b.Retain != nil && !b.Retain(bytes) {
		return false
	}
	b.Used += bytes
	return true
}
