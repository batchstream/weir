package execution

import "sync"

// ResultBudget tracks copied read data within the Store credits reserved before
// this bounded window starts database work.
type ResultBudget struct {
	mu    sync.Mutex
	Limit int
	Used  int
}

func (b *ResultBudget) Reserve(bytes int) bool {
	if b == nil {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes < 0 || bytes > b.Limit-b.Used {
		return false
	}
	b.Used += bytes
	return true
}
