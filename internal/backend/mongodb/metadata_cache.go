package mongodb

import (
	"context"
	"sync"

	"github.com/batchstream/weir/internal/backend"
)

type targetCheck struct {
	success bool
	ready   chan struct{}
}

// targetCache retains successful collection checks in completion order. Pending
// checks do not consume cache capacity; callers for the same target share I/O.
type targetCache struct {
	mu       sync.Mutex
	entries  map[namespace]*targetCheck
	order    []namespace
	capacity int
}

// acquire returns nil for a successful check, or the new check owned by this
// caller. A failed owner lets a waiting caller start a fresh check.
func (c *targetCache) acquire(ctx context.Context, target namespace) (*targetCheck, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		c.mu.Lock()
		if c.entries == nil {
			c.entries = make(map[namespace]*targetCheck)
			c.capacity = backend.DefaultOptions().MetadataCacheEntries
		}
		if existing := c.entries[target]; existing != nil {
			if existing.ready == nil {
				c.mu.Unlock()
				return nil, ctx.Err()
			}
			ready := existing.ready
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-ready:
				if existing.success {
					return nil, ctx.Err()
				}
			}
			continue
		}
		pending := &targetCheck{ready: make(chan struct{})}
		c.entries[target] = pending
		c.mu.Unlock()
		return pending, nil
	}
}

func (c *targetCache) complete(target namespace, pending *targetCheck, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if pending == nil || c.entries[target] != pending || pending.ready == nil {
		return
	}
	pending.success = success
	if success && c.capacity > 0 {
		c.order = append(c.order, target)
		if len(c.order) > c.capacity {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
	} else {
		delete(c.entries, target)
	}
	close(pending.ready)
	pending.ready = nil
}
