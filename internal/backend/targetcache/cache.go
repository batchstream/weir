// Package targetcache retains successful metadata checks for a bounded set of targets.
package targetcache

import (
	"context"
	"sync"
)

const Capacity = 64

type entry[V any] struct {
	value V
	ready chan struct{}
	done  bool
}

type Lookup[V any] struct {
	Value  V
	Cached bool
	entry  *entry[V]
}

// Cache owns no I/O. A cold caller acquires a reservation, checks its target,
// then completes that reservation. Other callers on the target wait with their
// own contexts. Completed targets are evicted in insertion order when full.
type Cache[K comparable, V any] struct {
	mu      sync.Mutex
	entries map[K]*entry[V]
	order   []K
	changed chan struct{}
}

func (c *Cache[K, V]) Acquire(ctx context.Context, key K) (Lookup[V], error) {
	for {
		if err := ctx.Err(); err != nil {
			empty := Lookup[V]{}
			return empty, err
		}
		c.mu.Lock()
		if c.entries == nil {
			c.entries = make(map[K]*entry[V])
			c.changed = make(chan struct{})
		}
		if existing := c.entries[key]; existing != nil {
			if existing.done {
				lookup := Lookup[V]{Value: existing.value, Cached: true}
				c.mu.Unlock()
				return lookup, ctx.Err()
			}
			ready := existing.ready
			c.mu.Unlock()
			if err := wait(ctx, ready); err != nil {
				empty := Lookup[V]{}
				return empty, err
			}
			continue
		}
		if len(c.entries) == Capacity {
			for i, oldest := range c.order {
				if c.entries[oldest].done {
					delete(c.entries, oldest)
					c.order = append(c.order[:i], c.order[i+1:]...)
					break
				}
			}
			if len(c.entries) == Capacity {
				changed := c.changed
				c.mu.Unlock()
				if err := wait(ctx, changed); err != nil {
					empty := Lookup[V]{}
					return empty, err
				}
				continue
			}
		}
		pending := &entry[V]{ready: make(chan struct{})}
		c.entries[key] = pending
		c.order = append(c.order, key)
		lookup := Lookup[V]{entry: pending}
		c.mu.Unlock()
		return lookup, nil
	}
}

// Complete releases a cold reservation. Only successful checks are retained;
// failures wake waiting callers so they can perform their own check.
func (c *Cache[K, V]) Complete(key K, lookup Lookup[V], value V, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := lookup.entry
	if pending == nil || c.entries[key] != pending || pending.done {
		return
	}
	if success {
		pending.value = value
		pending.done = true
	} else {
		delete(c.entries, key)
		for i, target := range c.order {
			if target == key {
				c.order = append(c.order[:i], c.order[i+1:]...)
				break
			}
		}
	}
	close(pending.ready)
	close(c.changed)
	c.changed = make(chan struct{})
}

func wait(ctx context.Context, ready <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-ready:
		return ctx.Err()
	}
}
