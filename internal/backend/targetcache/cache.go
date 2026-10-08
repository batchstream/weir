// Package targetcache retains completed metadata checks without limiting cold checks.
package targetcache

import (
	"context"
	"sync"
)

const DefaultCapacity = 64

type entry[V any] struct {
	value   V
	success bool
	ready   chan struct{}
}

type Lookup[V any] struct {
	Value  V
	Cached bool
	entry  *entry[V]
}

// Capacity applies only to successful completed checks. Pending checks for
// distinct targets never wait for cache space; callers for one target share I/O.
type Cache[K comparable, V any] struct {
	mu       sync.Mutex
	entries  map[K]*entry[V]
	order    []K
	capacity int
}

func New[K comparable, V any](capacity int) *Cache[K, V] {
	cache := &Cache[K, V]{entries: make(map[K]*entry[V]), capacity: capacity}
	return cache
}

// SetCapacity configures completed entries, independently of pending checks.
func (c *Cache[K, V]) SetCapacity(capacity int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[K]*entry[V])
	}
	c.capacity = capacity
	for len(c.order) > capacity {
		delete(c.entries, c.order[0])
		c.order = c.order[1:]
	}
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
			c.capacity = DefaultCapacity
		}
		if existing := c.entries[key]; existing != nil {
			if existing.ready == nil {
				lookup := Lookup[V]{Value: existing.value, Cached: true}
				c.mu.Unlock()
				return lookup, ctx.Err()
			}
			ready := existing.ready
			c.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-ready:
				if existing.success {
					lookup := Lookup[V]{Value: existing.value, Cached: true}
					return lookup, ctx.Err()
				}
			}
			continue
		}
		pending := &entry[V]{ready: make(chan struct{})}
		c.entries[key] = pending
		lookup := Lookup[V]{entry: pending}
		c.mu.Unlock()
		return lookup, nil
	}
}

func (c *Cache[K, V]) Complete(key K, lookup Lookup[V], value V, success bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := lookup.entry
	if pending == nil || c.entries[key] != pending || pending.ready == nil {
		return
	}
	pending.value = value
	pending.success = success
	if success && c.capacity > 0 {
		c.order = append(c.order, key)
		if len(c.order) > c.capacity {
			delete(c.entries, c.order[0])
			c.order = c.order[1:]
		}
	} else {
		delete(c.entries, key)
	}
	close(pending.ready)
	pending.ready = nil
}
