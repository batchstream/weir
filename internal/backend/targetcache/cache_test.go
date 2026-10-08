package targetcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestConcurrentColdTargetSharesOneReservation(t *testing.T) {
	var cache Cache[string, int]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	owner, err := cache.Acquire(ctx, "records")
	if err != nil || owner.Cached {
		t.Fatal(owner, err)
	}
	var workers sync.WaitGroup
	var owners atomic.Int32
	for range 32 {
		workers.Go(func() {
			lookup, err := cache.Acquire(ctx, "records")
			if err != nil || lookup.Value != 42 {
				t.Error("shared target lookup failed", lookup, err)
			}
			if !lookup.Cached {
				owners.Add(1)
				cache.Complete("records", lookup, 42, true)
			}
		})
	}
	cache.Complete("records", owner, 42, true)
	workers.Wait()
	if owners.Load() != 0 {
		t.Fatal("cold target started duplicate checks", owners.Load())
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if len(cache.entries) != 1 {
		t.Fatal("duplicate metadata entries", len(cache.entries))
	}
}

func TestFailedReservationCanBeCheckedAgain(t *testing.T) {
	var cache Cache[string, int]
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	failed, err := cache.Acquire(ctx, "records")
	if err != nil {
		t.Fatal(err)
	}
	cache.Complete("records", failed, 99, false)
	retry, err := cache.Acquire(ctx, "records")
	if err != nil || retry.Cached {
		t.Fatal("failed check was retained", retry, err)
	}
	// Old owners cannot complete a new check, and repeated completion must not
	// discard or replace the successful result.
	cache.Complete("records", failed, 99, true)
	cache.Complete("records", retry, 42, true)
	cache.Complete("records", retry, 99, false)
	hot, err := cache.Acquire(ctx, "records")
	if err != nil || !hot.Cached || hot.Value != 42 {
		t.Fatal("successful retry was not retained", hot, err)
	}
}

func TestCanceledWaiterDoesNotCancelSharedCheck(t *testing.T) {
	var cache Cache[string, int]
	owner, err := cache.Acquire(context.Background(), "records")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	_, err = cache.Acquire(ctx, "records")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("waiting target ignored its deadline", err)
	}
	cache.Complete("records", owner, 42, true)
	hot, err := cache.Acquire(context.Background(), "records")
	if err != nil || !hot.Cached || hot.Value != 42 {
		t.Fatal("canceled waiter released another caller's check", hot, err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	_, err = cache.Acquire(canceled, "records")
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cache hit ignored cancellation", err)
	}
}

func TestPendingChecksDoNotConsumeCompletedCacheCapacity(t *testing.T) {
	cache := New[int, int](2)
	owners := make([]Lookup[int], DefaultCapacity+1)
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	for i := range owners {
		lookup, err := cache.Acquire(ctx, i)
		if err != nil || lookup.Cached {
			t.Fatal(lookup, err)
		}
		owners[i] = lookup
	}
	for i := range owners {
		cache.Complete(i, owners[i], i, true)
	}
	if len(cache.entries) != 2 || len(cache.order) != 2 {
		t.Fatal("completed cache exceeded configured capacity")
	}
	lookup, err := cache.Acquire(ctx, 0)
	if err != nil || lookup.Cached {
		t.Fatal("evicted target was reused", err)
	}
	cache.Complete(0, lookup, 0, true)
}

func TestZeroCapacityDisablesCompletedCache(t *testing.T) {
	cache := New[string, int](0)
	owner, err := cache.Acquire(t.Context(), "target")
	if err != nil {
		t.Fatal(err)
	}
	cache.Complete("target", owner, 42, true)
	next, err := cache.Acquire(t.Context(), "target")
	if err != nil || next.Cached {
		t.Fatal("disabled cache retained a completed check", err)
	}
	cache.Complete("target", next, 43, true)
}
